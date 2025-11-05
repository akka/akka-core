/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalApi
import akka.cluster.ddata.Replicator.ReadAll
import akka.cluster.ddata.Replicator.ReadMajorityPlus
import akka.cluster.ddata.Replicator.WriteAll
import akka.cluster.ddata.Replicator.WriteMajorityPlus
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator
import akka.cluster.ddata.typed.scaladsl.ReplicatorMessageAdapter
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.typed.ChangeNumberOfProcesses
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.GetNumberOfProcesses
import akka.cluster.sharding.typed.NumberOfProcesses
import akka.cluster.sharding.typed.ShardedDaemonProcessCommand
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.StartEntity
import akka.pattern.StatusReply
import akka.stream.KillSwitches
import akka.stream.UniqueKillSwitch
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 *
 * A ShardedDaemonProcessCoordinator manages dynamic scaling of one setup of ShardedDaemonProcess
 */
@InternalApi
private[akka] object ShardedDaemonProcessCoordinator {

  private trait InternalMessage extends ShardedDaemonProcessCommand

  private case class InternalGetResponse(rsp: Replicator.GetResponse[ShardedDaemonProcessState]) extends InternalMessage

  private case class InternalUpdateResponse(rsp: Replicator.UpdateResponse[ShardedDaemonProcessState])
      extends InternalMessage

  private case class ShardStopped(shard: String) extends InternalMessage

  private case object Tick extends InternalMessage
  private case object SendKeepAliveDone extends InternalMessage

  private object ShardStopTimeout extends InternalMessage

  final case class GetNumberOfProcessesReply(
      numberOfProcesses: Int,
      started: Instant,
      rescaleInProgress: Boolean,
      revision: Long)
      extends NumberOfProcesses
      with ClusterShardingTypedSerializable

  def apply[T](
      settings: ShardedDaemonProcessSettings,
      shardingSettings: ClusterShardingSettings,
      initialNumberOfProcesses: Int,
      daemonProcessName: String,
      shardingRef: ActorRef[ShardingEnvelope[T]]): Behavior[ShardedDaemonProcessCommand] = {
    Behaviors
      .supervise[ShardedDaemonProcessCommand](Behaviors.setup { context =>
        Behaviors.withTimers {
          timers =>
            context.log.debug("ShardedDaemonProcessCoordinator for [{}] starting", daemonProcessName)
            val key = ShardedDaemonProcessStateKey(daemonProcessName)
            DistributedData.withReplicatorMessageAdapter[ShardedDaemonProcessCommand, ShardedDaemonProcessState] {
              replicatorAdapter =>
                new ShardedDaemonProcessCoordinator(
                  settings,
                  shardingSettings,
                  context,
                  timers,
                  daemonProcessName,
                  shardingRef.toClassic,
                  initialNumberOfProcesses,
                  key,
                  replicatorAdapter).start()

            }
        }
      })
      .onFailure(SupervisorStrategy.restart)
  }
}

/**
 * Rescaling workflow overview:
 *
 * 1. stop pinging workers
 * 2. Write to ddata new size and revision, in progress/not complete
 * 3. Stop shards for all workers via shard coordinator
 * 4. Update ddata state, mark rescaling completed
 * 5. start pinging workers again
 *
 * If coordinator moved before completing it will re-trigger workflow from step 2 on start, worst case stopping all
 * workers before starting them again.
 */
private final class ShardedDaemonProcessCoordinator private (
    settings: ShardedDaemonProcessSettings,
    shardingSettings: ClusterShardingSettings,
    context: ActorContext[ShardedDaemonProcessCommand],
    timers: TimerScheduler[ShardedDaemonProcessCommand],
    daemonProcessName: String,
    shardingRef: akka.actor.ActorRef,
    initialNumberOfProcesses: Int,
    key: ShardedDaemonProcessStateKey,
    replicatorAdapter: ReplicatorMessageAdapter[ShardedDaemonProcessCommand, ShardedDaemonProcessState]) {
  import ShardedDaemonProcessCoordinator._

  // Stopping shards can take a lot of time, so don't hammer it with retries (even if the
  // stop shard command is idempotent)
  private val stopShardsTimeout = shardingSettings.tuningParameters.handOffTimeout / 4

  private val majorityMinCap =
    context.system.settings.config.getInt("akka.cluster.sharding.distributed-data.majority-min-cap")
  private val stateReadConsistency = shardingSettings.tuningParameters.coordinatorStateReadMajorityPlus match {
    case Int.MaxValue => ReadAll(shardingSettings.tuningParameters.waitingForStateTimeout)
    case additional =>
      ReadMajorityPlus(shardingSettings.tuningParameters.waitingForStateTimeout, additional, majorityMinCap)
  }
  private val stateWriteConsistency = shardingSettings.tuningParameters.coordinatorStateWriteMajorityPlus match {
    case Int.MaxValue => WriteAll(shardingSettings.tuningParameters.updatingStateTimeout)
    case additional =>
      WriteMajorityPlus(shardingSettings.tuningParameters.updatingStateTimeout, additional, majorityMinCap)
  }

  private val shardStoppedAdapter = context
    .messageAdapter[ShardCoordinator.Internal.ShardStopped] {
      case ShardCoordinator.Internal.ShardStopped(shard) => ShardStopped(shard)
    }
    .toClassic

  private def initialState = ShardedDaemonProcessState.initialState(initialNumberOfProcesses)

  def start(): Behavior[ShardedDaemonProcessCommand] = {
    replicatorAdapter.askGet(
      replyTo => Replicator.Get(key, stateReadConsistency, replyTo),
      response => InternalGetResponse(response))

    Behaviors.withStash(100) { stash =>
      Behaviors.receiveMessage {
        case InternalGetResponse(success @ Replicator.GetSuccess(`key`)) =>
          val existingScaleState = success.get(key)
          if (existingScaleState.completed) {
            context.log.debugN(
              "{}: Previous completed state found for Sharded Daemon Process rev [{}] from [{}]",
              daemonProcessName,
              existingScaleState.revision,
              existingScaleState.started)
            stash.stash(Tick)
            stash.unstashAll(idle(existingScaleState))
          } else {
            context.log.debugN(
              "{}: Previous non completed state found for Sharded Daemon Process rev [{}] from [{}], retriggering scaling",
              daemonProcessName,
              existingScaleState.revision,
              existingScaleState.started)
            stopAllShards(existingScaleState, None, initialNumberOfProcesses)
          }

        case InternalGetResponse(_ @Replicator.NotFound(`key`)) =>
          context.log.debug("{}: No previous state stored for Sharded Daemon Process", daemonProcessName)
          stash.stash(Tick)
          stash.unstashAll(idle(initialState))

        case InternalGetResponse(_ @Replicator.GetFailure(`key`)) =>
          context.log.info("{}: Failed fetching initial state for Sharded Daemon Process, retrying", daemonProcessName)
          start()

        case other =>
          stash.stash(other)
          Behaviors.same
      }
    }
  }

  private def idle(
      currentState: ShardedDaemonProcessState,
      runningThrottledPingKillswitch: Option[UniqueKillSwitch] = None): Behavior[ShardedDaemonProcessCommand] = {

    Behaviors.receiveMessagePartial {
      case request: ChangeNumberOfProcesses =>
        if (request.newNumberOfProcesses == currentState.numberOfProcesses) {
          context.log.info(
            "{}: Rescale command for Sharded Daemon Process ignored, already running [{}] processes",
            daemonProcessName,
            request.newNumberOfProcesses)
          request.replyTo ! StatusReply.Ack
          Behaviors.same
        } else {
          runningThrottledPingKillswitch.foreach(_.shutdown())
          val newState = currentState.startScalingTo(request.newNumberOfProcesses)
          context.log.infoN(
            "{}: Starting rescaling of Sharded Daemon Process to [{}] processes, rev [{}]",
            daemonProcessName,
            request.newNumberOfProcesses,
            newState.revision)
          timers.cancel(Tick)
          prepareRescale(newState, request, currentState.numberOfProcesses)
        }

      case Tick =>
        val sortedIdentities =
          ShardedDaemonProcessId.sortedIdentitiesFor(currentState.revision, currentState.numberOfProcesses)
        context.log.debugN(
          "Sending periodic keep alive for Sharded Daemon Process [{}] to [{}] processes (revision [{}]).",
          daemonProcessName,
          sortedIdentities.size,
          currentState.revision)
        sendKeepAliveMessages(sortedIdentities) match {
          case None =>
            timers.startSingleTimer(Tick, settings.keepAliveInterval)
            Behaviors.same
          case Some((killSwitch, futureDone)) =>
            context.pipeToSelf(futureDone)(_ => SendKeepAliveDone)
            idle(currentState, Some(killSwitch))
        }

      case SendKeepAliveDone =>
        timers.startSingleTimer(Tick, settings.keepAliveInterval)
        Behaviors.same

      case get: GetNumberOfProcesses =>
        get.replyTo ! GetNumberOfProcessesReply(
          currentState.numberOfProcesses,
          currentState.started,
          !currentState.completed,
          currentState.revision)
        Behaviors.same
    }
  }

  // we can come here from an explicit rescale request, or because a previous rescale was interrupted by the singleton
  // moving between nodes and when started anew there was an incomplete scale read from ddata
  private def prepareRescale(
      newState: ShardedDaemonProcessState,
      request: ChangeNumberOfProcesses,
      previousNumberOfProcesses: Int,
      updateRetry: Int = 0): Behavior[ShardedDaemonProcessCommand] = {
    replicatorAdapter.askUpdate(
      replyTo => Replicator.Update(key, initialState, stateWriteConsistency, replyTo)(_ => newState),
      response => InternalUpdateResponse(response))

    // wait for update to complete before applying
    receiveWhileRescaling("prepareRescale", newState) {
      case InternalUpdateResponse(_ @Replicator.UpdateSuccess(`key`)) =>
        // this is a bit weird, we use our own state, rather than the result of the update but we also know
        // there is a single(ton) writer so no surprises here
        stopAllShards(newState, Some(request), previousNumberOfProcesses)

      case InternalUpdateResponse(_ @Replicator.UpdateTimeout(`key`)) =>
        // retry
        if (updateRetry > 5) {
          context.log.warn2(
            "{}: Failed updating scale state for sharded daemon process, retrying, retry [{}]",
            daemonProcessName,
            updateRetry)
        } else {
          context.log.debug("{}: Failed updating scale state for sharded daemon process, retrying", daemonProcessName)
        }
        prepareRescale(newState, request, previousNumberOfProcesses, updateRetry + 1)

    }
  }

  private def stopAllShards(
      state: ShardedDaemonProcessState,
      request: Option[ChangeNumberOfProcesses],
      previousNumberOfProcesses: Int): Behavior[ShardedDaemonProcessCommand] = {
    val allShards =
      ShardedDaemonProcessId.allShardsFor(state.revision - 1, previousNumberOfProcesses)
    shardingRef.tell(ShardCoordinator.Internal.StopShards(allShards), shardStoppedAdapter)

    timers.startSingleTimer(ShardStopTimeout, stopShardsTimeout)
    waitForShardsStopping(state, request, previousNumberOfProcesses, allShards)
  }

  private def waitForShardsStopping(
      state: ShardedDaemonProcessState,
      request: Option[ChangeNumberOfProcesses],
      previousNumberOfProcesses: Int,
      shardsStillRunning: Set[String]): Behavior[ShardedDaemonProcessCommand] = {

    receiveWhileRescaling("waitForShardsStopping", state) {
      case ShardStopped(shard) =>
        val newShardsStillRunning = shardsStillRunning - shard
        if (newShardsStillRunning.isEmpty) {
          timers.cancel(ShardStopTimeout)
          rescalingComplete(state, request)
        } else waitForShardsStopping(state, request, previousNumberOfProcesses, newShardsStillRunning)
      case ShardStopTimeout =>
        context.log.debug2("{}: Stopping shards timed out after [{}] retrying", daemonProcessName, stopShardsTimeout)
        stopAllShards(state, request, previousNumberOfProcesses)
    }
  }

  private def rescalingComplete(
      state: ShardedDaemonProcessState,
      request: Option[ChangeNumberOfProcesses],
      ddataWritRetries: Int = 0): Behavior[ShardedDaemonProcessCommand] = {
    request.foreach(req => req.replyTo ! StatusReply.Ack)
    val newState = state.completeScaling()
    replicatorAdapter.askUpdate(
      replyTo => Replicator.Update(key, initialState, stateWriteConsistency, replyTo)((_) => newState),
      response => InternalUpdateResponse(response))

    receiveWhileRescaling("rescalingComplete", state) {
      case InternalUpdateResponse(_ @Replicator.UpdateSuccess(`key`)) =>
        context.self ! Tick
        idle(newState)

      case InternalUpdateResponse(_ @Replicator.UpdateTimeout(`key`)) =>
        // retry
        if (ddataWritRetries < 5) {
          context.log.debug(
            "{}: Failed updating scale state for sharded daemon process after completing rescale, retrying",
            daemonProcessName)
        } else {
          context.log.warn(
            "{}: Failed updating scale state for sharded daemon process after completing rescale, retry [{}]",
            daemonProcessName,
            ddataWritRetries)
        }
        rescalingComplete(newState, None, ddataWritRetries + 1)
    }
  }

  private def receiveWhileRescaling(stepName: String, state: ShardedDaemonProcessState)(
      pf: PartialFunction[ShardedDaemonProcessCommand, Behavior[ShardedDaemonProcessCommand]])
      : Behavior[ShardedDaemonProcessCommand] =
    Behaviors.receiveMessage(pf.orElse {
      case request: ChangeNumberOfProcesses =>
        request.replyTo ! StatusReply.error("Rescale in progress, retry later")
        Behaviors.same
      case get: GetNumberOfProcesses =>
        get.replyTo ! GetNumberOfProcessesReply(
          state.numberOfProcesses,
          state.started,
          !state.completed,
          state.revision)
        Behaviors.same

      case msg =>
        context.log.debugN("{}: Unexpected message in step {}: {}", daemonProcessName, stepName, msg.getClass.getName)
        Behaviors.same
    })

  private def sendKeepAliveMessages(sortedIdentities: Vector[String]): Option[(UniqueKillSwitch, Future[Done])] = {
    if (settings.keepAliveThrottleInterval == Duration.Zero) {
      sortedIdentities.foreach(id => shardingRef ! StartEntity(id))
      None
    } else {
      implicit val system: ActorSystem[_] = context.system
      Some(
        Source(sortedIdentities)
          .viaMat(KillSwitches.single)(Keep.right)
          .throttle(1, settings.keepAliveThrottleInterval)
          .toMat(Sink.foreach(id => shardingRef ! StartEntity(id)))(Keep.both)
          .run())
    }
  }
}
