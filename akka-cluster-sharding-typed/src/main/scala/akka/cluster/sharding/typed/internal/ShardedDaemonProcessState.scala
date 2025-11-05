/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import java.time.Instant

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.cluster.ddata.Key
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator
import akka.cluster.sharding.typed.ShardedDaemonProcessContext

/**
 * INTERNAL API
 */
private[akka] final case class ShardedDaemonProcessState(
    revision: Long,
    numberOfProcesses: Int,
    completed: Boolean,
    started: Instant)
    extends ReplicatedData
    with ClusterShardingTypedSerializable {
  type T = ShardedDaemonProcessState

  override def merge(that: ShardedDaemonProcessState): ShardedDaemonProcessState =
    if (this.revision == that.revision) {
      if (this.completed) this
      else that
    } else if (this.revision > that.revision)
      this
    else that

  def startScalingTo(newNumberOfProcesses: Int): ShardedDaemonProcessState =
    copy(revision = revision + 1L, completed = false, numberOfProcesses = newNumberOfProcesses, started = Instant.now())
  def completeScaling(): ShardedDaemonProcessState = copy(completed = true)

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class ShardedDaemonProcessStateKey(_id: String)
    extends Key[ShardedDaemonProcessState](_id)
    with ClusterShardingTypedSerializable {
  override def withId(newId: Key.KeyId): ShardedDaemonProcessStateKey =
    ShardedDaemonProcessStateKey(newId)
}

/**
 * INTERNAL API
 */
private[akka] object ShardedDaemonProcessState {

  val startRevision = 0L

  def initialState(initialNumberOfProcesses: Int) =
    ShardedDaemonProcessState(
      revision = ShardedDaemonProcessState.startRevision,
      numberOfProcesses = initialNumberOfProcesses,
      completed = true,
      // not quite correct but also not important, only informational
      started = Instant.now())

  def verifyRevisionBeforeStarting[T](
      behaviorFactory: ShardedDaemonProcessContext => Behavior[T]): ShardedDaemonProcessContext => Behavior[T] = {
    sdpContext =>
      Behaviors.setup { context =>
        val revision = sdpContext.revision

        if (revision == -1) {
          context.log.debug2(
            "{}: Ping from old non-rescaling node during rolling upgrade, not starting worker [{}]",
            sdpContext.name,
            sdpContext.processNumber)
          Behaviors.stopped
        } else {
          val key = ShardedDaemonProcessStateKey(sdpContext.name)
          context.log.debug2(
            "{}: Deferred start of worker to verify its revision [{}] is the latest",
            sdpContext.name,
            revision)

          // we can't anyway turn reply into T so no need for the usual adapter
          val distributedData = DistributedData(context.system)
          distributedData.replicator ! Replicator.Get(key, Replicator.ReadLocal, context.self.unsafeUpcast)
          Behaviors.receiveMessagePartial {
            case reply @ Replicator.GetSuccess(`key`) =>
              val state = reply.get(key)
              if (state.revision == revision) {
                context.log.infoN(
                  "{}: Starting Sharded Daemon Process [{}] out of a total [{}] (revision [{}])",
                  sdpContext.name,
                  sdpContext.processNumber,
                  sdpContext.totalProcesses,
                  revision)
                behaviorFactory(sdpContext).unsafeCast
              } else {
                context.log.warnN(
                  "{}: Tried to start an old revision of worker ([{}] but latest revision is [{}], started at {})",
                  sdpContext.name,
                  sdpContext.revision,
                  state.revision,
                  state.started)
                Behaviors.stopped
              }
            case Replicator.NotFound(`key`) =>
              if (revision == startRevision) {
                // No state yet but initial revision, safe
                context.log.infoN(
                  "{}: Starting Sharded Daemon Process [{}] out of a total [{}] (revision [{}] and no state found)",
                  sdpContext.name,
                  sdpContext.processNumber,
                  sdpContext.totalProcesses,
                  revision)
                behaviorFactory(sdpContext).unsafeCast
              } else {
                context.log.error2(
                  "{}: Tried to start revision [{}] of worker but no ddata state found",
                  sdpContext.name,
                  sdpContext.revision)
                Behaviors.stopped
              }
          }
        }

      }
  }

}
