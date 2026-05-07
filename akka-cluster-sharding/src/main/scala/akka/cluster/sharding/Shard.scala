/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.net.URLEncoder
import java.util

import scala.annotation.nowarn
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.DeadLetterSuppression
import akka.actor.Deploy
import akka.actor.Dropped
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Terminated
import akka.actor.Timers
import akka.annotation.InternalStableApi
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberPreparingForShutdown
import akka.cluster.ClusterEvent.MemberReadyForShutdown
import akka.cluster.sharding.ShardRegion.ShardsUpdated
import akka.cluster.sharding.internal.EntityPassivationStrategy
import akka.cluster.sharding.internal.RememberEntitiesProvider
import akka.cluster.sharding.internal.RememberEntitiesShardStore
import akka.cluster.sharding.internal.RememberEntitiesShardStore.GetEntities
import akka.cluster.sharding.internal.RememberEntityStarterManager
import akka.cluster.sharding.internal.ShardingFlightRecorder
import akka.coordination.lease.scaladsl.Lease
import akka.coordination.lease.scaladsl.LeaseProvider
import akka.event.LoggingAdapter
import akka.pattern.pipe
import akka.util.Clock
import akka.util.MessageBufferMap
import akka.util.OptionVal
import akka.util.PrettyDuration._

/**
 * INTERNAL API
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
private[akka] object Shard {
  import ShardRegion.EntityId

  /**
   * A Shard command
   */
  sealed trait RememberEntityCommand

  /**
   * When remembering entities and the entity stops without issuing a `Passivate`, we
   * restart it after a back off using this message.
   */
  final case class RestartTerminatedEntity(entity: EntityId) extends RememberEntityCommand

  /**
   * If the shard id extractor is changed, remembered entities will start in a different shard
   * and this message is sent to the shard to not leak `entityId -> RememberedButNotStarted` entries
   */
  final case class EntitiesMovedToOtherShard(ids: Set[ShardRegion.ShardId]) extends RememberEntityCommand

  /**
   * A query for information about the shard
   */
  sealed trait ShardQuery

  @SerialVersionUID(1L) case object GetCurrentShardState extends ShardQuery

  @SerialVersionUID(1L) final case class CurrentShardState(shardId: ShardRegion.ShardId, entityIds: Set[EntityId])

  @SerialVersionUID(1L) case object GetShardStats extends ShardQuery with ClusterShardingSerializable

  @SerialVersionUID(1L) final case class ShardStats(shardId: ShardRegion.ShardId, entityCount: Int)
      extends ClusterShardingSerializable

  final case class LeaseAcquireResult(acquired: Boolean, reason: Option[Throwable]) extends DeadLetterSuppression
  final case class LeaseLost(reason: Option[Throwable]) extends DeadLetterSuppression

  case object LeaseRetry extends DeadLetterSuppression
  private val LeaseRetryTimer = "lease-retry"

  def props(
      typeName: String,
      shardId: ShardRegion.ShardId,
      entityProps: String => Props,
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      handOffStopMessage: Any,
      rememberEntitiesProvider: Option[RememberEntitiesProvider],
      rememberEntityStarterManager: ActorRef): Props =
    Props(
      new Shard(
        typeName,
        shardId,
        entityProps,
        settings,
        extractEntityId,
        extractShardId,
        handOffStopMessage,
        rememberEntitiesProvider,
        rememberEntityStarterManager)).withDeploy(Deploy.local)

  case object PassivateIntervalTick extends NoSerializationVerificationNeeded

  private final case class EntityTerminated(ref: ActorRef)

  private final case class PassivationTimedOut(ref: ActorRef)

  private final case class RememberedEntityIds(ids: Set[EntityId])
  private final case class RememberEntityStoreCrashed(store: ActorRef)

  private val RememberEntityTimeoutKey = "RememberEntityTimeout"
  final case class RememberEntityTimeout(operation: RememberEntitiesShardStore.Command)

  /**
   * State machine for an entity:
   * {{{
   *                                                                       Started on another shard bc. shard id extractor changed (we need to store that)
   *                                                                +------------------------------------------------------------------+
   *                                                                |                                                                  |
   *              Entity id remembered on shard start     +-------------------------+    StartEntity or early message for entity       |
   *                   +--------------------------------->| RememberedButNotCreated |------------------------------+                   |
   *                   |                                  +-------------------------+                              |                   |
   *                   |                                                                                           |                   |
   *                   |                                                                                           |                   |
   *                   |   Remember entities                                                                       |                   |
   *                   |   message or StartEntity         +-------------------+   start stored and entity started  |                   |
   *                   |        +-----------------------> | RememberingStart  |-------------+                      v                   |
   * No state for id   |        |                         +-------------------+             |               +------------+             |
   *      +---+        |        |                                                           +-------------> |   Active   |             |
   *      |   |--------|--------+-----------------------------------------------------------+               +------------+             |
   *      +---+                 |   Non remember entities message or StartEntity                                   |                   |
   *        ^                   |                                                                                  |                   |
   *        |                   |                                                             entity terminated    |                   |
   *        |                   |      restart after backoff                                  without passivation  |    passivation    |
   *        |                   |      or message for entity    +-------------------+    remember ent.             |     initiated     \          +-------------+
   *        |                   +<------------------------------| WaitingForRestart |<---+-------------+-----------+--------------------|-------> | Passivating |
   *        |                   |                               +-------------------+    |             |                               /          +-------------+
   *        |                   |                                                        |             | remember entities             |     entity     |
   *        |                   |                                                        |             | not used                      |     terminated +--------------+
   *        |                   |                                                        |             |                               |                v              |
   *        |                   |            There were buffered messages for entity     |             |                               |      +-------------------+    |
   *        |                   +<-------------------------------------------------------+             |                               +----> |   RememberingStop |    | remember entities
   *        |                                                                                          |                                      +-------------------+    | not used
   *        |                                                                                          |                                                |              |
   *        |                                                                                          v                                                |              |
   *        +------------------------------------------------------------------------------------------+------------------------------------------------+<-------------+
   *                       stop stored/passivation complete
   * }}}
   **/
  sealed trait EntityState {
    def transition(newState: EntityState, entities: Entities): EntityState
    final def invalidTransition(to: EntityState, entities: Entities): EntityState = {
      val exception = new IllegalArgumentException(
        s"Transition from $this to $to not allowed, remember entities: ${entities.rememberingEntities}")
      if (entities.failOnIllegalTransition) {
        // crash shard
        throw exception
      } else {
        // log and ignore
        entities.log.error(exception, "Ignoring illegal state transition in shard")
        to
      }
    }
  }

  /**
   * Empty state rather than using optionals,
   * is never really kept track of but used to verify state transitions
   * and as return value instead of null
   */
  case object NoState extends EntityState {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case RememberedButNotCreated if entities.rememberingEntities => RememberedButNotCreated
      case remembering: RememberingStart                           => remembering // we go via this state even if not really remembering
      case active: Active if !entities.rememberingEntities         => active
      case _                                                       => invalidTransition(newState, entities)
    }
  }

  /**
   * In this state we know the entity has been stored in the remember sore but
   * it hasn't been created yet. E.g. on restart when first getting all the
   * remembered entity ids.
   */
  case object RememberedButNotCreated extends EntityState {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case active: Active  => active // started on this shard
      case RememberingStop => RememberingStop // started on other shard
      case _               => invalidTransition(newState, entities)
    }
  }

  object RememberingStart {
    val empty = new RememberingStart(Set.empty)

    def apply(ackTo: Option[ActorRef]): RememberingStart =
      ackTo match {
        case None        => empty
        case Some(ackTo) => RememberingStart(Set(ackTo))
      }
  }

  /**
   * When remember entities is enabled an entity is in this state while
   * its existence is being recorded in the remember entities store, or while the stop is queued up
   * to be stored in the next batch.
   */
  final case class RememberingStart(ackTo: Set[ActorRef]) extends EntityState {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case active: Active => active
      case r: RememberingStart =>
        if (ackTo.isEmpty) {
          if (r.ackTo.isEmpty) RememberingStart.empty
          else newState
        } else {
          if (r.ackTo.isEmpty) this
          else RememberingStart(ackTo.union(r.ackTo))
        }
      case _ => invalidTransition(newState, entities)
    }
  }

  /**
   * When remember entities is enabled an entity is in this state while
   * its stop is being recorded in the remember entities store, or while the stop is queued up
   * to be stored in the next batch.
   */
  case object RememberingStop extends EntityState {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case NoState => NoState
      case _       => invalidTransition(newState, entities)
    }
  }

  sealed trait WithRef extends EntityState {
    def ref: ActorRef
  }
  final case class Active(ref: ActorRef) extends WithRef {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case passivating: Passivating                 => passivating
      case WaitingForRestart                        => WaitingForRestart
      case NoState if !entities.rememberingEntities => NoState
      case _                                        => invalidTransition(newState, entities)
    }
  }
  final case class Passivating(ref: ActorRef) extends WithRef {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case RememberingStop                          => RememberingStop
      case NoState if !entities.rememberingEntities => NoState
      case _                                        => invalidTransition(newState, entities)
    }
  }

  case object WaitingForRestart extends EntityState {
    override def transition(newState: EntityState, entities: Entities): EntityState = newState match {
      case remembering: RememberingStart => remembering
      case active: Active                => active
      case _                             => invalidTransition(newState, entities)
    }
  }

  final class Entities(
      val log: LoggingAdapter,
      val rememberingEntities: Boolean,
      verboseDebug: Boolean,
      val failOnIllegalTransition: Boolean) {
    private val entities: java.util.Map[EntityId, EntityState] = new util.HashMap[EntityId, EntityState]()
    // needed to look up entity by ref when a Passivating is received
    private val byRef = new util.HashMap[ActorRef, EntityId]()
    // optimization to not have to go through all entities to find batched writes
    private val remembering = new util.HashSet[EntityId]()

    def alreadyRemembered(set: Set[EntityId]): Unit = {
      set.foreach { entityId =>
        val state = entityState(entityId).transition(RememberedButNotCreated, this)
        entities.put(entityId, state)
      }
    }
    def rememberingStart(entityId: EntityId, ackTo: Option[ActorRef]): Unit = {
      val newState = RememberingStart(ackTo)
      val state = entityState(entityId).transition(newState, this)
      entities.put(entityId, state)
      if (rememberingEntities)
        remembering.add(entityId)
    }
    def rememberingStop(entityId: EntityId): Unit = {
      val state = entityState(entityId)
      removeRefIfThereIsOne(state)
      entities.put(entityId, state.transition(RememberingStop, this))
      if (rememberingEntities)
        remembering.add(entityId)
    }
    def waitingForRestart(id: EntityId): Unit = {
      val state = entities.get(id) match {
        case wr: WithRef =>
          byRef.remove(wr.ref)
          wr
        case null  => NoState
        case other => other
      }
      entities.put(id, state.transition(WaitingForRestart, this))
    }
    def removeEntity(entityId: EntityId): Unit = {
      val state = entityState(entityId)
      // just verify transition
      state.transition(NoState, this)
      removeRefIfThereIsOne(state)
      entities.remove(entityId)
      if (rememberingEntities)
        remembering.remove(entityId)
    }
    def addEntity(entityId: EntityId, ref: ActorRef): Unit = {
      val state = entityState(entityId).transition(Active(ref), this)
      entities.put(entityId, state)
      byRef.put(ref, entityId)
      if (rememberingEntities)
        remembering.remove(entityId)
    }
    def entity(entityId: EntityId): OptionVal[ActorRef] = entities.get(entityId) match {
      case wr: WithRef => OptionVal.Some(wr.ref)
      case _           => OptionVal.None

    }
    def entityState(id: EntityId): EntityState =
      entities.get(id) match {
        case null  => NoState
        case state => state
      }

    def entityId(ref: ActorRef): OptionVal[EntityId] = OptionVal(byRef.get(ref))

    def isPassivating(id: EntityId): Boolean = {
      entities.get(id) match {
        case _: Passivating => true
        case _              => false
      }
    }
    def entityPassivating(entityId: EntityId): Unit = {
      if (verboseDebug) log.debug("[{}] passivating", entityId)
      entities.get(entityId) match {
        case wf: WithRef =>
          val state = entityState(entityId).transition(Passivating(wf.ref), this)
          entities.put(entityId, state)
        case other =>
          throw new IllegalStateException(
            s"Tried to passivate entity without an actor ref $entityId. Current state $other")
      }
    }
    private def removeRefIfThereIsOne(state: EntityState): Unit = {
      state match {
        case wr: WithRef =>
          byRef.remove(wr.ref)
        case _ =>
      }
    }

    import scala.jdk.CollectionConverters._
    // only called once during handoff
    def activeEntities(): Set[ActorRef] = byRef.keySet.asScala.toSet

    def nrActiveEntities(): Int = byRef.size()

    // only called for getting shard stats
    def activeEntityIds(): Set[EntityId] = byRef.values.asScala.toSet

    /**
     * @return (remembering start, remembering stop)
     */
    def pendingRememberEntities(): (Map[EntityId, RememberingStart], Set[EntityId]) = {
      if (remembering.isEmpty) {
        (Map.empty, Set.empty)
      } else {
        val starts = Map.newBuilder[EntityId, RememberingStart]
        val stops = Set.newBuilder[EntityId]
        remembering.forEach(entityId =>
          entityState(entityId) match {
            case r: RememberingStart => starts += (entityId -> r)
            case RememberingStop     => stops += entityId
            case wat                 => throw new IllegalStateException(s"$entityId was in the remembering set but has state $wat")
          })
        (starts.result(), stops.result())
      }
    }

    def pendingRememberedEntitiesExist(): Boolean = !remembering.isEmpty

    def entityIdExists(id: EntityId): Boolean = entities.get(id) != null

    @InternalStableApi
    def size: Int = entities.size

    override def toString: EntityId = entities.toString
  }
}

/**
 * INTERNAL API
 *
 * This actor creates children entity actors on demand that it is told to be
 * responsible for.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
@InternalStableApi
private[akka] class Shard(
    typeName: String,
    shardId: ShardRegion.ShardId,
    entityProps: String => Props,
    settings: ClusterShardingSettings,
    extractEntityId: ShardRegion.ExtractEntityId,
    @nowarn("msg=never used") extractShardId: ShardRegion.ExtractShardId,
    handOffStopMessage: Any,
    rememberEntitiesProvider: Option[RememberEntitiesProvider],
    rememberEntityStarterManager: ActorRef)
    extends Actor
    with ActorLogging
    with Stash
    with Timers {

  import Shard._
  import ShardCoordinator.Internal.HandOff
  import ShardCoordinator.Internal.ShardStopped
  import ShardRegion.EntityId
  import ShardRegion.HandOffStopper
  import ShardRegion.Passivate
  import ShardRegion.ShardInitialized

  import akka.cluster.sharding.ShardCoordinator.Internal.CoordinatorMessage

  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")

  private val rememberEntitiesStore: Option[ActorRef] =
    rememberEntitiesProvider.map { provider =>
      val store = context.actorOf(provider.shardStoreProps(shardId).withDeploy(Deploy.local), "RememberEntitiesStore")
      context.watchWith(store, RememberEntityStoreCrashed(store))
      store
    }

  private val rememberEntities: Boolean = rememberEntitiesProvider.isDefined

  @InternalStableApi
  private val entities = {
    val failOnInvalidStateTransition =
      context.system.settings.config.getBoolean("akka.cluster.sharding.fail-on-invalid-entity-state-transition")
    new Entities(log, settings.rememberEntities, verboseDebug, failOnInvalidStateTransition)
  }

  // Messages are buffered while an entity is passivating or waiting for a response
  // for that entity from the remember store
  private val messageBuffers = new MessageBufferMap[EntityId]

  private var handOffStopper: Option[ActorRef] = None
  private var preparingForShutdown = false

  private val passivationStrategy = EntityPassivationStrategy(settings, clock = () => Clock(context.system))

  import context.dispatcher
  private val passivateIntervalTask = passivationStrategy.scheduledInterval.map { interval =>
    context.system.scheduler.scheduleWithFixedDelay(interval, interval, self, PassivateIntervalTick)
  }

  private val lease = settings.leaseSettings.map { ls =>
    val leaseName =
      if (ls.leaseName.isEmpty) s"${context.system.name}-shard-$typeName-$shardId"
      else s"${ls.leaseName}-$shardId"
    LeaseProvider(context.system)
      .getLease(leaseName, ls.leaseImplementation, Cluster(context.system).selfAddress.hostPort)
  }

  private val leaseRetryInterval = settings.leaseSettings match {
    case Some(l) => l.leaseRetryInterval
    case None    => 5.seconds // not used
  }

  def receive: Receive = {
    case _ => throw new IllegalStateException("Default receive never expected to actually be used")
  }

  override def preStart(): Unit = {
    Cluster(context.system).subscribe(
      self,
      InitialStateAsEvents,
      classOf[MemberPreparingForShutdown],
      classOf[MemberReadyForShutdown])
    acquireLeaseIfNeeded()
  }

  // ===== lease handling initialization =====
  def acquireLeaseIfNeeded(): Unit = {
    lease match {
      case Some(l) =>
        tryGetLease(l)
        context.become(awaitingLease())
      case None =>
        tryLoadRememberedEntities()
    }
  }

  // Don't send back ShardInitialized so that messages are buffered in the ShardRegion
  // while awaiting the lease
  private def awaitingLease(): Receive = {
    case LeaseAcquireResult(true, _) =>
      log.debug("{}: Lease acquired", typeName)
      tryLoadRememberedEntities()
    case LeaseAcquireResult(false, None) =>
      log.error("{}: Failed to get lease for shard id [{}]. Retry in {}", typeName, shardId, leaseRetryInterval.pretty)
      timers.startSingleTimer(LeaseRetryTimer, LeaseRetry, leaseRetryInterval)
    case LeaseAcquireResult(false, Some(t)) =>
      log.error(t, "{}: Failed to get lease for shard id [{}]. Retry in {}", typeName, shardId, leaseRetryInterval)
      timers.startSingleTimer(LeaseRetryTimer, LeaseRetry, leaseRetryInterval)
    case LeaseRetry =>
      tryGetLease(lease.get)
    case ll: LeaseLost =>
      receiveLeaseLost(ll)
    case me: MemberEvent =>
      receiveMemberEvent(me)
    case msg =>
      if (verboseDebug)
        log.debug(
          "{}: Got msg of type [{}] from [{}] while waiting for lease, stashing",
          typeName,
          msg.getClass.getName,
          sender())
      stash()
  }

  private def receiveMemberEvent(event: MemberEvent): Unit = event match {
    case _: MemberReadyForShutdown | _: MemberPreparingForShutdown =>
      if (!preparingForShutdown) {
        log.info("{}: Preparing for shutdown", typeName)
        preparingForShutdown = true
      }
    case _ =>
  }

  private def tryGetLease(l: Lease): Unit = {
    log.info("{}: Acquiring lease {}", typeName, l.settings)
    pipe(l.acquire(reason => self ! LeaseLost(reason)).map(r => LeaseAcquireResult(r, None)).recover {
      case t => LeaseAcquireResult(acquired = false, Some(t))
    }).to(self)
  }

  // ===== remember entities initialization =====
  def tryLoadRememberedEntities(): Unit = {
    rememberEntitiesStore match {
      case Some(store) =>
        log.debug("{}: Waiting for load of entity ids using [{}] to complete", typeName, store)
        store ! RememberEntitiesShardStore.GetEntities
        timers.startSingleTimer(
          RememberEntityTimeoutKey,
          RememberEntityTimeout(GetEntities),
          settings.tuningParameters.waitingForStateTimeout)
        context.become(awaitingRememberedEntities())
      case None =>
        shardInitialized()
    }
  }

  def awaitingRememberedEntities(): Receive = {
    case RememberEntitiesShardStore.RememberedEntities(entityIds) =>
      timers.cancel(RememberEntityTimeoutKey)
      onEntitiesRemembered(entityIds)
    case RememberEntityTimeout(GetEntities) =>
      loadingEntityIdsFailed()
    case me: MemberEvent =>
      receiveMemberEvent(me)
    case msg =>
      if (verboseDebug)
        log.debug(
          "{}: Got msg of type [{}] from [{}] while waiting for remember entities, stashing",
          typeName,
          msg.getClass.getName,
          sender())
      stash()
  }

  def loadingEntityIdsFailed(): Unit = {
    log.error(
      "{}: Failed to load initial entity ids from remember entities store within [{}], stopping shard for backoff and restart",
      typeName,
      settings.tuningParameters.waitingForStateTimeout.pretty)
    // parent ShardRegion supervisor will notice that it terminated and will start it again, after backoff
    context.stop(self)
  }

  def onEntitiesRemembered(ids: Set[EntityId]): Unit = {
    if (ids.nonEmpty) {
      entities.alreadyRemembered(ids)
      log.debug("{}: Restarting set of [{}] entities", typeName, ids.size)
      rememberEntityStarterManager ! RememberEntityStarterManager.StartEntities(self, shardId, ids)
    }
    shardInitialized()
  }

  private def shardInitialized(): Unit = {
    log.debug("{}: Shard initialized", typeName)
    context.parent ! ShardInitialized(shardId)
    context.become(idle)
    unstashAll()
  }

  // ===== shard up and running =====

  // when not remembering entities, we stay in this state all the time
  def idle: Receive = {
    case Terminated(ref)                         => receiveTerminated(ref)
    case me: MemberEvent                         => receiveMemberEvent(me)
    case EntityTerminated(ref)                   => entityTerminated(ref)
    case msg: CoordinatorMessage                 => receiveCoordinatorMessage(msg)
    case msg: RememberEntityCommand              => receiveRememberEntityCommand(msg)
    case msg: ShardRegion.StartEntity            => startEntity(msg.entityId, Some(sender()))
    case msg: ShardRegion.SetActiveEntityLimit   => activeEntityLimitUpdated(msg)
    case msg: ShardRegion.ShardsUpdated          => shardsUpdated(msg)
    case Passivate(stopMessage)                  => passivate(sender(), stopMessage)
    case PassivateIntervalTick                   => passivateEntitiesAfterInterval()
    case PassivationTimedOut(ref)                => passivationTimedOut(ref)
    case msg: ShardQuery                         => receiveShardQuery(msg)
    case msg: LeaseLost                          => receiveLeaseLost(msg)
    case msg: RememberEntityStoreCrashed         => rememberEntityStoreCrashed(msg)
    case msg if extractEntityId.isDefinedAt(msg) => deliverMessage(msg, sender())
  }

  def rememberUpdate(add: Set[EntityId] = Set.empty, remove: Set[EntityId] = Set.empty): Unit = {
    rememberEntitiesStore match {
      case None =>
        onUpdateDone(add, remove)
      case Some(store) =>
        sendToRememberStore(store, storingStarts = add, storingStops = remove)
    }
  }

  def sendToRememberStore(store: ActorRef, storingStarts: Set[EntityId], storingStops: Set[EntityId]): Unit = {
    if (verboseDebug)
      log.debug(
        "{}: Remember update [{}] and stops [{}] triggered",
        typeName,
        storingStarts.mkString(", "),
        storingStops.mkString(", "))

    storingStarts.foreach { entityId =>
      ShardingFlightRecorder.rememberEntityAdd(entityId)
    }
    storingStops.foreach { id =>
      ShardingFlightRecorder.rememberEntityRemove(id)
    }

    val startTimeNanos = System.nanoTime()
    val update = RememberEntitiesShardStore.Update(started = storingStarts, stopped = storingStops)
    store ! update
    timers.startSingleTimer(
      RememberEntityTimeoutKey,
      RememberEntityTimeout(update),
      settings.tuningParameters.updatingStateTimeout)

    context.become(waitingForRememberEntitiesStore(update, startTimeNanos))
  }

  private def waitingForRememberEntitiesStore(
      update: RememberEntitiesShardStore.Update,
      startTimeNanos: Long): Receive = {
    // none of the current impls will send back a partial update, yet!
    case RememberEntitiesShardStore.UpdateDone(storedStarts, storedStops) =>
      val duration = System.nanoTime() - startTimeNanos
      if (verboseDebug)
        log.debug(
          "{}: Update done for ids, started [{}], stopped [{}]. Duration {} ms",
          typeName,
          storedStarts.mkString(", "),
          storedStops.mkString(", "),
          duration.nanos.toMillis)
      ShardingFlightRecorder.rememberEntityOperation(duration)
      timers.cancel(RememberEntityTimeoutKey)
      onUpdateDone(storedStarts, storedStops)

    case RememberEntityTimeout(`update`) =>
      log.error("{}: Remember entity store did not respond, restarting shard", typeName)
      throw new RuntimeException(
        s"Async write timed out after ${settings.tuningParameters.updatingStateTimeout.pretty}")
    case ShardRegion.StartEntity(entityId) => startEntity(entityId, Some(sender()))
    case me: MemberEvent                   => receiveMemberEvent(me)
    case Terminated(ref)                   => receiveTerminated(ref)
    case EntityTerminated(ref)             => entityTerminated(ref)
    case _: CoordinatorMessage             => stash()
    case cmd: RememberEntityCommand        => receiveRememberEntityCommand(cmd)
    case l: LeaseLost                      => receiveLeaseLost(l)
    case ShardRegion.Passivate(stopMessage) =>
      if (verboseDebug)
        log.debug(
          "{}: Passivation of [{}] arrived while updating",
          typeName,
          entities.entityId(sender()).getOrElse(s"Unknown actor ${sender()}"))
      passivate(sender(), stopMessage)
    case msg: ShardQuery                 => receiveShardQuery(msg)
    case PassivateIntervalTick           => stash()
    case PassivationTimedOut(ref)        => passivationTimedOut(ref)
    case msg: RememberEntityStoreCrashed => rememberEntityStoreCrashed(msg)
    case msg: ShardsUpdated              => shardsUpdated(msg)
    case msg if extractEntityId.isDefinedAt(msg) =>
      deliverMessage(msg, sender())
    case msg =>
      // shouldn't be any other message types, but just in case
      log.warning(
        "{}: Stashing unexpected message [{}] while waiting for remember entities update of starts [{}], stops [{}]",
        typeName,
        msg.getClass.getName,
        update.started.mkString(", "),
        update.stopped.mkString(", "))
      stash()

  }

  def onUpdateDone(starts: Set[EntityId], stops: Set[EntityId]): Unit = {
    // entities can contain both ids from start/stops and pending ones, so we need
    // to mark the completed ones as complete to get the set of pending ones
    starts.foreach { entityId =>
      val stateBeforeStart = entities.entityState(entityId)
      // this will start the entity and transition the entity state in sessions to active
      getOrCreateEntity(entityId)
      sendMsgBuffer(entityId)
      stateBeforeStart match {
        case RememberingStart(ackTo) => ackTo.foreach(_ ! ShardRegion.StartEntityAck(entityId, shardId))
        case _                       =>
      }
    }
    stops.foreach { entityId =>
      entities.entityState(entityId) match {
        case RememberingStop =>
          // this updates entity state
          passivateCompleted(entityId)
        case state =>
          throw new IllegalStateException(
            s"Unexpected state [$state] when storing stop completed for entity id [$entityId]")
      }
    }

    val (pendingStarts, pendingStops) = entities.pendingRememberEntities()
    if (pendingStarts.isEmpty && pendingStops.isEmpty) {
      if (verboseDebug) log.debug("{}: Update complete, no pending updates, going to idle", typeName)
      unstashAll()
      context.become(idle)
    } else {
      // Note: no unstashing as long as we are batching, is that a problem?
      val pendingStartIds = pendingStarts.keySet
      if (verboseDebug)
        log.debug(
          "{}: Update complete, pending updates, doing another write. Starts [{}], stops [{}]",
          typeName,
          pendingStartIds.mkString(", "),
          pendingStops.mkString(", "))
      rememberUpdate(pendingStartIds, pendingStops)
    }
  }

  def receiveLeaseLost(msg: LeaseLost): Unit = {
    // The shard region will re-create this when it receives a message for this shard
    log.error(
      "{}: Shard id [{}] lease lost, stopping shard and killing [{}] entities.{}",
      typeName,
      shardId,
      entities.size,
      msg.reason match {
        case Some(reason) => s" Reason for losing lease: $reason"
        case None         => ""
      })
    // Stop entities ASAP rather than send termination message
    context.stop(self)
  }

  private def receiveRememberEntityCommand(msg: RememberEntityCommand): Unit = msg match {
    case RestartTerminatedEntity(entityId) =>
      entities.entityState(entityId) match {
        case WaitingForRestart =>
          if (verboseDebug) log.debug("{}: Restarting entity unexpectedly terminated entity [{}]", typeName, entityId)
          getOrCreateEntity(entityId)
        case Active(_) =>
          // it up could already have been started, that's fine
          if (verboseDebug)
            log.debug("{}: Got RestartTerminatedEntity for [{}] but it is already running", typeName, entityId)
        case other =>
          throw new IllegalStateException(
            s"Unexpected state for [$entityId] when getting RestartTerminatedEntity: [$other]")
      }

    case EntitiesMovedToOtherShard(movedEntityIds) =>
      log.info(
        "{}: Clearing [{}] remembered entities started elsewhere because of changed shard id extractor",
        typeName,
        movedEntityIds.size)
      movedEntityIds.foreach { entityId =>
        entities.entityState(entityId) match {
          case RememberedButNotCreated =>
            entities.rememberingStop(entityId)
          case other =>
            throw new IllegalStateException(s"Unexpected state for [$entityId] when getting ShardIdsMoved: [$other]")
        }
      }
      rememberUpdate(remove = movedEntityIds)
  }

  // this could be because of a start message or due to a new message for the entity
  // if it is a start entity then start entity ack is sent after it is created
  private def startEntity(entityId: EntityId, ackTo: Option[ActorRef]): Unit = {
    entities.entityState(entityId) match {
      case Active(_) =>
        if (verboseDebug)
          log.debug("{}: Request to start entity [{}] (Already started)", typeName, entityId)
        ackTo.foreach(_ ! ShardRegion.StartEntityAck(entityId, shardId))
      case _: RememberingStart =>
        entities.rememberingStart(entityId, ackTo)
      case state @ (RememberedButNotCreated | WaitingForRestart) =>
        // already remembered or waiting for backoff to restart, just start it -
        // this is the normal path for initially remembered entities getting started
        log.debug("{}: Request to start entity [{}] (in state [{}])", typeName, entityId, state)
        getOrCreateEntity(entityId)
        ackTo.foreach(_ ! ShardRegion.StartEntityAck(entityId, shardId))
      case Passivating(_) =>
        // since StartEntity is handled in deliverMsg we can buffer a StartEntity to handle when
        // passivation completes (triggering an immediate restart)
        messageBuffers.append(entityId, ShardRegion.StartEntity(entityId), ackTo.getOrElse(ActorRef.noSender))

      case RememberingStop =>
        // Optimally: if stop is already write in progress, we want to stash, if it is batched for later write we'd want to cancel
        // but for now
        stash()
      case NoState =>
        if (entities.pendingRememberedEntitiesExist()) {
          // No actor running and write in progress for some other entity id (can only happen with remember entities enabled)
          log.debug("{}: Request to start entity [{}] postponed, remember entity write in progress", typeName, entityId)
          entities.rememberingStart(entityId, ackTo = ackTo)
        } else {
          // started manually from the outside, or the shard id extractor was changed since the entity was remembered
          // we need to store that it was started
          log.debug("{}: Request to start entity [{}] and ack to [{}]", typeName, entityId, ackTo)
          entities.rememberingStart(entityId, ackTo)
          rememberUpdate(add = Set(entityId))
        }
    }
  }

  private def receiveCoordinatorMessage(msg: CoordinatorMessage): Unit = msg match {
    case HandOff(`shardId`) =>
      handOff(sender())
    case HandOff(shard) =>
      log.warning("{}: Shard [{}] can not hand off for another Shard [{}]", typeName, shardId, shard)
    case _ => unhandled(msg)
  }

  def receiveShardQuery(msg: ShardQuery): Unit = msg match {
    case GetCurrentShardState =>
      if (verboseDebug)
        log.debug(
          "{}: GetCurrentShardState, full state: [{}], active: [{}]",
          typeName,
          entities,
          entities.activeEntityIds())
      sender() ! CurrentShardState(shardId, entities.activeEntityIds())
    case GetShardStats => sender() ! ShardStats(shardId, entities.size)
  }

  private def handOff(replyTo: ActorRef): Unit = handOffStopper match {
    case Some(_) => log.warning("{}: HandOff shard [{}] received during existing handOff", typeName, shardId)
    case None =>
      log.debug("{}: HandOff shard [{}]", typeName, shardId)

      // does conversion so only do once
      val activeEntities = entities.activeEntities()
      if (preparingForShutdown) {
        log.info("{}: HandOff shard [{}] while preparing for shutdown. Stopping right away.", typeName, shardId)
        activeEntities.foreach { _ ! handOffStopMessage }
        replyTo ! ShardStopped(shardId)
        context.stop(self)
      } else if (activeEntities.nonEmpty && !preparingForShutdown) {
        log.debug(
          "{}: Starting HandOffStopper for shard [{}] to terminate [{}] entities.",
          typeName,
          shardId,
          activeEntities.size)
        activeEntities.foreach(context.unwatch)
        handOffStopper = Some(
          context.watch(
            context.actorOf(
              HandOffStopper.props(
                typeName,
                shardId,
                replyTo,
                activeEntities,
                handOffStopMessage,
                settings.tuningParameters.handOffTimeout),
              "HandOffStopper")))

        //During hand off we only care about watching for termination of the hand off stopper
        context.become {
          case Terminated(ref) => receiveTerminated(ref)
        }
      } else {
        replyTo ! ShardStopped(shardId)
        context.stop(self)
      }
  }

  private def receiveTerminated(ref: ActorRef): Unit = {
    if (handOffStopper.contains(ref))
      context.stop(self)
  }

  @InternalStableApi
  def entityTerminated(ref: ActorRef): Unit = {
    import settings.tuningParameters._
    entities.entityId(ref) match {
      case OptionVal.Some(entityId) =>
        passivationStrategy.entityTerminated(entityId)
        entities.entityState(entityId) match {
          case RememberingStop =>
            if (verboseDebug)
              log.debug("{}: Stop of [{}] arrived, already is among the pending stops", typeName, entityId)
          case Active(_) =>
            if (rememberEntitiesStore.isDefined) {
              log.debug("{}: Entity [{}] stopped without passivating, will restart after backoff", typeName, entityId)
              entities.waitingForRestart(entityId)
              val msg = RestartTerminatedEntity(entityId)
              timers.startSingleTimer(msg, msg, entityRestartBackoff)
            } else {
              log.debug("{}: Entity [{}] terminated", typeName, entityId)
              entities.removeEntity(entityId)
            }

          case Passivating(_) =>
            timers.cancel(PassivationTimedOut(ref))
            if (rememberEntitiesStore.isDefined) {
              if (entities.pendingRememberedEntitiesExist()) {
                // will go in next batch update
                if (verboseDebug)
                  log.debug(
                    "{}: [{}] terminated after passivating, arrived while updating, adding it to batch of pending stops",
                    typeName,
                    entityId)
                entities.rememberingStop(entityId)
              } else {
                entities.rememberingStop(entityId)
                rememberUpdate(remove = Set(entityId))
              }
            } else {
              if (messageBuffers.getOrEmpty(entityId).nonEmpty) {
                if (verboseDebug)
                  log.debug(
                    "{}: [{}] terminated after passivating, buffered messages found, restarting",
                    typeName,
                    entityId)
                entities.removeEntity(entityId)
                getOrCreateEntity(entityId)
                sendMsgBuffer(entityId)
              } else {
                if (verboseDebug)
                  log.debug("{}: [{}] terminated after passivating", typeName, entityId)
                entities.removeEntity(entityId)
              }
            }
          case unexpected =>
            val ref = entities.entity(entityId)
            log.warning(
              "{}: Got a terminated for [{}], entityId [{}] which is in unexpected state [{}]",
              typeName,
              ref,
              entityId,
              unexpected)
        }
      case _ =>
        log.warning("{}: Unexpected entity terminated: {}", typeName, ref)
    }
  }

  private def passivate(entity: ActorRef, stopMessage: Any): Unit = {
    entities.entityId(entity) match {
      case OptionVal.Some(id) =>
        if (entities.isPassivating(id)) {
          log.debug(
            "{}: Passivation already in progress for [{}]. Not sending stopMessage back to entity",
            typeName,
            id)
        } else if (messageBuffers.getOrEmpty(id).nonEmpty) {
          log.debug("{}: Passivation when there are buffered messages for [{}], ignoring passivation", typeName, id)
        } else {
          if (verboseDebug)
            log.debug("{}: Passivation started for [{}]", typeName, id)
          entities.entityPassivating(id)
          entity ! stopMessage

          val passivationTimeout = PassivationTimedOut(entity)
          timers.startSingleTimer(
            passivationTimeout,
            passivationTimeout,
            settings.tuningParameters.passivationStopTimeout)
          ShardingFlightRecorder.entityPassivate(id)
        }
      case _ =>
        log.debug("{}: Unknown entity passivating [{}]. Not sending stopMessage back to entity", typeName, entity)
    }
  }

  private def passivationTimedOut(entity: ActorRef): Unit = {
    entities.entityId(entity) match {
      case OptionVal.Some(id) =>
        if (entities.isPassivating(id)) {
          log.info("{}: Passivation of entity [{}] timed out. Stopping it.", typeName, id)
          context.stop(entity)
        }
      case _ =>
        log.debug("{}: Unknown entity passivation timeout [{}].", typeName, entity)
    }
  }

  private def activeEntityLimitUpdated(updated: ShardRegion.SetActiveEntityLimit): Unit = {
    val entitiesToPassivate = passivationStrategy.limitUpdated(updated.perRegionLimit)
    passivateEntities(entitiesToPassivate)
  }

  private def shardsUpdated(updated: ShardRegion.ShardsUpdated): Unit = {
    val entitiesToPassivate = passivationStrategy.shardsUpdated(updated.activeShards)
    passivateEntities(entitiesToPassivate)
  }

  private def passivateEntitiesAfterInterval(): Unit = {
    val entitiesToPassivate = passivationStrategy.intervalPassed()
    passivateEntities(entitiesToPassivate)
  }

  private def passivateEntities(entitiesToPassivate: EntityPassivationStrategy.PassivateEntities): Unit = {
    if (entitiesToPassivate.nonEmpty) {
      val refsToPassivate = entitiesToPassivate.collect {
        case entityId if entities.entity(entityId).isDefined => entities.entity(entityId).get
      }
      if (refsToPassivate.nonEmpty) {
        log.debug("{}: Passivating [{}] entities", typeName, refsToPassivate.size)
        refsToPassivate.foreach(passivate(_, handOffStopMessage))
      }
    }
  }

  // After entity stopped
  def passivateCompleted(entityId: EntityId): Unit = {
    val hasBufferedMessages = messageBuffers.getOrEmpty(entityId).nonEmpty
    entities.removeEntity(entityId)
    if (hasBufferedMessages) {
      log.debug(
        "{}: Entity stopped after passivation [{}], but will be started again due to buffered messages",
        typeName,
        entityId)
      ShardingFlightRecorder.entityPassivateRestart(entityId)
      if (rememberEntities) {
        // trigger start or batch in case we're already writing to the remember store
        entities.rememberingStart(entityId, None)
        if (!entities.pendingRememberedEntitiesExist()) rememberUpdate(Set(entityId))
      } else {
        getOrCreateEntity(entityId)
        sendMsgBuffer(entityId)
      }
    } else {
      log.debug("{}: Entity stopped after passivation [{}]", typeName, entityId)
    }
  }

  private def deliverMessage(msg: Any, snd: ActorRef): Unit = {
    val (entityId, payload) = extractEntityId(msg)
    if (entityId == null || entityId == "") {
      log.warning("{}: Id must not be empty, dropping message [{}]", typeName, msg.getClass.getName)
      context.system.deadLetters ! Dropped(msg, "No recipient entity id", snd, self)
    } else {
      payload match {
        case start: ShardRegion.StartEntity =>
          // Handling StartEntity both here and in the receives allows for sending it both as is and in an envelope
          // to be extracted by the entity id extractor.

          // we can only start a new entity if we are not currently waiting for another write
          if (entities.pendingRememberedEntitiesExist()) {
            if (verboseDebug)
              log.debug("{}: StartEntity({}) from [{}], adding to batch", typeName, start.entityId, snd)
            entities.rememberingStart(entityId, ackTo = Some(snd))
          } else {
            if (verboseDebug)
              log.debug("{}: StartEntity({}) from [{}], starting", typeName, start.entityId, snd)
            startEntity(start.entityId, Some(snd))
          }
        case _ =>
          entities.entityState(entityId) match {
            case Active(ref) =>
              if (verboseDebug)
                log.debug("{}: Delivering message of type [{}] to [{}]", typeName, payload.getClass.getName, entityId)
              val entitiesToPassivate = passivationStrategy.entityTouched(entityId)
              ref.tell(payload, snd)
              passivateEntities(entitiesToPassivate)
            case RememberingStart(_) | RememberingStop | Passivating(_) =>
              appendToMessageBuffer(entityId, msg, snd)
            case state @ (WaitingForRestart | RememberedButNotCreated) =>
              if (verboseDebug)
                log.debug(
                  "{}: Delivering message of type [{}] to [{}] (starting because [{}])",
                  typeName,
                  payload.getClass.getName,
                  entityId,
                  state)
              getOrCreateEntity(entityId).tell(payload, snd)
            case NoState =>
              if (!rememberEntities) {
                // don't buffer if remember entities not enabled
                val ref = getOrCreateEntity(entityId)
                val entitiesToPassivate = passivationStrategy.entityTouched(entityId)
                ref.tell(payload, snd)
                passivateEntities(entitiesToPassivate)
              } else {
                if (entities.pendingRememberedEntitiesExist()) {
                  // No actor running and write in progress for some other entity id (can only happen with remember entities enabled)
                  if (verboseDebug)
                    log.debug(
                      "{}: Buffer message [{}] to [{}] (which is not started) because of write in progress for [{}]",
                      typeName,
                      payload.getClass.getName,
                      entityId,
                      entities.pendingRememberEntities())
                  appendToMessageBuffer(entityId, msg, snd)
                  entities.rememberingStart(entityId, ackTo = None)
                } else {
                  // No actor running and no write in progress, start actor and deliver message when started
                  if (verboseDebug)
                    log.debug(
                      "{}: Buffering message [{}] to [{}] and starting actor",
                      typeName,
                      payload.getClass.getName,
                      entityId)
                  appendToMessageBuffer(entityId, msg, snd)
                  entities.rememberingStart(entityId, ackTo = None)
                  rememberUpdate(add = Set(entityId))
                }
              }
          }
      }
    }
  }

  @InternalStableApi
  def getOrCreateEntity(id: EntityId): ActorRef = {
    entities.entity(id) match {
      case OptionVal.Some(child) => child
      case _                     =>
        // if we're remembering entities, there's a chance we have a timer to restart, which is now moot
        if (rememberEntitiesStore.isDefined && entities.entityState(id) == WaitingForRestart) {
          timers.cancel(RestartTerminatedEntity(id))
        }

        val name = URLEncoder.encode(id, "utf-8")
        val a = context.actorOf(entityProps(id), name)
        context.watchWith(a, EntityTerminated(a))
        log.debug("{}: Started entity [{}] with entity id [{}] in shard [{}]", typeName, a, id, shardId)
        entities.addEntity(id, a)
        entityCreated(id)
        a
    }
  }

  /**
   * Called when an entity has been created. Returning the number
   * of active entities.
   */
  @InternalStableApi
  def entityCreated(@nowarn("msg=never used") id: EntityId): Int = entities.nrActiveEntities()

  // ===== buffering while busy saving a start or stop when remembering entities =====
  def appendToMessageBuffer(id: EntityId, msg: Any, snd: ActorRef): Unit = {
    if (messageBuffers.totalSize >= settings.tuningParameters.bufferSize) {
      if (log.isDebugEnabled)
        log.debug(
          "{}: Buffer is full, dropping message of type [{}] for entity [{}]",
          typeName,
          msg.getClass.getName,
          id)
      context.system.deadLetters ! Dropped(msg, s"Buffer for [$id] is full", snd, self)
    } else {
      if (log.isDebugEnabled)
        log.debug("{}: Message of type [{}] for entity [{}] buffered", typeName, msg.getClass.getName, id)
      messageBuffers.append(id, msg, snd)
    }
  }

  // After entity started
  def sendMsgBuffer(entityId: EntityId): Unit = {
    //Get the buffered messages and remove the buffer
    val messages = messageBuffers.getOrEmpty(entityId)
    messageBuffers.remove(entityId)

    if (messages.nonEmpty) {
      getOrCreateEntity(entityId)
      log.debug("{}: Sending message buffer for entity [{}] ([{}] messages)", typeName, entityId, messages.size)
      // Now there is no deliveryBuffer we can try to redeliver
      // and as the child exists, the message will be directly forwarded
      messages.foreach {
        case (ShardRegion.StartEntity(entityId), snd) => startEntity(entityId, Some(snd))
        case (msg, snd)                               => deliverMessage(msg, snd)
      }
    }
  }

  def dropBufferFor(entityId: EntityId, reason: String): Unit = {
    val count = messageBuffers.drop(entityId, reason, context.system.deadLetters)
    if (log.isDebugEnabled && count > 0) {
      log.debug("{}: Dropping [{}] buffered messages for [{}] because {}", typeName, count, entityId, reason)
    }
  }

  private def rememberEntityStoreCrashed(msg: RememberEntityStoreCrashed): Unit = {
    throw new RuntimeException(s"Remember entities store [${msg.store}] crashed")
  }

  override def postStop(): Unit = {
    passivateIntervalTask.foreach(_.cancel())
    lease.foreach(_.release())
    log.debug("{}: Shard [{}] shutting down", typeName, shardId)
  }

}
