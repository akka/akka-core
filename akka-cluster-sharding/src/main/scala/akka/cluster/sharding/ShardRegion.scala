/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.net.URLEncoder

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.runtime.AbstractFunction1
import scala.util.{ Failure, Success }

import akka.Done
import akka.actor._
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.annotation.ApiMayChange
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.ClusterSettings
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.sharding.ClusterShardingSettings.PassivationStrategy
import akka.cluster.sharding.Shard.ShardStats
import akka.cluster.sharding.internal.ClusterShardingInstrumentationProvider
import akka.cluster.sharding.internal.RememberEntitiesProvider
import akka.cluster.sharding.internal.RememberEntityStarterManager
import akka.event.Logging
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.MessageBufferMap
import akka.util.Timeout

/**
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
object ShardRegion {

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardRegion]] actor.
   */
  private[akka] def props(
      typeName: String,
      entityProps: String => Props,
      settings: ClusterShardingSettings,
      coordinatorPath: String,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      handOffStopMessage: Any,
      rememberEntitiesProvider: Option[RememberEntitiesProvider]): Props =
    Props(
      new ShardRegion(
        typeName,
        Some(entityProps),
        dataCenter = None,
        settings,
        coordinatorPath,
        extractEntityId,
        extractShardId,
        handOffStopMessage,
        rememberEntitiesProvider)).withDeploy(Deploy.local)

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardRegion]] actor
   * when using it in proxy only mode.
   */
  private[akka] def proxyProps(
      typeName: String,
      dataCenter: Option[DataCenter],
      settings: ClusterShardingSettings,
      coordinatorPath: String,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): Props =
    Props(
      new ShardRegion(
        typeName,
        None,
        dataCenter,
        settings,
        coordinatorPath,
        extractEntityId,
        extractShardId,
        PoisonPill,
        None)).withDeploy(Deploy.local)

  /**
   * Marker type of entity identifier (`String`).
   */
  type EntityId = String

  /**
   * Marker type of shard identifier (`String`).
   */
  type ShardId = String

  /**
   * Marker type of application messages (`Any`).
   */
  type Msg = Any

  /**
   * Interface of the partial function used by the [[ShardRegion]] to
   * extract the entity id and the message to send to the entity from an
   * incoming message. The implementation is application specific.
   * If the partial function does not match the message will be
   * `unhandled`, i.e. posted as `Unhandled` messages on the event stream.
   * Note that the extracted  message does not have to be the same as the incoming
   * message to support wrapping in message envelope that is unwrapped before
   * sending to the entity actor.
   */
  type ExtractEntityId = PartialFunction[Msg, (EntityId, Msg)]

  /**
   * Interface of the function used by the [[ShardRegion]] to
   * extract the shard id from an incoming message.
   * Only messages that passed the [[ExtractEntityId]] will be used
   * as input to this function.
   */
  type ExtractShardId = Msg => ShardId

  /**
   * Java API: Interface of functions to extract entity id,
   * shard id, and the message to send to the entity from an
   * incoming message.
   */
  trait MessageExtractor {

    /**
     * Extract the entity id from an incoming `message`. If `null` is returned
     * the message will be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
     */
    def entityId(message: Any): String

    /**
     * Extract the message to send to the entity from an incoming `message`.
     * Note that the extracted message does not have to be the same as the incoming
     * message to support wrapping in message envelope that is unwrapped before
     * sending to the entity actor.
     */
    def entityMessage(message: Any): Any

    /**
     * Extract the shard id from an incoming `message`. Only messages that passed the [[#entityId]]
     * function will be used as input to this function.
     */
    def shardId(message: Any): String
  }

  object HashCodeMessageExtractor {

    /** INTERNAL API */
    @InternalApi
    private[sharding] def shardId(id: String, maxNumberOfShards: Int): String = {
      // It would be better to have abs(id.hashCode % maxNumberOfShards), see issue #25034
      // but to avoid getting different values when rolling upgrade we keep the old way,
      // and it doesn't have any serious consequences
      (math.abs(id.hashCode) % maxNumberOfShards).toString
    }
  }

  /**
   * Convenience implementation of [[ShardRegion.MessageExtractor]] that
   * construct `shardId` based on the `hashCode` of the `entityId`. The number
   * of unique shards is limited by the given `maxNumberOfShards`.
   */
  abstract class HashCodeMessageExtractor(maxNumberOfShards: Int) extends MessageExtractor {

    /**
     * Default implementation pass on the message as is.
     */
    override def entityMessage(message: Any): Any = message

    override def shardId(message: Any): String = {
      val id = message match {
        case ShardRegion.StartEntity(entityId) => entityId
        case _                                 => entityId(message)
      }
      HashCodeMessageExtractor.shardId(id, maxNumberOfShards)
    }
  }

  sealed trait ShardRegionCommand

  /**
   * If the state of the entities are persistent you may stop entities that are not used to
   * reduce memory consumption. This is done by the application specific implementation of
   * the entity actors for example by defining receive timeout (`context.setReceiveTimeout`).
   * If a message is already enqueued to the entity when it stops itself the enqueued message
   * in the mailbox will be dropped. To support graceful passivation without losing such
   * messages the entity actor can send this `Passivate` message to its parent `ShardRegion`.
   * The specified wrapped `stopMessage` will be sent back to the entity, which is
   * then supposed to stop itself. Incoming messages will be buffered by the `ShardRegion`
   * between reception of `Passivate` and termination of the entity. Such buffered messages
   * are thereafter delivered to a new incarnation of the entity.
   *
   * [[akka.actor.PoisonPill]] is a perfectly fine `stopMessage`.
   */
  @SerialVersionUID(1L) final case class Passivate(stopMessage: Any) extends ShardRegionCommand

  /*
   * Send this message to the `ShardRegion` actor to handoff all shards that are hosted by
   * the `ShardRegion` and then the `ShardRegion` actor will be stopped. You can `watch`
   * it to know when it is completed.
   */
  @SerialVersionUID(1L) case object GracefulShutdown extends ShardRegionCommand

  /**
   * Java API: Send this message to the `ShardRegion` actor to handoff all shards that are hosted by
   * the `ShardRegion` and then the `ShardRegion` actor will be stopped. You can `watch`
   * it to know when it is completed.
   */
  def gracefulShutdownInstance = GracefulShutdown

  /** INTERNAL API */
  @InternalApi private case object GracefulShutdownTimeout extends ShardRegionCommand

  /**
   * We must be sure that a shard is initialized before to start send messages to it.
   * Shard could be terminated during initialization.
   */
  final case class ShardInitialized(shardId: ShardId)

  sealed trait ShardRegionQuery

  /**
   * Send this message to the `ShardRegion` actor to request for [[CurrentRegions]],
   * which contains the addresses of all registered regions.
   *
   * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
   * the state of the shard regions.
   */
  @SerialVersionUID(1L) case object GetCurrentRegions extends ShardRegionQuery with ClusterShardingSerializable

  /**
   * Java API: Send this message to the `ShardRegion` actor to request for [[CurrentRegions]],
   * which contains the addresses of all registered regions.
   *
   * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
   * the state of the shard regions.
   */
  def getCurrentRegionsInstance: GetCurrentRegions.type = GetCurrentRegions

  /**
   * Reply to `GetCurrentRegions`
   */
  @SerialVersionUID(1L) final case class CurrentRegions(regions: Set[Address]) extends ClusterShardingSerializable {

    /**
     * Java API
     */
    def getRegions: java.util.Set[Address] = {
      import scala.jdk.CollectionConverters._
      regions.asJava
    }

  }

  /**
   * Send this message to the `ShardRegion` actor to request for [[ClusterShardingStats]],
   * which contains statistics about the currently running sharded entities in the
   * entire cluster. If the `timeout` is reached without answers from all shard regions
   * the reply will contain an empty map of regions.
   *
   * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
   * the state of the shard regions.
   */
  @SerialVersionUID(1L) case class GetClusterShardingStats(timeout: FiniteDuration)
      extends ShardRegionQuery
      with ClusterShardingSerializable

  /**
   * Reply to [[GetClusterShardingStats]], contains statistics about all the sharding regions
   * in the cluster.
   */
  @SerialVersionUID(1L) final case class ClusterShardingStats(regions: Map[Address, ShardRegionStats])
      extends ClusterShardingSerializable {

    /**
     * Java API
     */
    def getRegions(): java.util.Map[Address, ShardRegionStats] = {
      import scala.jdk.CollectionConverters._
      regions.asJava
    }
  }

  /**
   * Send this message to the `ShardRegion` actor to request for [[ShardRegionStats]],
   * which contains statistics about the currently running sharded entities in the
   * entire region.
   *
   * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
   * the state of the shard regions.
   *
   * For the statistics for the entire cluster, see [[GetClusterShardingStats$]].
   */
  @SerialVersionUID(1L) case object GetShardRegionStats extends ShardRegionQuery with ClusterShardingSerializable

  /**
   * Java API:
   */
  def getRegionStatsInstance = GetShardRegionStats

  /**
   *
   * @param stats the region stats mapping of `ShardId` to number of entities
   * @param failed set of shards if any failed to respond within the timeout
   */
  @SerialVersionUID(1L) final class ShardRegionStats(val stats: Map[ShardId, Int], val failed: Set[ShardId])
      extends ClusterShardingSerializable
      with Product {

    /**
     * Java API
     */
    def getStats(): java.util.Map[ShardId, Integer] = {
      import scala.jdk.CollectionConverters._
      stats.map { case (k, v) => k -> Integer.valueOf(v) }.asJava
    }

    /** Java API */
    def getFailed(): java.util.Set[ShardId] = {
      import scala.jdk.CollectionConverters._
      failed.asJava
    }

    // For binary compatibility
    def this(stats: Map[ShardId, Int]) = this(stats, Set.empty[ShardId])
    private[sharding] def copy(stats: Map[ShardId, Int] = stats): ShardRegionStats =
      new ShardRegionStats(stats, this.failed)

    // For binary compatibility: class conversion from case class
    override def equals(other: Any): Boolean = other match {
      case o: ShardRegionStats => o.stats == stats && o.failed == failed
      case _                   => false
    }
    override def hashCode: Int = stats.## + failed.##
    override def toString: String = s"ShardRegionStats[stats=$stats, failed=$failed]"
    override def productArity: Int = 2
    override def productElement(n: Int) =
      if (n == 0) stats else if (n == 1) failed else throw new NoSuchElementException
    override def canEqual(o: Any): Boolean = o.isInstanceOf[ShardRegionStats]

  }
  // For binary compatibility
  object ShardRegionStats extends AbstractFunction1[Map[ShardId, Int], ShardRegionStats] {
    def apply(stats: Map[ShardId, Int]): ShardRegionStats =
      apply(stats, Set.empty[ShardId])
    def apply(stats: Map[ShardId, Int], failed: Set[ShardId]): ShardRegionStats =
      new ShardRegionStats(stats, failed)
    def unapply(stats: ShardRegionStats): Option[Map[ShardId, Int]] =
      Option(stats.stats)
  }

  /**
   * Send this message to a `ShardRegion` actor instance to request a
   * [[CurrentShardRegionState]] which describes the current state of the region.
   * The state contains information about what shards are running in this region
   * and what entities are running on each of those shards.
   */
  @SerialVersionUID(1L) case object GetShardRegionState extends ShardRegionQuery with ClusterShardingSerializable

  /**
   * Java API:
   */
  def getShardRegionStateInstance = GetShardRegionState

  /**
   * Reply to [[GetShardRegionState$]]
   *
   * If gathering the shard information times out the set of shards will be empty.
   */
  @SerialVersionUID(1L) final class CurrentShardRegionState(val shards: Set[ShardState], val failed: Set[ShardId])
      extends ClusterShardingSerializable
      with Product {

    /**
     * Java API:
     *
     * If gathering the shard information times out the set of shards will be empty.
     */
    def getShards(): java.util.Set[ShardState] = {
      import scala.jdk.CollectionConverters._
      shards.asJava
    }

    /** Java API */
    def getFailed(): java.util.Set[ShardId] = {
      import scala.jdk.CollectionConverters._
      failed.asJava
    }

    // For binary compatibility
    def this(shards: Set[ShardState]) = this(shards, Set.empty[ShardId])
    private[sharding] def copy(shards: Set[ShardState] = shards): CurrentShardRegionState =
      new CurrentShardRegionState(shards, this.failed)

    // For binary compatibility: class conversion from case class
    override def equals(other: Any): Boolean = other match {
      case o: CurrentShardRegionState => o.shards == shards && o.failed == failed
      case _                          => false
    }
    override def hashCode: Int = shards.## + failed.##
    override def toString: String = s"CurrentShardRegionState[shards=$shards, failed=$failed]"
    override def productArity: Int = 2
    override def productElement(n: Int) =
      if (n == 0) shards else if (n == 1) failed else throw new NoSuchElementException
    override def canEqual(o: Any): Boolean = o.isInstanceOf[CurrentShardRegionState]
  }
  // For binary compatibility
  object CurrentShardRegionState extends AbstractFunction1[Set[ShardState], CurrentShardRegionState] {
    def apply(shards: Set[ShardState]): CurrentShardRegionState =
      apply(shards, Set.empty[ShardId])
    def apply(shards: Set[ShardState], failed: Set[ShardId]): CurrentShardRegionState =
      new CurrentShardRegionState(shards, failed)
    def unapply(state: CurrentShardRegionState): Option[Set[ShardState]] =
      Option(state.shards)
  }

  @SerialVersionUID(1L) final case class ShardState(shardId: ShardId, entityIds: Set[EntityId]) {

    /**
     * Java API:
     */
    def getEntityIds(): java.util.Set[EntityId] = {
      import scala.jdk.CollectionConverters._
      entityIds.asJava
    }
  }

  /**
   * INTERNAL API
   *
   * Discover if the shard region is registered with the coordinator.
   * Not serializable as only to be sent to the local shard region
   * Response is [[ShardRegionState]]
   */
  @InternalApi
  private[akka] object GetShardRegionStatus extends ShardRegionQuery

  /**
   * INTERNAL API
   *
   * Status of a ShardRegion. Only for local requests so not serializable.
   */
  @InternalApi
  private[akka] final class ShardRegionStatus(val typeName: String, val registeredWithCoordinator: Boolean)

  /**
   * Periodic tick to run some house-keeping.
   * This message is continuously sent to `self` using a timer configured with `retryInterval`.
   */
  private case object Retry extends ShardRegionCommand

  /**
   * Similar to Retry but used only when ShardRegion is starting and when we detect that
   * the coordinator is moving.
   *
   * This is to ensure that a ShardRegion can register as soon as possible while the
   * ShardCoordinator is in the process of recovering its state.
   *
   * This message is sent to `self` using a interval lower then [[Retry]] (higher frequency).
   * The interval increases exponentially until it equals `retryInterval` in which case
   * we stop to schedule it and let [[Retry]] take over.
   */
  private case object RegisterRetry extends ShardRegionCommand

  /**
   * When an remembering entities and the shard stops unexpected (e.g. persist failure), we
   * restart it after a back off using this message.
   */
  private final case class RestartShard(shardId: ShardId)

  /**
   * When remembering entities and a shard is started, each entity id that needs to
   * be running will trigger this message being sent through sharding. For this to work
   * the message *must* be handled by the shard id extractor.
   */
  final case class StartEntity(entityId: EntityId) extends ClusterShardingSerializable

  /**
   * Sent back when a `ShardRegion.StartEntity` message was received and triggered the entity
   * to start (it does not guarantee the entity successfully started)
   */
  final case class StartEntityAck(entityId: EntityId, shardId: ShardRegion.ShardId)
      extends ClusterShardingSerializable
      with DeadLetterSuppression

  /**
   * API MAY CHANGE: Messages for passivation strategies may change after additional testing and feedback.
   *
   * When limit-based automatic passivation is enabled, set a new active entity limit for a shard region.
   */
  @ApiMayChange
  final case class SetActiveEntityLimit(perRegionLimit: Int)

  /**
   * INTERNAL API
   *
   * Updated shard details sent to all active shards when a shard is started or stopped.
   * Used for passivation strategies that change limits based on the number of active shards.
   */
  @InternalApi
  private[akka] final case class ShardsUpdated(activeShards: Int) extends DeadLetterSuppression

  /**
   * INTERNAL API. Sends stopMessage (e.g. `PoisonPill`) to the entities and when all of
   * them have terminated it replies with `ShardStopped`.
   * If the entities don't terminate after `handoffTimeout` it will try stopping them forcefully.
   */
  @InternalApi private[akka] class HandOffStopper(
      typeName: String,
      shard: String,
      replyTo: ActorRef,
      entities: Set[ActorRef],
      stopMessage: Any,
      handoffTimeout: FiniteDuration)
      extends Actor
      with ActorLogging
      with Timers {
    import HandOffStopper._
    import ShardCoordinator.Internal.ShardStopped

    val (entityHandOffTimeout, effectiveStopTimeoutWarningAfter) =
      if (CoordinatedShutdown(context.system).getShutdownReason().isEmpty) {
        val eht = (handoffTimeout - 5.seconds).max(1.seconds)
        eht -> StopTimeoutWarningAfter
      } else {
        val shutdownTimeout = CoordinatedShutdown(context.system).totalTimeout()
        val eht = (handoffTimeout.min(shutdownTimeout - 1.second) - 5.seconds).max(1.second)
        val estwa = (eht * 0.75).min(StopTimeoutWarningAfter).toMillis.millis
        eht -> estwa
      }

    timers.startSingleTimer(StopTimeoutWarning, StopTimeoutWarning, effectiveStopTimeoutWarningAfter)
    timers.startSingleTimer(StopTimeout, StopTimeout, entityHandOffTimeout)

    entities.foreach { a =>
      context.watch(a)
      a ! stopMessage
    }

    var remaining = entities

    def receive: Receive = {
      case Terminated(ref) =>
        remaining -= ref
        if (remaining.isEmpty) {
          replyTo ! ShardStopped(shard)
          context.stop(self)
        }

      case StopTimeoutWarning =>
        log.warning(
          s"$typeName: [${remaining.size}] of the entities in shard [{}] not stopped after [{}]. " +
          "Maybe the handOffStopMessage [{}] is not handled? {}",
          shard,
          effectiveStopTimeoutWarningAfter.toCoarsest,
          stopMessage.getClass.getName,
          s"Waiting additional [${(entityHandOffTimeout - effectiveStopTimeoutWarningAfter).toCoarsest}] before stopping the remaining entities.")

      case StopTimeout =>
        log.warning(
          s"$typeName: HandOffStopMessage [{}] is not handled by some of the entities in shard [{}] after [{}], " +
          "stopping the remaining [{}] entities.",
          stopMessage.getClass.getName,
          shard,
          entityHandOffTimeout.toCoarsest,
          remaining.size)
        timers.cancel(StopTimeoutWarning)

        remaining.foreach { ref =>
          context.stop(ref)
        }

    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] object HandOffStopper {
    private case object StopTimeout
    private case object StopTimeoutWarning

    private val StopTimeoutWarningAfter: FiniteDuration = 5.seconds

    def props(
        typeName: String,
        shard: String,
        replyTo: ActorRef,
        entities: Set[ActorRef],
        stopMessage: Any,
        handoffTimeout: FiniteDuration): Props =
      Props(new HandOffStopper(typeName, shard, replyTo, entities, stopMessage, handoffTimeout))
        .withDeploy(Deploy.local)
  }

}

/**
 * INTERNAL API
 *
 * This actor creates children shard actors on demand that it is told to be responsible for.
 * The shard actors in turn create entity actors on demand.
 * It delegates messages targeted to other shards to the responsible
 * `ShardRegion` actor on other nodes.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
@InternalStableApi
private[akka] class ShardRegion(
    typeName: String,
    entityProps: Option[String => Props],
    dataCenter: Option[DataCenter],
    settings: ClusterShardingSettings,
    coordinatorPath: String,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId,
    handOffStopMessage: Any,
    rememberEntitiesProvider: Option[RememberEntitiesProvider])
    extends Actor
    with Timers {

  import ShardCoordinator.Internal._
  import ShardRegion._
  import ShardingQueries.ShardsQueryResult
  import settings._
  import settings.tuningParameters._

  val log = Logging.withMarker(context.system, this)

  val cluster = Cluster(context.system)

  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")

  val scope = "shard_region"
  private val instrumentation = ClusterShardingInstrumentationProvider.get(context.system).instrumentation

  // sort by age, oldest first
  val ageOrdering = Member.ageOrdering
  // membersByAge is only used for tracking where coordinator is running
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)
  // membersByAge contains members with these status
  private val memberStatusOfInterest: Set[MemberStatus] =
    Set(MemberStatus.Up, MemberStatus.Leaving, MemberStatus.Exiting)

  var regions = Map.empty[ActorRef, Set[ShardId]]
  var regionByShard = Map.empty[ShardId, ActorRef]
  var shardBuffers = new MessageBufferMap[ShardId]
  var loggedFullBufferWarning = false
  var shards = Map.empty[ShardId, ActorRef]
  var shardsByRef = Map.empty[ActorRef, ShardId]
  var startingShards = Set.empty[ShardId]
  var handingOff = Set.empty[ActorRef]
  var gracefulShutdownInProgress = false
  var preparingForShutdown = false

  import context.dispatcher
  var retryCount = 0
  val initRegistrationDelay: FiniteDuration = 100.millis.max(retryInterval / 2 / 2 / 2)
  var nextRegistrationDelay: FiniteDuration = initRegistrationDelay

  private val rememberEntityStarterManager =
    if (rememberEntitiesProvider.isDefined)
      context.actorOf(RememberEntityStarterManager.props(context.self, settings), "RememberEntityStarter")
    else
      context.system.deadLetters

  // for CoordinatedShutdown
  val gracefulShutdownProgress = Promise[Done]()
  CoordinatedShutdown(context.system)
    .addTask(CoordinatedShutdown.PhaseClusterShardingShutdownRegion, "region-shutdown") { () =>
      if (cluster.isTerminated || cluster.selfMember.status == MemberStatus.Down) {
        Future.successful(Done)
      } else {
        self ! GracefulShutdown
        gracefulShutdownProgress.future
      }
    }

  // subscribe to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    timers.startTimerWithFixedDelay(Retry, Retry, retryInterval)
    startRegistration()
    logPassivationStrategy()
  }

  override def postStop(): Unit = {
    super.postStop()
    log.debug("{}: Region stopped", typeName)
    coordinator.foreach(_ ! RegionStopped(context.self))
    cluster.unsubscribe(self)
    gracefulShutdownProgress.trySuccess(Done)
  }

  private def logPassivationStrategy(): Unit = {
    if (settings.passivationStrategySettings.oldSettingUsed) {
      log.warning(
        "The `akka.cluster.sharding.passivate-idle-entity-after` setting and associated methods are deprecated. " +
        "Use the `akka.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` setting instead. " +
        "See the documentation and reference config for more information on automatic passivation strategies.")
    }
    if (settings.rememberEntities) {
      log.debug("{}: Entities will not be passivated automatically because 'rememberEntities' is enabled.", typeName)
    } else {
      log.info(
        "{}: Automatic entity passivation: {}",
        typeName,
        PassivationStrategy.describe(settings.passivationStrategy))
    }
  }

  // when using proxy the data center can be different from the own data center
  private val targetDcRole = dataCenter match {
    case Some(t) => ClusterSettings.DcRolePrefix + t
    case None    => ClusterSettings.DcRolePrefix + cluster.settings.SelfDataCenter
  }

  def matchingCoordinatorRole(member: Member): Boolean =
    member.hasRole(targetDcRole) && coordinatorSingletonRole.forall(member.hasRole)

  /**
   * When leaving the coordinator singleton is started rather quickly on next
   * oldest node and therefore it is good to send the Register and GracefulShutdownReq to
   * the likely locations of the coordinator.
   */
  def coordinatorSelection: List[ActorSelection] = {
    @tailrec def select(result: List[Member], remaining: immutable.SortedSet[Member]): List[Member] = {
      if (remaining.isEmpty)
        result
      else {
        val m = remaining.head
        if (m.status == MemberStatus.Up)
          m :: result
        else
          select(m :: result, remaining.tail)
      }
    }

    select(Nil, membersByAge).map(m => context.actorSelection(RootActorPath(m.address).toString + coordinatorPath))
  }

  var coordinator: Option[ActorRef] = None
  def coordinatorAddress: Option[Address] =
    coordinator.map { coordRef =>
      val a = coordRef.path.address

      if (a.hasLocalScope) cluster.selfMember.address
      else a
    }

  def coordinatorStatus(members: immutable.SortedSet[Member]): Option[MemberStatus] =
    coordinatorAddress.flatMap { coordAddress =>
      members.find(_.address == coordAddress).map(_.status)
    }

  def reRegisterIfCoordinatorNotUp(): Unit =
    if (coordinator.nonEmpty) {
      coordinatorStatus(membersByAge) match {
        case Some(MemberStatus.Up) => () // Do nothing
        case Some(notUp) =>
          if (log.isDebugEnabled)
            log.debug(
              "{}: Coordinator is on node with status [{}], proactively attempting to find next coordinator",
              typeName,
              notUp)

          // For now, attempt to register with the oldest Up member we know about without forgetting
          // about the current coordinator
          // We only attempt one candidate so as to not flood with registration messages
          coordinatorSelection.headOption.foreach(sendRegistrationMessage)

          if (!timers.isTimerActive(RegisterRetry)) {
            nextRegistrationDelay = initRegistrationDelay

            scheduleNextRegistration()
          }

        case None =>
          // coordinator is on node which has been removed... can this actually happen?
          log.warning(
            "{}: Coordinator was on removed node [{}], attempting to re-register",
            typeName,
            coordinatorAddress.get) // safe: guarded by nonEmpty
          coordinator = None
          startRegistration()
      }
    }

  def changeMembers(newMembers: immutable.SortedSet[Member]): Unit = {
    val oldMembers = membersByAge
    val before = oldMembers.headOption
    val after = newMembers.headOption
    membersByAge = newMembers
    // NB: equaliity check is on uniqueAddress, not status etc.
    if (before != after) {
      if (log.isDebugEnabled)
        log.debug(
          "{}: Coordinator moved from [{}] to [{}]",
          typeName,
          before.map(_.address).getOrElse(""),
          after.map(_.address).getOrElse(""))
      coordinator = None
      startRegistration()
    } else if (coordinator.isEmpty) {
      // NB: resets registration retry backoff, but the situation has changed
      startRegistration()
    } else if (coordinatorStatus(oldMembers) != coordinatorStatus(newMembers)) {
      // coordinator status changed
      reRegisterIfCoordinatorNotUp()
    }
  }

  def receive: Receive = {
    case Terminated(ref)                         => receiveTerminated(ref)
    case ShardInitialized(shardId)               => initializeShard(shardId, sender())
    case evt: ClusterDomainEvent                 => receiveClusterEvent(evt)
    case state: CurrentClusterState              => receiveClusterState(state)
    case msg: CoordinatorMessage                 => receiveCoordinatorMessage(msg)
    case cmd: ShardRegionCommand                 => receiveCommand(cmd)
    case query: ShardRegionQuery                 => receiveQuery(query)
    case msg: RestartShard                       => deliverMessage(msg, sender())
    case msg: StartEntity                        => deliverStartEntity(msg, sender())
    case msg: SetActiveEntityLimit               => deliverToAllShards(msg, sender())
    case msg: CoordinatorCommand                 => deliverToCoordinator(msg, sender())
    case msg if extractEntityId.isDefinedAt(msg) => deliverMessage(msg, sender())
    case unknownMsg =>
      log.warning("{}: Message does not have an extractor defined in shard so it was ignored: {}", typeName, unknownMsg)
  }

  def receiveClusterState(state: CurrentClusterState): Unit = {
    changeMembers(
      immutable.SortedSet
        .empty(ageOrdering)
        .union(state.members.filter(m => memberStatusOfInterest(m.status) && matchingCoordinatorRole(m))))
  }

  def receiveClusterEvent(evt: ClusterDomainEvent): Unit = evt match {
    case MemberUp(m) =>
      addMember(m)

    case MemberLeft(m) =>
      // updates the status in the set
      addMember(m)

    case MemberExited(m) =>
      // updates the status in the set
      addMember(m)

    case MemberRemoved(m, _) =>
      if (m.uniqueAddress == cluster.selfUniqueAddress)
        context.stop(self)
      else if (matchingCoordinatorRole(m))
        changeMembers(membersByAge.filterNot(_.uniqueAddress == m.uniqueAddress))

    case MemberDowned(m) =>
      if (m.uniqueAddress == cluster.selfUniqueAddress) {
        log.info("{}: Self downed, stopping ShardRegion [{}]", typeName, self.path)
        context.stop(self)
      }

    case _: MemberReadyForShutdown | _: MemberPreparingForShutdown =>
      if (!preparingForShutdown) {
        log.info("{}. preparing for shutdown", typeName)
      }
      preparingForShutdown = true

    case _: MemberEvent => // these are expected, no need to warn about them

    case _ => unhandled(evt)
  }

  private def addMember(m: Member): Unit = {
    if (matchingCoordinatorRole(m) && memberStatusOfInterest(m.status)) {
      // replace, it's possible that the status, or upNumber is changed
      changeMembers(membersByAge.filterNot(_.uniqueAddress == m.uniqueAddress) + m)
    }
  }

  def receiveCoordinatorMessage(msg: CoordinatorMessage): Unit = msg match {
    case HostShard(shard) =>
      if (gracefulShutdownInProgress) {
        log.debug("{}: Ignoring Host Shard request for [{}] as region is shutting down", typeName, shard)

        // if the coordinator is sending HostShard to a region that is shutting down
        // it means that it missed the shutting down message (coordinator moved?)
        // we want to inform it as soon as possible so it doesn't keep trying to allocate the shard here
        sendGracefulShutdownToCoordinatorIfInProgress()

      } else {
        log.debug("{}: Host Shard [{}] ", typeName, shard)
        regionByShard = regionByShard.updated(shard, self)
        regions = regions.updated(self, regions.getOrElse(self, Set.empty) + shard)

        //Start the shard, if already started this does nothing
        getShard(shard)
        sender() ! ShardStarted(shard)
      }

    case ShardHome(shard, shardRegionRef) =>
      receiveShardHome(shard, shardRegionRef)

    case ShardHomes(homes) =>
      if (log.isDebugEnabled)
        log.debug("Got shard homes for regions [{}]", homes.keySet.mkString(", "))
      homes.foreach {
        case (shardRegionRef, shards) =>
          shards.foreach(shardId => receiveShardHome(shardId, shardRegionRef))
      }

    case RegisterAck(coord) =>
      context.watch(coord)
      coordinator = Some(coord)
      finishRegistration()
      tryRequestShardBufferHomes()

    case BeginHandOff(shard) =>
      log.debug("{}: BeginHandOff shard [{}]", typeName, shard)
      if (!preparingForShutdown) {
        if (regionByShard.contains(shard)) {
          val regionRef = regionByShard(shard)
          val updatedShards = regions(regionRef) - shard
          if (updatedShards.isEmpty) regions -= regionRef
          else regions = regions.updated(regionRef, updatedShards)
          regionByShard -= shard
        }
        sender() ! BeginHandOffAck(shard)
      } else {
        log.debug("{}: Ignoring begin handoff of shard [{}] as preparing to shutdown", typeName, shard)
      }

    case msg @ HandOff(shard) =>
      log.debug("{}: HandOff shard [{}]", typeName, shard)

      // must drop requests that came in between the BeginHandOff and now,
      // because they might be forwarded from other regions and there
      // is a risk or message re-ordering otherwise
      if (shardBuffers.contains(shard)) {
        val dropped = shardBuffers
          .drop(shard, "Avoiding reordering of buffered messages at shard handoff", context.system.deadLetters)
        if (dropped > 0) {
          log.warning(
            "{}: Dropping [{}] buffered messages to shard [{}] during hand off to avoid re-ordering",
            typeName,
            dropped,
            shard)
          // better to decrease by "dropped" to avoid calculating the size?
          instrumentation.shardBufferSize(scope, typeName, shardBuffers.size)
        }
        loggedFullBufferWarning = false
      }

      if (shards.contains(shard)) {
        handingOff += shards(shard)
        shards(shard).forward(msg)
      } else
        sender() ! ShardStopped(shard)

    case _ => unhandled(msg)

  }

  def receiveCommand(cmd: ShardRegionCommand): Unit = cmd match {

    case Retry =>
      // retryCount is used to avoid flooding the logs
      // it's used inside register() whenever shardBuffers.nonEmpty
      // therefore we update it if needed on each Retry msg
      // the reason why it's updated here is because we don't want to increase it on each RegisterRetry, only on Retry
      if (shardBuffers.nonEmpty) retryCount += 1

      // we depend on the coordinator each time, if empty we need to register
      // otherwise we can try to deliver some buffered messages
      if (coordinator.isEmpty) register()
      else {
        // Note: we do try to deliver buffered messages even in the middle of
        // a graceful shutdown every message that we manage to deliver is a win
        tryRequestShardBufferHomes()
      }

      // eventually, also re-trigger a graceful shutdown if one is in progress
      sendGracefulShutdownToCoordinatorIfInProgress()
      tryCompleteGracefulShutdownIfInProgress()

    case RegisterRetry =>
      if (coordinator.isEmpty) {
        register()
        scheduleNextRegistration()
      } else reRegisterIfCoordinatorNotUp()

    case GracefulShutdown =>
      if (preparingForShutdown) {
        log.debug(
          "{}: Skipping graceful shutdown of region and all its shards as cluster is preparing for shutdown",
          typeName)
        gracefulShutdownProgress.trySuccess(Done)
        context.stop(self)
      } else {
        log.debug("{}: Starting graceful shutdown of region and all its shards", typeName)

        val coordShutdown = CoordinatedShutdown(context.system)
        if (coordShutdown.getShutdownReason().isPresent) {
          // use a shorter timeout than the coordinated shutdown phase to be able to log better reason for the timeout
          val timeout = coordShutdown.timeout(CoordinatedShutdown.PhaseClusterShardingShutdownRegion) - 1.second
          if (timeout > Duration.Zero) {
            timers.startSingleTimer(GracefulShutdownTimeout, GracefulShutdownTimeout, timeout)
          }
        }

        gracefulShutdownInProgress = true
        sendGracefulShutdownToCoordinatorIfInProgress()
        tryCompleteGracefulShutdownIfInProgress()
      }

    case GracefulShutdownTimeout =>
      log.warning(
        "{}: Graceful shutdown of shard region timed out, region will be stopped. Remaining shards [{}], " +
        "remaining buffered messages [{}].",
        typeName,
        shards.keysIterator.mkString(","),
        shardBuffers.totalSize)
      context.stop(self)

    case _ => unhandled(cmd)
  }

  def receiveQuery(query: ShardRegionQuery): Unit = query match {
    case GetCurrentRegions =>
      coordinator match {
        case Some(c) => c.forward(GetCurrentRegions)
        case None    => sender() ! CurrentRegions(Set.empty)
      }

    case msg: GetClusterShardingStats =>
      coordinator.fold(sender() ! ClusterShardingStats(Map.empty))(_.forward(msg))

    case GetShardRegionState =>
      replyToRegionStateQuery(sender())

    case GetShardRegionStats =>
      replyToRegionStatsQuery(sender())

    case GetShardRegionStatus =>
      sender() ! new ShardRegionStatus(typeName, coordinator.isDefined)

    case null => unhandled(query)
  }

  def receiveTerminated(ref: ActorRef): Unit = {
    if (coordinator.contains(ref)) {
      coordinator = None
      startRegistration()
    } else if (regions.contains(ref)) {
      val shards = regions(ref)
      regionByShard --= shards
      regions -= ref
      if (log.isDebugEnabled) {
        if (verboseDebug)
          log.debug(
            "{}: Region [{}] terminated with [{}] shards [{}]",
            typeName,
            ref,
            shards.size,
            shards.mkString(", "))
        else
          log.debug("{}: Region [{}] terminated with [{}] shards", typeName, ref, shards.size)
      }
    } else if (shardsByRef.contains(ref)) {
      val shardId: ShardId = shardsByRef(ref)

      shardsByRef = shardsByRef - ref
      shards = shards - shardId
      startingShards -= shardId
      if (settings.passivationStrategy != ClusterShardingSettings.NoPassivationStrategy) {
        shards.values.foreach(_ ! ShardsUpdated(shards.size))
      }
      if (handingOff.contains(ref)) {
        handingOff = handingOff - ref
        log.debug("{}: Shard [{}] handoff complete", typeName, shardId)
      } else {
        // if persist fails it will stop
        log.debug("{}: Shard [{}]  terminated while not being handed off", typeName, shardId)
        if (rememberEntities) {
          context.system.scheduler.scheduleOnce(shardFailureBackoff, self, RestartShard(shardId))
        }
      }

      // did this shard get removed because the ShardRegion is shutting down?
      // If so, we can try to speed-up the region shutdown. We don't need to wait for the next tick.
      tryCompleteGracefulShutdownIfInProgress()
    }
  }

  def receiveShardHome(shard: ShardId, shardRegionRef: ActorRef): Unit = {
    log.debug("{}: Shard [{}] located at [{}]", typeName, shard, shardRegionRef)
    regionByShard.get(shard) match {
      case Some(r) if r == self && shardRegionRef != self =>
        // should not happen, inconsistency between ShardRegion and ShardCoordinator
        throw new IllegalStateException(
          s"$typeName: Unexpected change of shard [$shard] from self to [$shardRegionRef]")
      case _ =>
    }
    regionByShard = regionByShard.updated(shard, shardRegionRef)
    regions = regions.updated(shardRegionRef, regions.getOrElse(shardRegionRef, Set.empty) + shard)

    if (shardRegionRef != self)
      context.watch(shardRegionRef)

    if (shardRegionRef == self)
      getShard(shard).foreach(deliverBufferedMessages(shard, _))
    else
      deliverBufferedMessages(shard, shardRegionRef)
  }

  def replyToRegionStateQuery(ref: ActorRef): Unit = {
    queryShards[Shard.CurrentShardState](shards, Shard.GetCurrentShardState)
      .map { qr =>
        CurrentShardRegionState(
          qr.responses.map(state => ShardRegion.ShardState(state.shardId, state.entityIds)).toSet,
          qr.failed)
      }
      .pipeTo(ref)
  }

  def replyToRegionStatsQuery(ref: ActorRef): Unit = {
    queryShards[ShardStats](shards, Shard.GetShardStats)
      .map { qr =>
        ShardRegionStats(qr.responses.map(stats => (stats.shardId, stats.entityCount)).toMap, qr.failed)
      }
      .pipeTo(ref)
  }

  /**
   * Query all or a subset of shards, e.g. unresponsive shards that initially timed out.
   * If the number of `shards` are less than this.shards.size, this could be a retry.
   * Returns a partitioned set of any shards that may have not replied within the
   * timeout and shards that did reply, to provide retry on only that subset.
   *
   * Logs a warning if any of the group timed out.
   *
   * To check subset unresponsive: {{{ queryShards[T](shards.filterKeys(u.contains), shardQuery) }}}
   */
  def queryShards[T: ClassTag](shards: Map[ShardId, ActorRef], msg: Any): Future[ShardsQueryResult[T]] = {
    implicit val timeout: Timeout = settings.shardRegionQueryTimeout

    Future.traverse(shards.toSeq) { case (shardId, shard) => askOne(shard, msg, shardId) }.map { ps =>
      val qr = ShardsQueryResult[T](ps, this.shards.size, timeout.duration)
      if (qr.failed.nonEmpty) log.warning(s"{}: $qr", typeName)
      qr
    }
  }

  private def askOne[T: ClassTag](shard: ActorRef, msg: Any, shardId: ShardId)(
      implicit timeout: Timeout): Future[Either[ShardId, T]] =
    (shard ? msg).mapTo[T].transform {
      case Success(t) => Success(Right(t))
      case Failure(_) => Success(Left(shardId))
    }

  private def tryCompleteGracefulShutdownIfInProgress(): Unit =
    if (gracefulShutdownInProgress && shards.isEmpty && shardBuffers.isEmpty) {
      log.debug("{}: Completed graceful shutdown of region.", typeName)
      context.stop(self) // all shards have been rebalanced, complete graceful shutdown
    }

  def startRegistration(): Unit = {
    nextRegistrationDelay = initRegistrationDelay

    register()
    scheduleNextRegistration()
  }

  def scheduleNextRegistration(): Unit = {
    if (nextRegistrationDelay < retryInterval) {
      timers.startSingleTimer(RegisterRetry, RegisterRetry, nextRegistrationDelay)
      // exponentially increasing retry interval until reaching the normal retryInterval
      nextRegistrationDelay *= 2
    }
  }

  def finishRegistration(): Unit = {
    timers.cancel(RegisterRetry)
  }

  def register(): Unit = {
    val actorSelections = coordinatorSelection
    actorSelections.foreach(sendRegistrationMessage)
    if (shardBuffers.nonEmpty && retryCount >= 5) {
      if (actorSelections.nonEmpty) {
        val coordinatorMessage =
          if (cluster.state.unreachable(membersByAge.head)) s"Coordinator [${membersByAge.head}] is unreachable."
          else s"Coordinator [${membersByAge.head}] is reachable."
        val bufferSize = shardBuffers.totalSize
        if (bufferSize > 0) {
          if (log.isWarningEnabled) {
            log.warning(
              "{}: Trying to register to coordinator at [{}], but no acknowledgement. Total [{}] buffered messages. [{}]",
              typeName,
              actorSelections.mkString(", "),
              bufferSize,
              coordinatorMessage)
          }
        } else if (log.isDebugEnabled) {
          log.debug(
            "{}: Trying to register to coordinator at [{}], but no acknowledgement. No buffered messages yet. [{}]",
            typeName,
            actorSelections.mkString(", "),
            coordinatorMessage)
        }
      } else {
        // Members start off as "Removed"
        val partOfCluster = cluster.selfMember.status != MemberStatus.Removed
        val possibleReason =
          if (partOfCluster)
            "Has Cluster Sharding been started on every node and nodes been configured with the correct role(s)?"
          else
            "Probably, no seed-nodes configured and manual cluster or bootstrap join not performed?"

        val bufferSize = shardBuffers.totalSize
        if (bufferSize > 0) {
          log.warning(
            "{}: No coordinator found to register. {} Total [{}] buffered messages.",
            typeName,
            possibleReason,
            bufferSize)
        } else {
          log.debug("{}: No coordinator found to register. {} No buffered messages yet.", typeName, possibleReason)
        }

      }
    }
  }

  def sendRegistrationMessage(selection: ActorSelection): Unit = {
    selection ! registrationMessage
  }

  def registrationMessage: Any =
    if (entityProps.isDefined) Register(self) else RegisterProxy(self)

  /**
   * Send GetShardHome for all shards with buffered messages
   * If coordinator is empty, nothing happens
   */
  def tryRequestShardBufferHomes(): Unit = {
    coordinator.foreach { coord =>
      // Have to use vars because MessageBufferMap has no map, only foreach
      var totalBuffered = 0
      var shards = List.empty[String]
      shardBuffers.foreach {
        case (shard, buf) =>
          totalBuffered += buf.size
          shards ::= shard
          log.debug(
            "{}: Requesting shard home for [{}] from coordinator at [{}]. [{}] buffered messages.",
            typeName,
            shard,
            coord,
            buf.size)
          coord ! GetShardHome(shard)
      }

      if (retryCount >= 5 && retryCount % 5 == 0 && log.isWarningEnabled) {
        log.warning(
          "{}: Requested shard homes [{}] from coordinator at [{}]. [{}] total buffered messages.",
          typeName,
          shards.sorted.mkString(","),
          coord,
          totalBuffered)
      }
    }
  }

  def initializeShard(id: ShardId, shard: ActorRef): Unit = {
    log.debug("{}: Shard was initialized [{}]", typeName, id)
    startingShards -= id
    deliverBufferedMessages(id, shard)
  }

  def bufferMessage(shardId: ShardId, msg: Any, snd: ActorRef) = {
    val totBufSize = shardBuffers.totalSize
    if (totBufSize >= bufferSize) {
      if (loggedFullBufferWarning)
        log.debug("{}: Buffer is full, dropping message for shard [{}]", typeName, shardId)
      else {
        log.warning("{}: Buffer is full, dropping message for shard [{}]", typeName, shardId)
        loggedFullBufferWarning = true
      }
      context.system.deadLetters ! msg
    } else {
      shardBuffers.append(shardId, msg, snd)
      instrumentation.incrementShardBufferSize(scope, typeName)
      // log some insight to how buffers are filled up every 10% of the buffer capacity
      val tot = totBufSize + 1
      if (tot % (bufferSize / 10) == 0) {
        val logMsg = s"$typeName: ShardRegion is using [${100.0 * tot / bufferSize} %] of its buffer capacity."
        if (tot <= bufferSize / 2)
          log.info(logMsg)
        else
          log.warning(
            logMsg + " The coordinator might not be available. You might want to check cluster membership status.")
      }
    }
  }

  def deliverBufferedMessages(shardId: ShardId, receiver: ActorRef): Unit = {
    if (shardBuffers.contains(shardId)) {
      val buf = shardBuffers.getOrEmpty(shardId)
      log.debug("{}: Deliver [{}] buffered messages for shard [{}]", typeName, buf.size, shardId)

      buf.foreach {
        case (msg, snd) =>
          msg match {
            case msg @ RestartShard(_) if receiver != self =>
              log.debug(
                "{}: Dropping buffered message {}, these are only processed by a local ShardRegion.",
                typeName,
                msg)
            case _ =>
              receiver.tell(msg, snd)
          }
      }

      shardBuffers.remove(shardId)
      instrumentation.shardBufferSize(scope, typeName, shardBuffers.totalSize)
    }
    loggedFullBufferWarning = false
    retryCount = 0
  }

  def deliverStartEntity(msg: StartEntity, snd: ActorRef): Unit = {
    try {
      deliverMessage(msg, snd)
    } catch {
      case ex: MatchError =>
        log.error(
          ex,
          "{}: When using remember-entities the shard id extractor must handle ShardRegion.StartEntity(id).",
          typeName)
    }
  }

  def deliverToAllShards(msg: Any, snd: ActorRef): Unit =
    shards.values.foreach(_.tell(msg, snd))

  def deliverToCoordinator(msg: CoordinatorCommand, snd: ActorRef): Unit =
    coordinator.foreach(_.tell(msg, snd))

  def deliverMessage(msg: Any, snd: ActorRef): Unit =
    msg match {
      case RestartShard(shardId) =>
        regionByShard.get(shardId) match {
          case Some(ref) =>
            if (ref == self)
              getShard(shardId)
          case None =>
            if (!shardBuffers.contains(shardId)) {
              log.debug("{}: Request shard [{}] home. Coordinator [{}]", typeName, shardId, coordinator)
              coordinator.foreach(_ ! GetShardHome(shardId))
            }
            val buf = shardBuffers.getOrEmpty(shardId)
            log.debug(
              "{}: Buffer message for shard [{}]. Total [{}] buffered messages.",
              typeName,
              shardId,
              buf.size + 1)
            shardBuffers.append(shardId, msg, snd)
            instrumentation.incrementShardBufferSize(scope, typeName)
        }

      case _ =>
        val shardId = extractShardId(msg)
        regionByShard.get(shardId) match {
          case Some(shardRegionRef) if shardRegionRef == self =>
            getShard(shardId) match {
              case Some(shard) =>
                if (shardBuffers.contains(shardId)) {
                  // Since now messages to a shard is buffered then those messages must be in right order
                  bufferMessage(shardId, msg, snd)
                  deliverBufferedMessages(shardId, shard)
                } else shard.tell(msg, snd)
              case None => bufferMessage(shardId, msg, snd)
            }
          case Some(shardRegionRef) =>
            if (verboseDebug)
              log.debug("{}: Forwarding message for shard [{}] to [{}]", typeName, shardId, shardRegionRef)
            shardRegionRef.tell(msg, snd)
          case None if shardId == null || shardId == "" =>
            log.warning("{}: Shard must not be empty, dropping message [{}]", typeName, msg.getClass.getName)
            context.system.deadLetters ! msg
          case None =>
            if (!shardBuffers.contains(shardId)) {
              log.debug("{}: Request shard [{}] home. Coordinator [{}]", typeName, shardId, coordinator)
              coordinator.foreach(_ ! GetShardHome(shardId))
            }
            bufferMessage(shardId, msg, snd)
        }
    }

  def getShard(id: ShardId): Option[ActorRef] = {
    if (startingShards.contains(id))
      None
    else {
      shards
        .get(id)
        .orElse(entityProps match {
          case Some(props) if !shardsByRef.values.exists(_ == id) =>
            log.debug(ShardingLogMarker.shardStarted(typeName, id), "{}: Starting shard [{}] in region", typeName, id)
            val name = URLEncoder.encode(id, "utf-8")
            val shard = context.watch(
              context.actorOf(
                Shard
                  .props(
                    typeName,
                    id,
                    props,
                    settings,
                    extractEntityId,
                    extractShardId,
                    handOffStopMessage,
                    rememberEntitiesProvider,
                    rememberEntityStarterManager)
                  .withDispatcher(context.props.dispatcher),
                name))
            shardsByRef = shardsByRef.updated(shard, id)
            shards = shards.updated(id, shard)
            startingShards += id
            if (settings.passivationStrategy != ClusterShardingSettings.NoPassivationStrategy) {
              shards.values.foreach(_ ! ShardsUpdated(shards.size))
            }
            None
          case Some(_) =>
            None
          case None =>
            throw new IllegalStateException("Shard must not be allocated to a proxy only ShardRegion")
        })
    }
  }

  def sendGracefulShutdownToCoordinatorIfInProgress(): Unit = {
    if (gracefulShutdownInProgress) {
      val actorSelections = coordinatorSelection
      log.debug("{}: Sending graceful shutdown to {}", typeName, actorSelections)
      actorSelections.foreach(_ ! GracefulShutdownReq(self))
    }
  }
}
