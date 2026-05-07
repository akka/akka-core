/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.control.NonFatal

import com.typesafe.config.Config

import akka.AkkaException
import akka.Done
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.CoordinatedShutdown
import akka.actor.DeadLetterSuppression
import akka.actor.Deploy
import akka.actor.FSM
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.Terminated
import akka.annotation.DoNotInherit
import akka.annotation.InternalStableApi
import akka.cluster._
import akka.cluster.ClusterEvent._
import akka.coordination.lease.LeaseUsageSettings
import akka.coordination.lease.scaladsl.Lease
import akka.coordination.lease.scaladsl.LeaseProvider
import akka.dispatch.Dispatchers
import akka.event.LogMarker
import akka.event.Logging
import akka.event.MarkerLoggingAdapter
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout

object ClusterSingletonManagerSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.singleton`.
   */
  def apply(system: ActorSystem): ClusterSingletonManagerSettings =
    apply(system.settings.config.getConfig("akka.cluster.singleton"))
    // note that this setting has some additional logic inside the ClusterSingletonManager
    // falling back to DowningProvider.downRemovalMargin if it is off/Zero
      .withRemovalMargin(Cluster(system).settings.DownRemovalMargin)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.singleton`.
   */
  def apply(config: Config): ClusterSingletonManagerSettings = {
    val lease = config.getString("use-lease") match {
      case s if s.isEmpty => None
      case leaseConfigPath =>
        Some(
          new LeaseUsageSettings(
            leaseConfigPath,
            config.getDuration("lease-retry-interval").toScala,
            leaseName = "" // intentionally not in config because would be high risk of not using unique names
          ))
    }
    new ClusterSingletonManagerSettings(
      singletonName = config.getString("singleton-name"),
      role = roleOption(config.getString("role")),
      removalMargin = Duration.Zero, // defaults to ClusterSettings.DownRemovalMargin
      handOverRetryInterval = config.getDuration("hand-over-retry-interval", MILLISECONDS).millis,
      lease)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.cluster.singleton`.
   */
  def create(system: ActorSystem): ClusterSingletonManagerSettings = apply(system)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.singleton`.
   */
  def create(config: Config): ClusterSingletonManagerSettings = apply(config)

  /**
   * INTERNAL API
   */
  private[akka] def roleOption(role: String): Option[String] =
    if (role == "") None else Option(role)

}

/**
 * @param singletonName The actor name of the child singleton actor.
 *
 * @param role Singleton among the nodes tagged with specified role.
 *   If the role is not specified it's a singleton among all nodes in
 *   the cluster.
 *
 * @param removalMargin Margin until the singleton instance that belonged to
 *   a downed/removed partition is created in surviving partition. The purpose of
 *   this margin is that in case of a network partition the singleton actors
 *   in the non-surviving partitions must be stopped before corresponding actors
 *   are started somewhere else. This is especially important for persistent
 *   actors.
 *
 * @param handOverRetryInterval When a node is becoming oldest it sends hand-over
 *   request to previous oldest, that might be leaving the cluster. This is
 *   retried with this interval until the previous oldest confirms that the hand
 *   over has started or the previous oldest member is removed from the cluster
 *   (+ `removalMargin`).
 *
 * @param leaseSettings LeaseSettings for acquiring before creating the singleton actor.
 *   Note that if you define a custom lease name and have several singletons each
 *   one must have a unique lease name. If the lease name is undefined it will be
 *   derived from ActorSystem name and singleton actor path, but that may result in
 *   too long lease names.
 */
final class ClusterSingletonManagerSettings(
    val singletonName: String,
    val role: Option[String],
    val removalMargin: FiniteDuration,
    val handOverRetryInterval: FiniteDuration,
    val leaseSettings: Option[LeaseUsageSettings])
    extends NoSerializationVerificationNeeded {

  // bin compat for akka 2.5.21
  def this(
      singletonName: String,
      role: Option[String],
      removalMargin: FiniteDuration,
      handOverRetryInterval: FiniteDuration) =
    this(singletonName, role, removalMargin, handOverRetryInterval, None)

  def withSingletonName(name: String): ClusterSingletonManagerSettings = copy(singletonName = name)

  def withRole(role: String): ClusterSingletonManagerSettings =
    copy(role = ClusterSingletonManagerSettings.roleOption(role))

  def withRole(role: Option[String]) = copy(role = role)

  def withRemovalMargin(removalMargin: FiniteDuration): ClusterSingletonManagerSettings =
    copy(removalMargin = removalMargin)

  def withHandOverRetryInterval(retryInterval: FiniteDuration): ClusterSingletonManagerSettings =
    copy(handOverRetryInterval = retryInterval)

  /**
   * Note that if you define a custom lease name and have several singletons each one must have a unique
   * lease name. If the lease name is undefined it will be derived from ActorSystem name and singleton
   * actor path, but that may result in too long lease names.
   */
  def withLeaseSettings(leaseSettings: LeaseUsageSettings): ClusterSingletonManagerSettings =
    copy(leaseSettings = Some(leaseSettings))

  private def copy(
      singletonName: String = singletonName,
      role: Option[String] = role,
      removalMargin: FiniteDuration = removalMargin,
      handOverRetryInterval: FiniteDuration = handOverRetryInterval,
      leaseSettings: Option[LeaseUsageSettings] = leaseSettings): ClusterSingletonManagerSettings =
    new ClusterSingletonManagerSettings(singletonName, role, removalMargin, handOverRetryInterval, leaseSettings)
}

/**
 * Marker trait for remote messages with special serializer.
 */
sealed trait ClusterSingletonMessage extends Serializable

object ClusterSingletonManager {

  /**
   * Scala API: Factory method for `ClusterSingletonManager` [[akka.actor.Props]].
   */
  def props(singletonProps: Props, terminationMessage: Any, settings: ClusterSingletonManagerSettings): Props =
    Props(new ClusterSingletonManager(singletonProps, terminationMessage, settings))
      .withDispatcher(Dispatchers.InternalDispatcherId)
      .withDeploy(Deploy.local)

  /**
   * INTERNAL API
   * public due to the `with FSM` type parameters
   */
  sealed trait State

  /**
   * INTERNAL API
   * public due to the `with FSM` type parameters
   */
  sealed trait Data

  /**
   * INTERNAL API
   */
  private[akka] object Internal {

    /**
     * Sent from new oldest to previous oldest to initiate the
     * hand-over process. `HandOverInProgress` and `HandOverDone`
     * are expected replies.
     */
    case object HandOverToMe extends ClusterSingletonMessage with DeadLetterSuppression

    /**
     * Confirmation by the previous oldest that the hand
     * over process, shut down of the singleton actor, has
     * started.
     */
    case object HandOverInProgress extends ClusterSingletonMessage

    /**
     * Confirmation by the previous oldest that the singleton
     * actor has been terminated and the hand-over process is
     * completed.
     */
    case object HandOverDone extends ClusterSingletonMessage

    /**
     * Sent from from previous oldest to new oldest to
     * initiate the normal hand-over process.
     * Especially useful when new node joins and becomes
     * oldest immediately, without knowing who was previous
     * oldest.
     */
    case object TakeOverFromMe extends ClusterSingletonMessage with DeadLetterSuppression

    final case class HandOverRetry(count: Int)
    final case class TakeOverRetry(count: Int)
    case object LeaseRetry
    case object Cleanup
    case object StartOldestChangedBuffer

    case object Start extends State
    case object AcquiringLease extends State
    case object Oldest extends State
    case object Younger extends State
    case object BecomingOldest extends State
    case object WasOldest extends State
    case object HandingOver extends State
    case object TakeOver extends State
    case object Stopping extends State
    case object End extends State

    case object Uninitialized extends Data
    final case class YoungerData(oldest: List[UniqueAddress]) extends Data
    final case class BecomingOldestData(previousOldest: List[UniqueAddress]) extends Data
    final case class OldestData(singleton: Option[ActorRef]) extends Data
    final case class WasOldestData(singleton: Option[ActorRef], newOldestOption: Option[UniqueAddress]) extends Data
    final case class HandingOverData(singleton: ActorRef, handOverTo: Option[ActorRef]) extends Data
    final case class StoppingData(singleton: ActorRef) extends Data
    case object EndData extends Data
    final case class DelayedMemberRemoved(member: Member)
    case object SelfExiting
    case class AcquiringLeaseData(leaseRequestInProgress: Boolean, singleton: Option[ActorRef]) extends Data

    val HandOverRetryTimer = "hand-over-retry"
    val TakeOverRetryTimer = "take-over-retry"
    val CleanupTimer = "cleanup"
    val LeaseRetryTimer = "lease-retry"

    object OldestChangedBuffer {

      /**
       * Request to deliver one more event.
       */
      case object GetNext

      /**
       * The first event, corresponding to CurrentClusterState.
       */
      final case class InitialOldestState(oldest: List[UniqueAddress], safeToBeOldest: Boolean)

      final case class OldestChanged(oldest: Option[UniqueAddress])
    }

    final case class AcquireLeaseResult(holdingLease: Boolean) extends DeadLetterSuppression
    final case class ReleaseLeaseResult(released: Boolean) extends DeadLetterSuppression
    final case class AcquireLeaseFailure(t: Throwable) extends DeadLetterSuppression
    final case class ReleaseLeaseFailure(t: Throwable) extends DeadLetterSuppression
    final case class LeaseLost(reason: Option[Throwable]) extends DeadLetterSuppression

    /**
     * Notifications of member events that track oldest member are tunneled
     * via this actor (child of ClusterSingletonManager) to be able to deliver
     * one change at a time. Avoiding simultaneous changes simplifies
     * the process in ClusterSingletonManager. ClusterSingletonManager requests
     * next event with `GetNext` when it is ready for it. Only one outstanding
     * `GetNext` request is allowed. Incoming events are buffered and delivered
     * upon `GetNext` request.
     */
    class OldestChangedBuffer(role: Option[String]) extends Actor {
      import OldestChangedBuffer._

      val cluster = Cluster(context.system)
      // sort by age, oldest first
      val ageOrdering = Member.ageOrdering
      var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

      var changes = Vector.empty[AnyRef]

      // subscribe to MemberEvent, re-subscribe when restart
      override def preStart(): Unit = {
        cluster.subscribe(self, classOf[MemberEvent])

        // It's a delicate difference between CoordinatedShutdown.PhaseClusterExiting and MemberExited.
        // MemberExited event is published immediately (leader may have performed that transition on other node),
        // and that will trigger run of CoordinatedShutdown, while PhaseClusterExiting will happen later.
        // Using PhaseClusterExiting in the singleton because the graceful shutdown of sharding region
        // should preferably complete before stopping the singleton sharding coordinator on same node.
        val coordShutdown = CoordinatedShutdown(context.system)
        coordShutdown.addTask(CoordinatedShutdown.PhaseClusterExiting, "singleton-exiting-1") { () =>
          if (cluster.isTerminated || cluster.selfMember.status == MemberStatus.Down) {
            Future.successful(Done)
          } else {
            implicit val timeout = Timeout(coordShutdown.timeout(CoordinatedShutdown.PhaseClusterExiting))
            self.ask(SelfExiting).mapTo[Done]
          }
        }
      }
      override def postStop(): Unit = cluster.unsubscribe(self)

      private val selfDc = ClusterSettings.DcRolePrefix + cluster.settings.SelfDataCenter

      def matchingRole(member: Member): Boolean =
        member.hasRole(selfDc) && role.forall(member.hasRole)

      def trackChange(block: () => Unit): Unit = {
        val before = membersByAge.headOption
        block()
        val after = membersByAge.headOption
        if (before != after)
          changes :+= OldestChanged(after.map(_.uniqueAddress))
      }

      def handleInitial(state: CurrentClusterState): Unit = {
        // all members except Joining and WeaklyUp
        membersByAge = immutable.SortedSet
          .empty(ageOrdering)
          .union(state.members.filter(m => m.upNumber != Int.MaxValue && matchingRole(m)))

        // If there is some removal in progress of an older node it's not safe to immediately become oldest,
        // removal of younger nodes doesn't matter. Note that it can also be started via restart after
        // ClusterSingletonManagerIsStuck.
        val selfUpNumber = state.members
          .collectFirst { case m if m.uniqueAddress == cluster.selfUniqueAddress => m.upNumber }
          .getOrElse(Int.MaxValue)
        val oldest = membersByAge.takeWhile(_.upNumber <= selfUpNumber)
        val safeToBeOldest = !oldest.exists { m =>
          m.status == MemberStatus.Down || m.status == MemberStatus.Exiting || m.status == MemberStatus.Leaving
        }

        val initial = InitialOldestState(oldest.toList.map(_.uniqueAddress), safeToBeOldest)
        changes :+= initial
      }

      def add(m: Member): Unit = {
        if (matchingRole(m))
          trackChange { () =>
            // replace, it's possible that the upNumber is changed
            membersByAge = membersByAge.filterNot(_.uniqueAddress == m.uniqueAddress)
            membersByAge += m
          }
      }

      def remove(m: Member): Unit = {
        if (matchingRole(m))
          trackChange { () =>
            membersByAge = membersByAge.filterNot(_.uniqueAddress == m.uniqueAddress)
          }
      }

      def sendFirstChange(): Unit = {
        // don't send cluster change events if this node is shutting its self down, just wait for SelfExiting
        if (!cluster.isTerminated) {
          val event = changes.head
          changes = changes.tail
          context.parent ! event
        }
      }

      def receive = {
        case state: CurrentClusterState => handleInitial(state)
        case MemberUp(m)                => add(m)
        case MemberRemoved(m, _)        => remove(m)
        case MemberExited(m) if m.uniqueAddress != cluster.selfUniqueAddress =>
          remove(m)
        case SelfExiting =>
          remove(cluster.readView.self)
          sender() ! Done // reply to ask
        case GetNext if changes.isEmpty =>
          context.become(deliverNext, discardOld = false)
        case GetNext =>
          sendFirstChange()
      }

      // the buffer was empty when GetNext was received, deliver next event immediately
      def deliverNext: Actor.Receive = {
        case state: CurrentClusterState =>
          handleInitial(state)
          sendFirstChange()
          context.unbecome()
        case MemberUp(m) =>
          add(m)
          deliverChanges()
        case MemberRemoved(m, _) =>
          remove(m)
          deliverChanges()
        case MemberExited(m) if m.uniqueAddress != cluster.selfUniqueAddress =>
          remove(m)
          deliverChanges()
        case SelfExiting =>
          remove(cluster.readView.self)
          deliverChanges()
          sender() ! Done // reply to ask
      }

      def deliverChanges(): Unit = {
        if (changes.nonEmpty) {
          sendFirstChange()
          context.unbecome()
        }
      }

      override def unhandled(msg: Any): Unit = {
        msg match {
          case _: MemberEvent => // ok, silence
          case _              => super.unhandled(msg)
        }
      }
    }
  }
}

/**
 * Thrown when a consistent state can't be determined within the
 * defined retry limits. Eventually it will reach a stable state and
 * can continue, and that is simplified by starting over with a clean
 * state. Parent supervisor should typically restart the actor, i.e.
 * default decision.
 */
class ClusterSingletonManagerIsStuck(message: String) extends AkkaException(message, null)

/**
 * Manages singleton actor instance among all cluster nodes or a group
 * of nodes tagged with a specific role. At most one singleton instance
 * is running at any point in time.
 *
 * The ClusterSingletonManager is supposed to be started on all nodes,
 * or all nodes with specified role, in the cluster with `actorOf`.
 * The actual singleton is started on the oldest node by creating a child
 * actor from the supplied `singletonProps`.
 *
 * The singleton actor is always running on the oldest member with specified role.
 * The oldest member is determined by [[akka.cluster.Member#isOlderThan]].
 * This can change when removing members. A graceful hand over can normally
 * be performed when current oldest node is leaving the cluster. Be aware that
 * there is a short time period when there is no active singleton during the
 * hand-over process.
 *
 * The cluster failure detector will notice when oldest node
 * becomes unreachable due to things like JVM crash, hard shut down,
 * or network failure. When the crashed node has been removed (via down) from the
 * cluster then a new oldest node will take over and a new singleton actor is
 * created. For these failure scenarios there will not be a graceful hand-over,
 * but more than one active singletons is prevented by all reasonable means. Some
 * corner cases are eventually resolved by configurable timeouts.
 *
 * You access the singleton actor with [[ClusterSingletonProxy]].
 * Alternatively the singleton actor may broadcast its existence when it is started.
 *
 * Use factory method [[ClusterSingletonManager#props]] to create the
 * [[akka.actor.Props]] for the actor.
 *
 * Not intended for subclassing by user code.
 *
 *
 * @param singletonProps [[akka.actor.Props]] of the singleton actor instance.
 *
 * @param terminationMessage When handing over to a new oldest node
 *   this `terminationMessage` is sent to the singleton actor to tell
 *   it to finish its work, close resources, and stop.
 *   The hand-over to the new oldest node is completed when the
 *   singleton actor is terminated.
 *   Note that [[akka.actor.PoisonPill]] is a perfectly fine
 *   `terminationMessage` if you only need to stop the actor.
 *
 * @param settings see [[ClusterSingletonManagerSettings]]
 */
@DoNotInherit
class ClusterSingletonManager(singletonProps: Props, terminationMessage: Any, settings: ClusterSingletonManagerSettings)
    extends Actor
    with FSM[ClusterSingletonManager.State, ClusterSingletonManager.Data] {

  import ClusterSingletonManager.Internal._
  import ClusterSingletonManager.Internal.OldestChangedBuffer._
  import settings._

  val cluster = Cluster(context.system)
  val selfUniqueAddressOption = Some(cluster.selfUniqueAddress)
  import cluster.settings.LogInfo

  require(
    role.forall(cluster.selfRoles.contains),
    s"This cluster member [${cluster.selfAddress}] doesn't have the role [$role]")

  override val log: MarkerLoggingAdapter = Logging.withMarker(context.system, this)

  val lease: Option[Lease] = settings.leaseSettings.map { settings =>
    val leaseName =
      if (settings.leaseName.isEmpty) s"${context.system.name}-singleton-${self.path}"
      else settings.leaseName
    LeaseProvider(context.system).getLease(leaseName, settings.leaseImplementation, cluster.selfAddress.hostPort)
  }
  val leaseRetryInterval: FiniteDuration = settings.leaseSettings match {
    case Some(s) => s.leaseRetryInterval
    case None    => 5.seconds // won't be used
  }

  val removalMargin =
    if (settings.removalMargin <= Duration.Zero) cluster.downingProvider.downRemovalMargin
    else settings.removalMargin

  val (maxHandOverRetries, maxTakeOverRetries) = {
    val n = (removalMargin.toMillis / handOverRetryInterval.toMillis).toInt
    val minRetries = context.system.settings.config.getInt("akka.cluster.singleton.min-number-of-hand-over-retries")
    require(minRetries >= 1, "min-number-of-hand-over-retries must be >= 1")
    val handOverRetries = math.max(minRetries, n + 3)
    val takeOverRetries = math.max(1, handOverRetries - 3)

    (handOverRetries, takeOverRetries)
  }

  // started when self member is Up
  var oldestChangedBuffer: ActorRef = _
  // Previous GetNext request delivered event and new GetNext is to be sent
  var oldestChangedReceived = true
  var preparingForFullShutdown = false

  var selfExited = false

  // keep track of previously removed members
  var removed = Map.empty[UniqueAddress, Deadline]

  def addRemoved(node: UniqueAddress): Unit =
    removed += node -> (Deadline.now + 15.minutes)

  def cleanupOverdueNotMemberAnyMore(): Unit = {
    removed = removed.filter { case (_, deadline) => deadline.hasTimeLeft() }
  }

  // for CoordinatedShutdown
  val coordShutdown = CoordinatedShutdown(context.system)
  val memberExitingProgress = Promise[Done]()
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterExiting, "wait-singleton-exiting") { () =>
    if (cluster.isTerminated || cluster.selfMember.status == MemberStatus.Down)
      Future.successful(Done)
    else
      memberExitingProgress.future
  }
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterExiting, "singleton-exiting-2") { () =>
    if (cluster.isTerminated || cluster.selfMember.status == MemberStatus.Down) {
      Future.successful(Done)
    } else {
      implicit val timeout = Timeout(coordShutdown.timeout(CoordinatedShutdown.PhaseClusterExiting))
      self.ask(SelfExiting).mapTo[Done]
    }
  }

  def logInfo(message: String): Unit =
    if (LogInfo) log.info(message)

  def logInfo(marker: LogMarker, message: String): Unit =
    if (LogInfo) log.info(marker, message)

  def logInfo(template: String, arg1: Any): Unit =
    if (LogInfo) log.info(template, arg1)

  def logInfo(marker: LogMarker, template: String, arg1: Any): Unit =
    if (LogInfo) log.info(marker, template, arg1)

  def logInfo(template: String, arg1: Any, arg2: Any): Unit =
    if (LogInfo) log.info(template, arg1, arg2)

  def logInfo(marker: LogMarker, template: String, arg1: Any, arg2: Any): Unit =
    if (LogInfo) log.info(marker, template, arg1, arg2)

  def logInfo(template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
    if (LogInfo) log.info(template, arg1, arg2, arg3)

  override def preStart(): Unit = {
    super.preStart()
    require(!cluster.isTerminated, "Cluster node must not be terminated")

    // subscribe to cluster changes, re-subscribe when restart
    cluster.subscribe(
      self,
      ClusterEvent.InitialStateAsEvents,
      classOf[MemberRemoved],
      classOf[MemberDowned],
      classOf[MemberPreparingForShutdown],
      classOf[MemberReadyForShutdown])

    startTimerWithFixedDelay(CleanupTimer, Cleanup, 1.minute)

    // defer subscription to avoid some jitter when
    // starting/joining several nodes at the same time
    cluster.registerOnMemberUp(self ! StartOldestChangedBuffer)
  }

  override def postStop(): Unit = {
    cancelTimer(CleanupTimer)
    cluster.unsubscribe(self)
    memberExitingProgress.trySuccess(Done)
    super.postStop()
  }

  def peer(at: Address): ActorSelection = context.actorSelection(self.path.toStringWithAddress(at))

  def getNextOldestChanged(): Unit =
    if (oldestChangedReceived) {
      oldestChangedReceived = false
      oldestChangedBuffer ! GetNext
    }

  startWith(Start, Uninitialized)

  when(Start) {
    case Event(StartOldestChangedBuffer, _) =>
      oldestChangedBuffer =
        context.actorOf(Props(new OldestChangedBuffer(role)).withDispatcher(context.props.dispatcher))
      getNextOldestChanged()
      stay()

    case Event(InitialOldestState(oldest, safeToBeOldest), _) =>
      oldestChangedReceived = true

      if (oldest.headOption == selfUniqueAddressOption && safeToBeOldest)
        // oldest immediately
        tryGotoOldest()
      else if (oldest.headOption == selfUniqueAddressOption)
        goto(BecomingOldest).using(BecomingOldestData(oldest.filterNot(_ == cluster.selfUniqueAddress)))
      else
        goto(Younger).using(YoungerData(oldest.filterNot(_ == cluster.selfUniqueAddress)))

    case Event(HandOverToMe, _) =>
      // nothing to hand over in start
      stay()

    case Event(event: MemberEvent, _) =>
      handleMemberEvent(event)
  }

  def handleMemberEvent(event: MemberEvent): State = {
    event match {
      case _: MemberRemoved if event.member.uniqueAddress == cluster.selfUniqueAddress =>
        logInfo("Self removed, stopping ClusterSingletonManager")
        stop()
      case _: MemberDowned if event.member.uniqueAddress == cluster.selfUniqueAddress =>
        logInfo("Self downed, stopping ClusterSingletonManager")
        stop()
      case _: MemberReadyForShutdown | _: MemberPreparingForShutdown =>
        if (!preparingForFullShutdown) {
          logInfo("Preparing for shut down, disabling expensive actions")
          preparingForFullShutdown = true
        }
        stay()
      case _ =>
        stay()
    }
  }

  when(Younger) {
    case Event(OldestChanged(oldestOption), YoungerData(previousOldest)) =>
      oldestChangedReceived = true
      if (oldestOption == selfUniqueAddressOption) {
        logInfo("Younger observed OldestChanged: [{} -> myself]", previousOldest.headOption.map(_.address))
        if (previousOldest.forall(removed.contains))
          tryGotoOldest()
        else {
          if (!preparingForFullShutdown) {
            peer(previousOldest.head.address) ! HandOverToMe
          }
          goto(BecomingOldest).using(BecomingOldestData(previousOldest))
        }
      } else {
        logInfo(
          "Younger observed OldestChanged: [{} -> {}]",
          previousOldest.headOption.map(_.address),
          oldestOption.map(_.address))
        getNextOldestChanged()
        val newPreviousOldest = oldestOption match {
          case Some(oldest) if !previousOldest.contains(oldest) => oldest :: previousOldest
          case _                                                => previousOldest
        }
        stay().using(YoungerData(newPreviousOldest))
      }

    case Event(event @ MemberRemoved(m, _), _) if event.member.uniqueAddress != cluster.selfUniqueAddress =>
      scheduleDelayedMemberRemoved(m)
      stay()

    case Event(event: MemberEvent, _) =>
      handleMemberEvent(event)

    case Event(DelayedMemberRemoved(m), YoungerData(previousOldest)) =>
      if (!selfExited)
        logInfo("Member removed [{}]", m.address)
      addRemoved(m.uniqueAddress)
      // transition when OldestChanged
      stay().using(YoungerData(previousOldest.filterNot(_ == m.uniqueAddress)))

    case Event(HandOverToMe, _) =>
      val selfStatus = cluster.selfMember.status
      if (selfStatus == MemberStatus.Leaving || selfStatus == MemberStatus.Exiting)
        logInfo("Ignoring HandOverToMe in Younger from [{}] because self is [{}].", sender().path.address, selfStatus)
      else {
        // this node was probably quickly restarted with same hostname:port,
        // confirm that the old singleton instance has been stopped
        if (!preparingForFullShutdown) {
          sender() ! HandOverDone
        }
      }

      stay()
  }

  when(BecomingOldest) {

    case Event(HandOverInProgress, _) =>
      // confirmation that the hand-over process has started
      logInfo("Hand-over in progress at [{}]", sender().path.address)
      cancelTimer(HandOverRetryTimer)
      stay()

    case Event(HandOverDone, BecomingOldestData(previousOldest)) =>
      previousOldest.headOption match {
        case Some(oldest) =>
          if (sender().path.address == oldest.address)
            tryGotoOldest()
          else {
            logInfo(
              "Ignoring HandOverDone in BecomingOldest from [{}]. Expected previous oldest [{}]",
              sender().path.address,
              oldest.address)
            stay()
          }
        case None =>
          logInfo("Ignoring HandOverDone in BecomingOldest from [{}].", sender().path.address)
          stay()
      }

    case Event(event @ MemberRemoved(m, _), _) if event.member.uniqueAddress != cluster.selfUniqueAddress =>
      scheduleDelayedMemberRemoved(m)
      stay()

    case Event(event: MemberEvent, _) if event.member.uniqueAddress == cluster.selfUniqueAddress =>
      handleMemberEvent(event)

    case Event(DelayedMemberRemoved(m), BecomingOldestData(previousOldest)) =>
      if (!selfExited)
        logInfo("Member removed [{}], previous oldest [{}]", m.address, previousOldest.map(_.address).mkString(", "))
      addRemoved(m.uniqueAddress)
      if (cluster.isTerminated) {
        // don't act on DelayedMemberRemoved (starting singleton) if this node is shutting its self down,
        // just wait for self MemberRemoved
        stay()
      } else if (previousOldest.contains(m.uniqueAddress) && previousOldest.forall(removed.contains))
        tryGotoOldest()
      else
        stay().using(BecomingOldestData(previousOldest.filterNot(_ == m.uniqueAddress)))

    case Event(TakeOverFromMe, BecomingOldestData(previousOldest)) =>
      val senderAddress = sender().path.address
      // it would have been better to include the UniqueAddress in the TakeOverFromMe message,
      // but can't change due to backwards compatibility
      cluster.state.members.collectFirst { case m if m.address == senderAddress => m.uniqueAddress } match {
        case None =>
          // from unknown node, ignore
          logInfo("Ignoring TakeOver request from unknown node in BecomingOldest from [{}].", senderAddress)
          stay()
        case Some(senderUniqueAddress) =>
          previousOldest.headOption match {
            case Some(oldest) =>
              if (oldest == senderUniqueAddress) {
                if (!preparingForFullShutdown) {
                  sender() ! HandOverToMe
                }
              } else
                logInfo(
                  "Ignoring TakeOver request in BecomingOldest from [{}]. Expected previous oldest [{}]",
                  sender().path.address,
                  oldest.address)
              stay()
            case None =>
              if (!preparingForFullShutdown) {
                sender() ! HandOverToMe
              }
              stay().using(BecomingOldestData(senderUniqueAddress :: previousOldest))
          }
      }

    case Event(HandOverRetry(count), BecomingOldestData(previousOldest)) =>
      if (count <= maxHandOverRetries) {
        if (!preparingForFullShutdown) {
          logInfo("Retry [{}], sending HandOverToMe to [{}]", count, previousOldest.headOption.map(_.address))
          previousOldest.headOption.foreach(node => peer(node.address) ! HandOverToMe)
          startSingleTimer(HandOverRetryTimer, HandOverRetry(count + 1), handOverRetryInterval)
        }
        stay()
      } else if (previousOldest.forall(removed.contains)) {
        // can't send HandOverToMe, previousOldest unknown for new node (or restart)
        // previous oldest might be down or removed, so no TakeOverFromMe message is received
        logInfo("Timeout in BecomingOldest. Previous oldest unknown, removed and no TakeOver request.")
        tryGotoOldest()
      } else if (cluster.isTerminated)
        stop()
      else
        throw new ClusterSingletonManagerIsStuck(
          s"Becoming singleton oldest was stuck because previous oldest [${previousOldest.headOption}] is unresponsive")
  }

  def scheduleDelayedMemberRemoved(m: Member): Unit = {
    if (removalMargin > Duration.Zero) {
      log.debug("Schedule DelayedMemberRemoved for [{}]", m.address)
      context.system.scheduler.scheduleOnce(removalMargin, self, DelayedMemberRemoved(m))(context.dispatcher)
    } else
      self ! DelayedMemberRemoved(m)
  }

  def tryAcquireLease() = {
    import context.dispatcher

    if (!preparingForFullShutdown) {
      pipe(lease.get.acquire(reason => self ! LeaseLost(reason)).map[Any](AcquireLeaseResult(_)).recover {
        case NonFatal(t) => AcquireLeaseFailure(t)
      }).to(self)
    }
    goto(AcquiringLease).using(AcquiringLeaseData(leaseRequestInProgress = true, None))
  }

  // Try and go to oldest, taking the lease if needed
  def tryGotoOldest(): State = {
    // check if lease
    lease match {
      case None =>
        gotoOldest()
      case Some(_) =>
        logInfo("Trying to acquire lease before starting singleton")
        tryAcquireLease()
    }
  }

  when(AcquiringLease) {
    case Event(AcquireLeaseResult(result), _) =>
      logInfo("Acquire lease result {}", result)
      if (result) {
        gotoOldest()
      } else {
        startSingleTimer(LeaseRetryTimer, LeaseRetry, leaseRetryInterval)
        stay().using(AcquiringLeaseData(leaseRequestInProgress = false, None))
      }
    case Event(Terminated(ref), AcquiringLeaseData(_, Some(singleton))) if ref == singleton =>
      logInfo(
        ClusterLogMarker.singletonTerminated,
        "Singleton actor terminated. Trying to acquire lease again before re-creating.")
      // tryAcquireLease sets the state to None for singleton actor
      tryAcquireLease()
    case Event(AcquireLeaseFailure(t), _) =>
      log.error(t, "failed to get lease (will be retried)")
      startSingleTimer(LeaseRetryTimer, LeaseRetry, leaseRetryInterval)
      stay().using(AcquiringLeaseData(leaseRequestInProgress = false, None))
    case Event(LeaseRetry, _) =>
      // If lease was lost (so previous state was oldest) then we don't try and get the lease
      // until the old singleton instance has been terminated so we know there isn't an
      // instance in this case
      tryAcquireLease()
    case Event(OldestChanged(oldestOption), AcquiringLeaseData(_, singleton)) =>
      handleOldestChanged(singleton, oldestOption)
    case Event(HandOverToMe, AcquiringLeaseData(_, singleton)) =>
      gotoHandingOver(singleton, Some(sender()))
    case Event(TakeOverFromMe, _) =>
      // already oldest, so confirm and continue like that
      sender() ! HandOverToMe
      stay()
    case Event(SelfExiting, _) =>
      selfMemberExited()
      // complete memberExitingProgress when handOverDone
      sender() ! Done // reply to ask
      stay()

    case Event(event: MemberEvent, _) =>
      handleMemberEvent(event)

  }

  @InternalStableApi
  def gotoOldest(): State = {
    if (preparingForFullShutdown) {
      logInfo(
        ClusterLogMarker.singletonStarted,
        "Singleton manager NOT starting singleton actor [{}] as cluster is preparing to shutdown",
        self.path / singletonName)
      goto(Oldest).using(OldestData(None))
    } else {
      logInfo(
        ClusterLogMarker.singletonStarted,
        "Singleton manager starting singleton actor [{}]",
        self.path / singletonName)
      val singleton = context.watch(context.actorOf(singletonProps, singletonName))
      goto(Oldest).using(OldestData(Some(singleton)))
    }

  }

  def handleOldestChanged(singleton: Option[ActorRef], oldestOption: Option[UniqueAddress]) = {
    oldestChangedReceived = true
    logInfo("{} observed OldestChanged: [{} -> {}]", stateName, cluster.selfAddress, oldestOption.map(_.address))
    oldestOption match {
      case Some(a) if a == cluster.selfUniqueAddress =>
        // already oldest
        stay()
      case Some(a) if !selfExited && removed.contains(a) =>
        // The member removal was not completed and the old removed node is considered
        // oldest again. Safest is to terminate the singleton instance and goto Younger.
        // This node will become oldest again when the other is removed again.
        gotoHandingOver(singleton, None)
      case Some(a) =>
        // send TakeOver request in case the new oldest doesn't know previous oldest
        if (!preparingForFullShutdown) {
          peer(a.address) ! TakeOverFromMe
          startSingleTimer(TakeOverRetryTimer, TakeOverRetry(1), handOverRetryInterval)
        }
        goto(WasOldest).using(WasOldestData(singleton, newOldestOption = Some(a)))
      case None =>
        // new oldest will initiate the hand-over
        if (!preparingForFullShutdown) {
          startSingleTimer(TakeOverRetryTimer, TakeOverRetry(1), handOverRetryInterval)
        }
        goto(WasOldest).using(WasOldestData(singleton, newOldestOption = None))
    }
  }

  when(Oldest) {
    case Event(OldestChanged(oldestOption), OldestData(singleton)) =>
      handleOldestChanged(singleton, oldestOption)
    case Event(HandOverToMe, OldestData(singleton)) =>
      gotoHandingOver(singleton, Some(sender()))
    case Event(TakeOverFromMe, _) =>
      // already oldest, so confirm and continue like that
      sender() ! HandOverToMe
      stay()

    case Event(Terminated(ref), d @ OldestData(Some(singleton))) if ref == singleton =>
      logInfo(ClusterLogMarker.singletonTerminated, "Singleton actor [{}] was terminated", singleton.path)
      stay().using(d.copy(singleton = None))

    case Event(SelfExiting, _) =>
      selfMemberExited()
      // complete memberExitingProgress when handOverDone
      sender() ! Done // reply to ask
      stay()

    case Event(MemberDowned(m), OldestData(singleton)) if m.uniqueAddress == cluster.selfUniqueAddress =>
      singleton match {
        case Some(s) =>
          logInfo("Self downed, stopping")
          gotoStopping(s)
        case None =>
          logInfo("Self downed, stopping ClusterSingletonManager")
          stop()
      }

    // Downed in this case is handled differently so keep this below it
    case Event(event: MemberEvent, _) =>
      handleMemberEvent(event)

    case Event(LeaseLost(reason), OldestData(singleton)) =>
      log.warning("Lease has been lost. Reason: {}. Terminating singleton and trying to re-acquire lease", reason)
      singleton match {
        case Some(s) =>
          s ! terminationMessage
          goto(AcquiringLease).using(AcquiringLeaseData(leaseRequestInProgress = false, singleton))
        case None =>
          tryAcquireLease()
      }

    case Event(HandOverDone, _) =>
      // nothing to do
      stay()
  }

  when(WasOldest) {
    case Event(TakeOverRetry(count), WasOldestData(singleton, newOldestOption)) =>
      if ((cluster.isTerminated || selfExited) && (newOldestOption.isEmpty || count > maxTakeOverRetries)) {
        singleton match {
          case Some(s) => gotoStopping(s)
          case None    => stop()
        }
      } else if (count <= maxTakeOverRetries) {
        if (maxTakeOverRetries - count <= 3)
          logInfo("Retry [{}], sending TakeOverFromMe to [{}]", count, newOldestOption.map(_.address))
        else
          log.debug("Retry [{}], sending TakeOverFromMe to [{}]", count, newOldestOption.map(_.address))

        if (!preparingForFullShutdown) {
          newOldestOption.foreach(node => peer(node.address) ! TakeOverFromMe)
          startSingleTimer(TakeOverRetryTimer, TakeOverRetry(count + 1), handOverRetryInterval)
        }
        stay()
      } else
        throw new ClusterSingletonManagerIsStuck(s"Expected hand-over to [$newOldestOption] never occurred")

    case Event(HandOverToMe, WasOldestData(singleton, _)) =>
      gotoHandingOver(singleton, Some(sender()))

    case Event(MemberRemoved(m, _), _) if m.uniqueAddress == cluster.selfUniqueAddress && !selfExited =>
      logInfo("Self removed, stopping ClusterSingletonManager")
      stop()

    case Event(MemberRemoved(m, _), WasOldestData(singleton, Some(newOldest)))
        if !selfExited && m.uniqueAddress == newOldest =>
      addRemoved(m.uniqueAddress)
      gotoHandingOver(singleton, None)

    case Event(Terminated(ref), d @ WasOldestData(singleton, _)) if singleton.contains(ref) =>
      logInfo(ClusterLogMarker.singletonTerminated, "Singleton actor [{}] was terminated", ref.path)
      stay().using(d.copy(singleton = None))

    case Event(SelfExiting, _) =>
      selfMemberExited()
      // complete memberExitingProgress when handOverDone
      sender() ! Done // reply to ask
      stay()

    case Event(MemberDowned(m), WasOldestData(singleton, _)) if m.uniqueAddress == cluster.selfUniqueAddress =>
      singleton match {
        case None =>
          logInfo("Self downed, stopping ClusterSingletonManager")
          stop()
        case Some(s) =>
          logInfo("Self downed, stopping")
          gotoStopping(s)
      }

    case Event(event: MemberEvent, _) =>
      handleMemberEvent(event)

  }

  def gotoHandingOver(singleton: Option[ActorRef], handOverTo: Option[ActorRef]): State = {
    singleton match {
      case None =>
        handOverDone(handOverTo)
      case Some(s) =>
        handOverTo.foreach { _ ! HandOverInProgress }
        logInfo("Singleton manager stopping singleton actor [{}]", s.path)
        s ! terminationMessage
        goto(HandingOver).using(HandingOverData(s, handOverTo))
    }
  }

  when(HandingOver) {
    case Event(Terminated(ref), HandingOverData(singleton, handOverTo)) if ref == singleton =>
      handOverDone(handOverTo)

    case Event(HandOverToMe, HandingOverData(_, handOverTo)) if handOverTo.contains(sender()) =>
      // retry
      if (!preparingForFullShutdown) {
        sender() ! HandOverInProgress
      }
      stay()

    case Event(SelfExiting, _) =>
      selfMemberExited()
      // complete memberExitingProgress when handOverDone
      sender() ! Done // reply to ask
      stay()

    case Event(MemberReadyForShutdown(m), _) if m.uniqueAddress == cluster.selfUniqueAddress =>
      logInfo("Ready for shutdown when handing over. Giving up on handover.")
      stop()

    case Event(MemberPreparingForShutdown(m), _) if m.uniqueAddress == cluster.selfUniqueAddress =>
      logInfo("Preparing for shutdown when handing over. Giving up on handover.")
      stop()

  }

  def handOverDone(handOverTo: Option[ActorRef]): State = {
    val newOldest = handOverTo.map(_.path.address)
    logInfo(
      ClusterLogMarker.singletonTerminated,
      "Singleton terminated, hand-over done [{} -> {}]",
      cluster.selfAddress,
      newOldest)
    handOverTo.foreach { _ ! HandOverDone }
    memberExitingProgress.trySuccess(Done)
    if (removed.contains(cluster.selfUniqueAddress)) {
      logInfo("Self removed, stopping ClusterSingletonManager")
      stop()
    } else if (handOverTo.isEmpty)
      goto(Younger).using(YoungerData(Nil))
    else
      goto(End).using(EndData)
  }

  def gotoStopping(singleton: ActorRef): State = {
    logInfo("Singleton manager stopping singleton actor [{}]", singleton.path)
    singleton ! terminationMessage
    goto(Stopping).using(StoppingData(singleton))
  }

  when(Stopping) {
    case Event(Terminated(ref), StoppingData(singleton)) if ref == singleton =>
      logInfo(ClusterLogMarker.singletonTerminated, "Singleton actor [{}] was terminated", singleton.path)
      stop()

    case Event(MemberReadyForShutdown(m), _) if m.uniqueAddress == cluster.selfUniqueAddress =>
      logInfo("Ready for shutdown when stopping. Not waiting for user actor to shutdown")
      stop()

    case Event(MemberPreparingForShutdown(m), _) if m.uniqueAddress == cluster.selfUniqueAddress =>
      logInfo("Preparing for shutdown when stopping. Not waiting for user actor to shutdown")
      stop()

  }

  when(End) {
    case Event(MemberRemoved(m, _), _) if m.uniqueAddress == cluster.selfUniqueAddress =>
      logInfo("Self removed, stopping ClusterSingletonManager")
      stop()
    case Event(_: OldestChanged | HandOverToMe, _) =>
      // not interested anymore - waiting for removal
      stay()
  }

  def selfMemberExited(): Unit = {
    selfExited = true
    logInfo(
      "Exited [{}].{}",
      cluster.selfAddress,
      if (preparingForFullShutdown) " From preparing from shutdown" else "")
    // handover won't be done so just complete right away
    if (preparingForFullShutdown) {
      memberExitingProgress.trySuccess(Done)
    }
  }

  whenUnhandled {
    case Event(SelfExiting, _) =>
      selfMemberExited()
      memberExitingProgress.trySuccess(Done)
      sender() ! Done // reply to ask
      stay()
    case Event(MemberRemoved(m, _), _) if m.uniqueAddress == cluster.selfUniqueAddress && !selfExited =>
      logInfo("Self removed, stopping ClusterSingletonManager")
      stop()
    case Event(MemberRemoved(m, _), _) =>
      if (!selfExited) logInfo("Member removed [{}]", m.address)
      addRemoved(m.uniqueAddress)
      stay()
    case Event(DelayedMemberRemoved(m), _) =>
      if (!selfExited) logInfo("Member removed [{}]", m.address)
      addRemoved(m.uniqueAddress)
      stay()
    case Event(TakeOverFromMe, _) =>
      log.debug("Ignoring TakeOver request in [{}] from [{}].", stateName, sender().path.address)
      stay()
    case Event(Cleanup, _) =>
      cleanupOverdueNotMemberAnyMore()
      stay()
    case Event(MemberDowned(m), _) =>
      if (m.uniqueAddress == cluster.selfUniqueAddress)
        logInfo("Self downed, waiting for removal")
      stay()
    case Event(ReleaseLeaseFailure(t), _) =>
      log.error(
        t,
        "Failed to release lease. Singleton may not be able to run on another node until lease timeout occurs")
      stay()
    case Event(ReleaseLeaseResult(released), _) =>
      if (released) {
        logInfo("Lease released")
      } else {
        // TODO we could retry
        log.error(
          "Failed to release lease. Singleton may not be able to run on another node until lease timeout occurs")
      }
      stay()
    case Event(_: MemberEvent, _) =>
      stay() // silence
  }

  onTransition {
    case from -> to => logInfo("ClusterSingletonManager state change [{} -> {}]", from, to)
  }

  onTransition {
    case _ -> BecomingOldest => startSingleTimer(HandOverRetryTimer, HandOverRetry(1), handOverRetryInterval)
  }

  onTransition {
    case BecomingOldest -> _ => cancelTimer(HandOverRetryTimer)
    case WasOldest -> _      => cancelTimer(TakeOverRetryTimer)
  }

  onTransition {
    case (AcquiringLease, to) if to != Oldest =>
      stateData match {
        case AcquiringLeaseData(true, _) =>
          logInfo("Releasing lease as leaving AcquiringLease going to [{}]", to)
          import context.dispatcher
          lease.foreach(l =>
            pipe(l.release().map[Any](ReleaseLeaseResult(_)).recover {
              case t => ReleaseLeaseFailure(t)
            }).to(self))
        case _ =>
      }
  }

  onTransition {
    case Oldest -> _ =>
      lease.foreach { l =>
        logInfo("Releasing lease as leaving Oldest")
        import context.dispatcher
        pipe(l.release().map(ReleaseLeaseResult(_))).to(self)
      }
  }

  onTransition {
    case _ -> (Younger | Oldest) => getNextOldestChanged()
  }

  onTransition {
    case _ -> (Younger | End) if removed.contains(cluster.selfUniqueAddress) =>
      logInfo("Self removed, stopping ClusterSingletonManager")
      // note that FSM.stop() can't be used in onTransition
      context.stop(self)
  }

}
