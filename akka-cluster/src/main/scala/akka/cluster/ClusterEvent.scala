/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable
import scala.collection.immutable.{ SortedSet, VectorBuilder }
import scala.runtime.AbstractFunction5
import scala.jdk.CollectionConverters._

import language.postfixOps

import akka.actor.{ Actor, ActorRef, Address }
import akka.actor.DeadLetterSuppression
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.MemberStatus._
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.event.EventStream

/**
 * Domain events published to the event bus.
 * Subscribe with:
 * {{{
 *   Cluster(system).subscribe(actorRef, classOf[ClusterDomainEvent])
 * }}}
 */
@nowarn("msg=Use Akka Distributed Cluster")
object ClusterEvent {

  sealed abstract class SubscriptionInitialStateMode

  /**
   * When using this subscription mode a snapshot of
   * [[akka.cluster.ClusterEvent.CurrentClusterState]] will be sent to the
   * subscriber as the first message.
   */
  case object InitialStateAsSnapshot extends SubscriptionInitialStateMode

  /**
   * When using this subscription mode the events corresponding
   * to the current state will be sent to the subscriber to mimic what you would
   * have seen if you were listening to the events when they occurred in the past.
   */
  case object InitialStateAsEvents extends SubscriptionInitialStateMode

  /**
   * Java API
   */
  def initialStateAsSnapshot = InitialStateAsSnapshot

  /**
   * Java API
   */
  def initialStateAsEvents = InitialStateAsEvents

  /**
   * Marker interface for cluster domain events.
   *
   * Not intended for user extension.
   */
  @DoNotInherit
  trait ClusterDomainEvent extends DeadLetterSuppression

  // for binary compatibility (used to be a case class)
  object CurrentClusterState
      extends AbstractFunction5[
        immutable.SortedSet[Member],
        Set[Member],
        Set[Address],
        Option[Address],
        Map[String, Option[Address]],
        CurrentClusterState] {

    @nowarn("msg=deprecated")
    def apply(
        members: immutable.SortedSet[Member] = immutable.SortedSet.empty,
        unreachable: Set[Member] = Set.empty,
        seenBy: Set[Address] = Set.empty,
        leader: Option[Address] = None,
        roleLeaderMap: Map[String, Option[Address]] = Map.empty): CurrentClusterState =
      new CurrentClusterState(members, unreachable, seenBy, leader, roleLeaderMap)

    def unapply(cs: CurrentClusterState): Option[
      (immutable.SortedSet[Member], Set[Member], Set[Address], Option[Address], Map[String, Option[Address]])] =
      Some((cs.members, cs.unreachable, cs.seenBy, cs.leader, cs.roleLeaderMap))

  }

  /**
   * Current snapshot state of the cluster. Sent to new subscriber.
   *
   * @param leader leader of the data center of this node
   * @param memberTombstones INTERNAL API
   */
  @SerialVersionUID(2)
  @nowarn("msg=Use Akka Distributed Cluster")
  final class CurrentClusterState(
      val members: immutable.SortedSet[Member],
      val unreachable: Set[Member],
      val seenBy: Set[Address],
      val leader: Option[Address],
      val roleLeaderMap: Map[String, Option[Address]],
      val unreachableDataCenters: Set[DataCenter],
      @InternalApi private[akka] val memberTombstones: Set[UniqueAddress])
      extends Product5[
        immutable.SortedSet[Member],
        Set[Member],
        Set[Address],
        Option[Address],
        Map[String, Option[Address]]]
      with Serializable {

    // for binary compatibility
    @deprecated("use main constructor", since = "2.6.10")
    def this(
        members: immutable.SortedSet[Member],
        unreachable: Set[Member],
        seenBy: Set[Address],
        leader: Option[Address],
        roleLeaderMap: Map[String, Option[Address]],
        unreachableDataCenters: Set[DataCenter]) =
      this(members, unreachable, seenBy, leader, roleLeaderMap, unreachableDataCenters, Set.empty)

    // for binary compatibility
    @deprecated("use main constructor", since = "2.6.10")
    def this(
        members: immutable.SortedSet[Member] = immutable.SortedSet.empty,
        unreachable: Set[Member] = Set.empty,
        seenBy: Set[Address] = Set.empty,
        leader: Option[Address] = None,
        roleLeaderMap: Map[String, Option[Address]] = Map.empty) =
      this(members, unreachable, seenBy, leader, roleLeaderMap, Set.empty, Set.empty)

    /**
     * Java API: get current member list.
     */
    def getMembers: java.lang.Iterable[Member] = {
      import scala.jdk.CollectionConverters._
      members.asJava
    }

    /**
     * Java API: get current unreachable set.
     */
    def getUnreachable: java.util.Set[Member] = {
      unreachable.asJava
    }

    /**
     * Java API: All data centers in the cluster
     */
    def getUnreachableDataCenters: java.util.Set[String] = unreachableDataCenters.asJava

    /**
     * Java API: get current “seen-by” set.
     */
    def getSeenBy: java.util.Set[Address] = seenBy.asJava

    /**
     * Java API: get address of current data center leader, or null if none
     */
    def getLeader: Address = leader orNull

    /**
     * get address of current leader, if any, within the data center that has the given role
     */
    def roleLeader(role: String): Option[Address] = roleLeaderMap.getOrElse(role, None)

    /**
     * Java API: get address of current leader, if any, within the data center that has the given role
     * or null if no such node exists
     */
    def getRoleLeader(role: String): Address = roleLeaderMap.get(role).flatten.orNull

    /**
     * All node roles in the cluster
     */
    def allRoles: Set[String] = roleLeaderMap.keySet

    /**
     * Java API: All node roles in the cluster
     */
    def getAllRoles: java.util.Set[String] = allRoles.asJava

    /**
     * All data centers in the cluster
     */
    @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
    def allDataCenters: Set[String] = members.iterator.map(_.dataCenter).to(immutable.Set)

    /**
     * Java API: All data centers in the cluster
     */
    def getAllDataCenters: java.util.Set[String] = allDataCenters.asJava

    /**
     * Replace the set of unreachable datacenters with the given set
     */
    def withUnreachableDataCenters(unreachableDataCenters: Set[DataCenter]): CurrentClusterState =
      new CurrentClusterState(
        members,
        unreachable,
        seenBy,
        leader,
        roleLeaderMap,
        unreachableDataCenters,
        memberTombstones)

    def withMemberTombstones(memberTombstones: Set[UniqueAddress]): CurrentClusterState =
      new CurrentClusterState(
        members,
        unreachable,
        seenBy,
        leader,
        roleLeaderMap,
        unreachableDataCenters,
        memberTombstones)

    /**
     * INTERNAL API
     * Returns true if the address is a cluster member and that member is `MemberStatus.Up`.
     */
    @InternalApi
    private[akka] def isMemberUp(address: Address): Boolean =
      members.exists(m => m.address == address && m.status == MemberStatus.Up)

    // for binary compatibility (used to be a case class)
    def copy(
        members: immutable.SortedSet[Member] = this.members,
        unreachable: Set[Member] = this.unreachable,
        seenBy: Set[Address] = this.seenBy,
        leader: Option[Address] = this.leader,
        roleLeaderMap: Map[String, Option[Address]] = this.roleLeaderMap) =
      new CurrentClusterState(
        members,
        unreachable,
        seenBy,
        leader,
        roleLeaderMap,
        unreachableDataCenters,
        memberTombstones)

    override def equals(other: Any): Boolean = other match {
      case that: CurrentClusterState =>
        (this eq that) || (members == that.members &&
        unreachable == that.unreachable &&
        seenBy == that.seenBy &&
        leader == that.leader &&
        roleLeaderMap == that.roleLeaderMap)
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(members, unreachable, seenBy, leader, roleLeaderMap)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

    // Product5
    override def productPrefix = "CurrentClusterState"
    def _1: SortedSet[Member] = members
    def _2: Set[Member] = unreachable
    def _3: Set[Address] = seenBy
    def _4: Option[Address] = leader
    def _5: Map[String, Option[Address]] = roleLeaderMap
    def canEqual(that: Any): Boolean = that.isInstanceOf[CurrentClusterState]

    override def toString = s"CurrentClusterState($members, $unreachable, $seenBy, $leader, $roleLeaderMap)"
  }

  /**
   * Marker interface for membership events.
   * Published when the state change is first seen on a node.
   * The state change was performed by the leader when there was
   * convergence on the leader node, i.e. all members had seen previous
   * state.
   */
  sealed trait MemberEvent extends ClusterDomainEvent {
    def member: Member
  }

  /**
   * Member status changed to Joining.
   */
  final case class MemberJoined(member: Member) extends MemberEvent {
    if (member.status != Joining) throw new IllegalArgumentException("Expected Joining status, got: " + member)
  }

  /**
   * Member status changed to WeaklyUp.
   * A joining member can be moved to `WeaklyUp` if convergence
   * cannot be reached, i.e. there are unreachable nodes.
   * It will be moved to `Up` when convergence is reached.
   */
  final case class MemberWeaklyUp(member: Member) extends MemberEvent {
    if (member.status != WeaklyUp) throw new IllegalArgumentException("Expected WeaklyUp status, got: " + member)
  }

  /**
   * Member status changed to Up.
   */
  final case class MemberUp(member: Member) extends MemberEvent {
    if (member.status != Up) throw new IllegalArgumentException("Expected Up status, got: " + member)
  }

  /**
   * Member status changed to Leaving.
   */
  final case class MemberLeft(member: Member) extends MemberEvent {
    if (member.status != Leaving) throw new IllegalArgumentException("Expected Leaving status, got: " + member)
  }

  final case class MemberPreparingForShutdown(member: Member) extends MemberEvent {
    if (member.status != PreparingForShutdown)
      throw new IllegalArgumentException("Expected PreparingForShutdown status, got: " + member)
  }

  final case class MemberReadyForShutdown(member: Member) extends MemberEvent {
    if (member.status != ReadyForShutdown)
      throw new IllegalArgumentException("Expected ReadyForShutdown status, got: " + member)
  }

  /**
   * Member status changed to `MemberStatus.Exiting` and will be removed
   * when all members have seen the `Exiting` status.
   */
  final case class MemberExited(member: Member) extends MemberEvent {
    if (member.status != Exiting) throw new IllegalArgumentException("Expected Exiting status, got: " + member)
  }

  /**
   * Member status changed to `MemberStatus.Down` and will be removed
   * when all members have seen the `Down` status.
   */
  final case class MemberDowned(member: Member) extends MemberEvent {
    if (member.status != Down) throw new IllegalArgumentException("Expected Down status, got: " + member)
  }

  /**
   * Member completely removed from the cluster.
   * When `previousStatus` is `MemberStatus.Down` the node was removed
   * after being detected as unreachable and downed.
   * When `previousStatus` is `MemberStatus.Exiting` the node was removed
   * after graceful leaving and exiting.
   */
  final case class MemberRemoved(member: Member, previousStatus: MemberStatus) extends MemberEvent {
    if (member.status != Removed) throw new IllegalArgumentException("Expected Removed status, got: " + member)
  }

  /**
   * Leader of the cluster data center of this node changed. Published when the state change
   * is first seen on a node.
   */
  final case class LeaderChanged(leader: Option[Address]) extends ClusterDomainEvent {

    /**
     * Java API
     * @return address of current leader, or null if none
     */
    def getLeader: Address = leader orNull
  }

  /**
   * First member (leader) of the members within a role set (in the same data center as this node,
   * if data centers are used) changed.
   * Published when the state change is first seen on a node.
   */
  final case class RoleLeaderChanged(role: String, leader: Option[Address]) extends ClusterDomainEvent {

    /**
     * Java API
     * @return address of current leader, or null if none
     */
    def getLeader: Address = leader orNull
  }

  /**
   * This event is published when the cluster node is shutting down,
   * before the final [[MemberRemoved]] events are published.
   */
  case object ClusterShuttingDown extends ClusterDomainEvent

  /**
   * Java API: get the singleton instance of `ClusterShuttingDown` event
   */
  def getClusterShuttingDownInstance = ClusterShuttingDown

  /**
   * Marker interface to facilitate subscription of
   * both [[UnreachableMember]] and [[ReachableMember]].
   */
  sealed trait ReachabilityEvent extends ClusterDomainEvent {
    def member: Member
  }

  /**
   * A member is considered as unreachable by the failure detector.
   */
  final case class UnreachableMember(member: Member) extends ReachabilityEvent

  /**
   * A member is considered as reachable by the failure detector
   * after having been unreachable.
   * @see [[UnreachableMember]]
   */
  final case class ReachableMember(member: Member) extends ReachabilityEvent

  /**
   * Marker interface to facilitate subscription of
   * both [[UnreachableDataCenter]] and [[ReachableDataCenter]].
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  sealed trait DataCenterReachabilityEvent extends ClusterDomainEvent

  /**
   * A data center is considered as unreachable when any members from the data center are unreachable
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  final case class UnreachableDataCenter(dataCenter: DataCenter) extends DataCenterReachabilityEvent

  /**
   * A data center is considered reachable when all members from the data center are reachable
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  final case class ReachableDataCenter(dataCenter: DataCenter) extends DataCenterReachabilityEvent

  /**
   * INTERNAL API
   * The nodes that have seen current version of the Gossip.
   */
  @InternalApi
  private[cluster] final case class SeenChanged(convergence: Boolean, seenBy: Set[Address]) extends ClusterDomainEvent

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] final case class ReachabilityChanged(reachability: Reachability) extends ClusterDomainEvent

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] final case class CurrentInternalStats(gossipStats: GossipStats, vclockStats: VectorClockStats)
      extends ClusterDomainEvent

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] final case class MemberTombstonesChanged(tombstones: Set[UniqueAddress]) extends ClusterDomainEvent

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffUnreachable(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[UnreachableMember] =
    if (newState eq oldState) Nil
    else {
      val newGossip = newState.latestGossip
      val oldUnreachableNodes = oldState.dcReachabilityNoOutsideNodes.allUnreachableOrTerminated
      newState.dcReachabilityNoOutsideNodes.allUnreachableOrTerminated.iterator
        .collect {
          case node if !oldUnreachableNodes.contains(node) && node != newState.selfUniqueAddress =>
            UnreachableMember(newGossip.member(node))
        }
        .to(immutable.IndexedSeq)
    }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffReachable(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[ReachableMember] =
    if (newState eq oldState) Nil
    else {
      val newGossip = newState.latestGossip
      oldState.dcReachabilityNoOutsideNodes.allUnreachable.iterator
        .collect {
          case node
              if newGossip.hasMember(node) && newState.dcReachabilityNoOutsideNodes.isReachable(node) && node != newState.selfUniqueAddress =>
            ReachableMember(newGossip.member(node))
        }
        .to(immutable.IndexedSeq)
    }

  /**
   * Internal API
   */
  @InternalApi
  private[cluster] def isDataCenterReachable(state: MembershipState)(otherDc: DataCenter): Boolean = {
    val unrelatedDcNodes = state.latestGossip.members.collect {
      case m if m.dataCenter != otherDc && m.dataCenter != state.selfDc => m.uniqueAddress
    }

    val reachabilityForOtherDc = state.dcReachabilityWithoutObservationsWithin.remove(unrelatedDcNodes)
    reachabilityForOtherDc.allUnreachable.isEmpty
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffUnreachableDataCenter(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[UnreachableDataCenter] = {
    if (newState eq oldState) Nil
    else {
      val otherDcs = (oldState.latestGossip.allDataCenters
          .union(newState.latestGossip.allDataCenters)) - newState.selfDc

      val oldUnreachableDcs = otherDcs.filterNot(isDataCenterReachable(oldState))
      val currentUnreachableDcs = otherDcs.filterNot(isDataCenterReachable(newState))

      currentUnreachableDcs.diff(oldUnreachableDcs).iterator.map(UnreachableDataCenter.apply).to(immutable.IndexedSeq)
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffReachableDataCenter(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[ReachableDataCenter] = {
    if (newState eq oldState) Nil
    else {
      val otherDcs = (oldState.latestGossip.allDataCenters
          .union(newState.latestGossip.allDataCenters)) - newState.selfDc

      val oldUnreachableDcs = otherDcs.filterNot(isDataCenterReachable(oldState))
      val currentUnreachableDcs = otherDcs.filterNot(isDataCenterReachable(newState))

      oldUnreachableDcs.diff(currentUnreachableDcs).iterator.map(ReachableDataCenter.apply).to(immutable.IndexedSeq)
    }
  }

  /**
   * INTERNAL API.
   */
  @InternalApi
  private[cluster] def diffMemberEvents(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[MemberEvent] =
    if (newState eq oldState) Nil
    else {
      val oldGossip = oldState.latestGossip
      val newGossip = newState.latestGossip
      val newMembers = newGossip.members.diff(oldGossip.members)
      val membersGroupedByAddress = List(newGossip.members, oldGossip.members).flatten.groupBy(_.uniqueAddress)
      val changedMembers = membersGroupedByAddress.collect {
        case (_, newMember :: oldMember :: Nil)
            if newMember.status != oldMember.status || newMember.upNumber != oldMember.upNumber =>
          newMember
      }
      val memberEvents = (newMembers ++ changedMembers).unsorted.collect {
        case m if m.status == Joining              => MemberJoined(m)
        case m if m.status == WeaklyUp             => MemberWeaklyUp(m)
        case m if m.status == Up                   => MemberUp(m)
        case m if m.status == Leaving              => MemberLeft(m)
        case m if m.status == Exiting              => MemberExited(m)
        case m if m.status == Down                 => MemberDowned(m)
        case m if m.status == PreparingForShutdown => MemberPreparingForShutdown(m)
        case m if m.status == ReadyForShutdown     => MemberReadyForShutdown(m)
        // no events for other transitions
      }

      val removedMembers = oldGossip.members.diff(newGossip.members)
      val removedEvents = removedMembers.unsorted.map(m => MemberRemoved(m.copy(status = Removed), m.status))

      (new VectorBuilder[MemberEvent]() ++= removedEvents ++= memberEvents).result()
    }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffLeader(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[LeaderChanged] = {
    val newLeader = newState.leader
    if (newLeader != oldState.leader) List(LeaderChanged(newLeader.map(_.address)))
    else Nil
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffRolesLeader(oldState: MembershipState, newState: MembershipState): Set[RoleLeaderChanged] = {
    for {
      role <- oldState.latestGossip.allRoles.union(newState.latestGossip.allRoles)
      newLeader = newState.roleLeader(role)
      if newLeader != oldState.roleLeader(role)
    } yield RoleLeaderChanged(role, newLeader.map(_.address))
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffSeen(oldState: MembershipState, newState: MembershipState): immutable.Seq[SeenChanged] =
    if (oldState eq newState) Nil
    else {
      val newConvergence = newState.convergence(Set.empty)
      val newSeenBy = newState.latestGossip.seenBy
      if (newConvergence != oldState.convergence(Set.empty) || newSeenBy != oldState.latestGossip.seenBy)
        List(SeenChanged(newConvergence, newSeenBy.map(_.address)))
      else Nil
    }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffReachability(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[ReachabilityChanged] =
    if (newState.overview.reachability eq oldState.overview.reachability) Nil
    else List(ReachabilityChanged(newState.overview.reachability))

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def diffTombstones(
      oldState: MembershipState,
      newState: MembershipState): immutable.Seq[MemberTombstonesChanged] =
    if (newState.latestGossip.tombstones == oldState.latestGossip.tombstones) Nil
    else MemberTombstonesChanged(newState.latestGossip.tombstones.keySet) :: Nil

  /**
   * INTERNAL API
   */
  @InternalApi
  private[cluster] def publishDiff(oldState: MembershipState, newState: MembershipState, pub: AnyRef => Unit): Unit = {
    diffTombstones(oldState, newState).foreach(pub)
    diffMemberEvents(oldState, newState).foreach(pub)
    diffUnreachable(oldState, newState).foreach(pub)
    diffReachable(oldState, newState).foreach(pub)
    diffUnreachableDataCenter(oldState, newState).foreach(pub)
    diffReachableDataCenter(oldState, newState).foreach(pub)
    diffLeader(oldState, newState).foreach(pub)
    diffRolesLeader(oldState, newState).foreach(pub)
    // publish internal SeenState for testing purposes
    diffSeen(oldState, newState).foreach(pub)
    diffReachability(oldState, newState).foreach(pub)
  }

}

/**
 * INTERNAL API.
 * Responsible for domain event subscriptions and publishing of
 * domain events to event bus.
 */
@InternalApi
private[cluster] final class ClusterDomainEventPublisher
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
  import InternalClusterAction._
  import ClusterEvent._

  val cluster = Cluster(context.system)
  val selfUniqueAddress = cluster.selfUniqueAddress
  val emptyMembershipState = MembershipState(
    Gossip.empty,
    cluster.selfUniqueAddress,
    cluster.settings.SelfDataCenter,
    cluster.settings.MultiDataCenter.CrossDcConnections)
  var membershipState: MembershipState = emptyMembershipState
  def selfDc = cluster.settings.SelfDataCenter

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    // don't postStop when restarted, no children to stop
  }

  override def postStop(): Unit = {
    // publish the final removed state before shutting down
    publish(ClusterShuttingDown)
    publishChanges(emptyMembershipState)
  }

  def receive = {
    case PublishChanges(newState)            => publishChanges(newState)
    case currentStats: CurrentInternalStats  => publishInternalStats(currentStats)
    case SendCurrentClusterState(receiver)   => sendCurrentClusterState(receiver)
    case Subscribe(subscriber, initMode, to) => subscribe(subscriber, initMode, to)
    case Unsubscribe(subscriber, to)         => unsubscribe(subscriber, to)
    case PublishEvent(event)                 => publish(event)
  }

  def eventStream: EventStream = context.system.eventStream

  /**
   * The current snapshot state corresponding to latest gossip
   * to mimic what you would have seen if you were listening to the events.
   */
  def sendCurrentClusterState(receiver: ActorRef): Unit = {
    val unreachable: Set[Member] =
      membershipState.dcReachabilityNoOutsideNodes.allUnreachableOrTerminated.collect {
        case node if node != selfUniqueAddress => membershipState.latestGossip.member(node)
      }

    val unreachableDataCenters: Set[DataCenter] =
      if (!membershipState.latestGossip.isMultiDc) Set.empty
      else membershipState.latestGossip.allDataCenters.filterNot(isDataCenterReachable(membershipState))

    val state = new CurrentClusterState(
      members = membershipState.latestGossip.members,
      unreachable = unreachable,
      seenBy = membershipState.latestGossip.seenBy.map(_.address),
      leader = membershipState.leader.map(_.address),
      roleLeaderMap = membershipState.latestGossip.allRoles.iterator
        .map(r => r -> membershipState.roleLeader(r).map(_.address))
        .toMap,
      unreachableDataCenters = unreachableDataCenters,
      memberTombstones = membershipState.latestGossip.tombstones.keySet)
    receiver ! state
  }

  def subscribe(subscriber: ActorRef, initMode: SubscriptionInitialStateMode, to: Set[Class[_]]): Unit = {
    initMode match {
      case InitialStateAsEvents =>
        def pub(event: AnyRef): Unit = {
          if (to.exists(_.isAssignableFrom(event.getClass)))
            subscriber ! event
        }
        publishDiff(emptyMembershipState, membershipState, pub)
      case InitialStateAsSnapshot =>
        sendCurrentClusterState(subscriber)
    }

    to.foreach { eventStream.subscribe(subscriber, _) }
  }

  def unsubscribe(subscriber: ActorRef, to: Option[Class[_]]): Unit = to match {
    case None    => eventStream.unsubscribe(subscriber)
    case Some(c) => eventStream.unsubscribe(subscriber, c)
  }

  def publishChanges(newState: MembershipState): Unit = {
    val oldState = membershipState
    // keep the latest state to be sent to new subscribers
    membershipState = newState
    publishDiff(oldState, newState, publish)
  }

  def publishInternalStats(currentStats: CurrentInternalStats): Unit = publish(currentStats)

  def publish(event: AnyRef): Unit = eventStream.publish(event)

  def clearState(): Unit = {
    membershipState = emptyMembershipState
  }
}
