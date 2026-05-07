/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.collection.immutable

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.PoisonPill
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.ClusterEvent._
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.UnboundedMessageQueueSemantics

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ClusterReadView {
  final case class State(
      clusterState: CurrentClusterState,
      reachability: Reachability,
      selfMember: Member,
      latestStats: CurrentInternalStats)

}

/**
 * INTERNAL API
 *
 * Read view of cluster state, updated via subscription of
 * cluster events published on the event bus.
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[akka] class ClusterReadView(cluster: Cluster) extends Closeable {
  import ClusterReadView.State
  import cluster.ClusterLogger._

  /**
   * State for synchronous read access via [[Cluster]] extension. Only updated from the `eventBusListener` actor.
   */
  private val _state: AtomicReference[State] = new AtomicReference[State](
    State(
      clusterState = CurrentClusterState(),
      reachability = Reachability.empty,
      selfMember = Member(cluster.selfUniqueAddress, cluster.selfRoles, cluster.settings.AppVersion)
        .copy(status = MemberStatus.Removed),
      latestStats = CurrentInternalStats(GossipStats(), VectorClockStats())))

  val selfAddress: Address = cluster.selfAddress

  // create actor that subscribes to the cluster eventBus to update current read view state
  private val eventBusListener: ActorRef = {
    cluster.system
      .systemActorOf(Props(new Actor with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
        override def preStart(): Unit = cluster.subscribe(this.self, classOf[ClusterDomainEvent])

        // make sure that final state has member status Removed
        override def postStop(): Unit = {
          selfRemoved() // make sure it ends as Removed even though MemberRemoved message didn't make it
        }

        private def selfRemoved(): Unit = {
          val oldState = _state.get()
          // keepig latestStats, but otherwise clear everything
          val newState = oldState.copy(
            clusterState = CurrentClusterState(),
            reachability = Reachability.empty,
            selfMember = oldState.selfMember.copy(MemberStatus.Removed))
          _state.set(newState)
        }

        def receive: Receive = {
          case e: ClusterDomainEvent =>
            val oldState = _state.get()
            val oldClusterState = oldState.clusterState
            e match {
              case SeenChanged(_, seenBy) =>
                _state.set(oldState.copy(clusterState = oldClusterState.copy(seenBy = seenBy)))
              case ReachabilityChanged(reachability) =>
                _state.set(oldState.copy(reachability = reachability))
              case MemberRemoved(member, _) if member.address == selfAddress =>
                selfRemoved()
              case MemberRemoved(member, _) =>
                _state.set(
                  oldState.copy(
                    clusterState = oldClusterState.copy(
                      members = oldClusterState.members - member,
                      unreachable = oldClusterState.unreachable - member)))
              case UnreachableMember(member) =>
                // replace current member with new member (might have different status, only address is used in equals)
                _state.set(
                  oldState.copy(
                    clusterState = oldClusterState.copy(unreachable = oldClusterState.unreachable - member + member)))
              case ReachableMember(member) =>
                _state.set(
                  oldState.copy(
                    clusterState = oldClusterState.copy(unreachable = oldClusterState.unreachable - member)))
              case event: MemberEvent =>
                val member = event.member
                // replace current member with new member (might have different status, only address is used in equals)
                val newUnreachable =
                  if (oldClusterState.unreachable.contains(member)) oldClusterState.unreachable - member + member
                  else oldClusterState.unreachable
                val newSelfMember = if (member.address == selfAddress) member else oldState.selfMember
                _state.set(
                  oldState.copy(
                    clusterState = oldClusterState
                      .copy(members = oldClusterState.members - member + member, unreachable = newUnreachable),
                    selfMember = newSelfMember))
              case LeaderChanged(leader) =>
                _state.set(oldState.copy(clusterState = oldClusterState.copy(leader = leader)))
              case RoleLeaderChanged(role, leader) =>
                _state.set(
                  oldState.copy(clusterState =
                    oldClusterState.copy(roleLeaderMap = oldClusterState.roleLeaderMap + (role -> leader))))
              case stats: CurrentInternalStats =>
                _state.set(oldState.copy(latestStats = stats))
              case ClusterShuttingDown =>
              case r: ReachableDataCenter =>
                _state.set(
                  oldState.copy(clusterState =
                    oldClusterState.withUnreachableDataCenters(oldClusterState.unreachableDataCenters - r.dataCenter)))
              case r: UnreachableDataCenter =>
                _state.set(
                  oldState.copy(clusterState =
                    oldClusterState.withUnreachableDataCenters(oldClusterState.unreachableDataCenters + r.dataCenter)))
              case MemberTombstonesChanged(tombstones) =>
                _state.set(oldState.copy(clusterState = oldClusterState.withMemberTombstones(tombstones)))
              case unexpected =>
                throw new IllegalArgumentException(s"Unexpected cluster event type ${unexpected.getClass}") // compiler exhaustiveness check pleaser
            }

            // once captured, optional verbose logging of event
            logInfoVerbose(e)

          case s: CurrentClusterState =>
            val oldState = _state.get()
            val newSelfMember =
              s.members.find(_.uniqueAddress == cluster.selfUniqueAddress).getOrElse(oldState.selfMember)
            _state.set(oldState.copy(clusterState = s, selfMember = newSelfMember))
        }
      }).withDispatcher(cluster.settings.UseDispatcher).withDeploy(Deploy.local), name = "clusterEventBusListener")
  }

  def state: CurrentClusterState = _state.get().clusterState

  def self: Member = _state.get().selfMember

  /**
   * Returns true if this cluster instance has be shutdown.
   */
  def isTerminated: Boolean = cluster.isTerminated

  /**
   * Current cluster members, sorted by address.
   */
  def members: immutable.SortedSet[Member] = _state.get().clusterState.members

  /**
   * Members that has been detected as unreachable.
   */
  def unreachableMembers: Set[Member] = _state.get().clusterState.unreachable

  /**
   * Member status for this node ([[akka.cluster.MemberStatus]]).
   *
   * NOTE: If the node has been removed from the cluster (and shut down) then it's status is set to the 'REMOVED' tombstone state
   *       and is no longer present in the node ring or any other part of the gossiping state. However in order to maintain the
   *       model and the semantics the user would expect, this method will in this situation return `MemberStatus.Removed`.
   */
  def status: MemberStatus = self.status

  /**
   * Is this node the current data center leader
   */
  def isLeader: Boolean = leader.contains(selfAddress)

  /**
   * Get the address of the current data center leader
   */
  def leader: Option[Address] = _state.get().clusterState.leader

  /**
   * Does the cluster consist of only one member?
   */
  def isSingletonCluster: Boolean = members.size == 1

  /**
   * Returns true if the node is not unreachable and not `Down`
   * and not `Removed`.
   */
  def isAvailable: Boolean = {
    val myself = self
    !unreachableMembers.contains(myself) &&
    myself.status != MemberStatus.Down &&
    myself.status != MemberStatus.Removed
  }

  def reachability: Reachability = _state.get().reachability

  /**
   * INTERNAL API
   * The nodes that has seen current version of the Gossip.
   */
  private[cluster] def seenBy: Set[Address] = _state.get().clusterState.seenBy

  /**
   * INTERNAL API
   */
  private[cluster] def latestStats: CurrentInternalStats = _state.get().latestStats

  private def logInfoVerbose(event: ClusterDomainEvent): Unit = {
    if (cluster.settings.LogInfoVerbose) {
      event match {
        case _: SeenChanged | _: CurrentInternalStats => // ignore
        case _: UnreachableMember =>
          val s = state
          logInfo("event {}, {} unreachable members [{}]", event, s.unreachable.size, s.unreachable.mkString(", "))
        case _: ReachableMember =>
          val s = state
          logInfo("event {}, {} unreachable members [{}]", event, s.unreachable.size, s.unreachable.mkString(", "))
        case _: MemberUp =>
          val s = state
          logInfo("event {}, {} members [{}]", event, s.members.size, s.members.mkString(", "))
        case _: MemberRemoved if !cluster.isTerminated =>
          val s = state
          logInfo("event {}, {} members [{}]", event, s.members.size, s.members.mkString(", "))
        case MemberTombstonesChanged(tombstones) =>
          logInfo("event MemberTombstonesChanged({})", tombstones.size)
        case _: ReachabilityChanged =>
        // ignore ReachabilityChanged because it's changed often, and typically not interesting
        case _ =>
          logInfo("event {}", event)
      }
    }
  }

  /**
   * Unsubscribe to cluster events.
   */
  def close(): Unit = {
    if (!eventBusListener.isTerminated)
      eventBusListener ! PoisonPill
  }

}
