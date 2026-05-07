/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.Address
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.Reachability
import akka.cluster.UniqueAddress
import akka.coordination.lease.scaladsl.Lease

/**
 * INTERNAL API
 */
@InternalApi private[akka] object DowningStrategy {
  sealed trait Decision {
    def isIndirectlyConnected: Boolean
  }
  case object DownReachable extends Decision {
    override def isIndirectlyConnected = false
  }
  case object DownUnreachable extends Decision {
    override def isIndirectlyConnected = false
  }
  case object DownAll extends Decision {
    override def isIndirectlyConnected = false
  }
  case object DownIndirectlyConnected extends Decision {
    override def isIndirectlyConnected = true
  }
  sealed trait AcquireLeaseDecision extends Decision {
    def acquireDelay: FiniteDuration
  }
  final case class AcquireLeaseAndDownUnreachable(acquireDelay: FiniteDuration) extends AcquireLeaseDecision {
    override def isIndirectlyConnected = false
  }
  final case class AcquireLeaseAndDownIndirectlyConnected(acquireDelay: FiniteDuration) extends AcquireLeaseDecision {
    override def isIndirectlyConnected = true
  }
  case object ReverseDownIndirectlyConnected extends Decision {
    override def isIndirectlyConnected = true
  }
  case object DownSelfQuarantinedByRemote extends Decision {
    override def isIndirectlyConnected: Boolean = false
  }
}

/**
 * INTERNAL API
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[akka] abstract class DowningStrategy(val selfDc: DataCenter, selfUniqueAddress: UniqueAddress) {
  import DowningStrategy._

  // may contain Joining and WeaklyUp
  private var _unreachable: Set[UniqueAddress] = Set.empty[UniqueAddress]

  @InternalStableApi
  def unreachable: Set[UniqueAddress] = _unreachable

  def unreachable(m: Member): Boolean = _unreachable(m.uniqueAddress)

  private var _reachability: Reachability = Reachability.empty

  private var _seenBy: Set[Address] = Set.empty

  protected def ordering: Ordering[Member] = Member.ordering

  // all members in self DC, both joining and up.
  private var _allMembers: immutable.SortedSet[Member] = immutable.SortedSet.empty(ordering)

  def role: Option[String]

  // all Joining and WeaklyUp members in self DC
  def joining: immutable.SortedSet[Member] =
    _allMembers.filter(m => m.status == MemberStatus.Joining || m.status == MemberStatus.WeaklyUp)

  // all members in self DC, both joining and up.
  @InternalStableApi
  def allMembersInDC: immutable.SortedSet[Member] = _allMembers

  /**
   * All members in self DC, but doesn't contain Joining, WeaklyUp, Down and Exiting.
   */
  @InternalStableApi
  def members: immutable.SortedSet[Member] =
    members(includingPossiblyUp = false, excludingPossiblyExiting = false)

  /**
   * All members in self DC, but doesn't contain Joining, WeaklyUp, Down and Exiting.
   *
   * When `includingPossiblyUp=true` it also includes Joining and WeaklyUp members that could have been
   * changed to Up on the other side of a partition.
   *
   * When `excludingPossiblyExiting=true` it doesn't include Leaving members that could have been
   * changed to Exiting on the other side of the partition.
   */
  def members(includingPossiblyUp: Boolean, excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] =
    _allMembers.filterNot(
      m =>
        (!includingPossiblyUp && m.status == MemberStatus.Joining) ||
        (!includingPossiblyUp && m.status == MemberStatus.WeaklyUp) ||
        (excludingPossiblyExiting && m.status == MemberStatus.Leaving) ||
        m.status == MemberStatus.Down ||
        m.status == MemberStatus.Exiting)

  def membersWithRole: immutable.SortedSet[Member] =
    membersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = false)

  def membersWithRole(includingPossiblyUp: Boolean, excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] =
    role match {
      case None    => members(includingPossiblyUp, excludingPossiblyExiting)
      case Some(r) => members(includingPossiblyUp, excludingPossiblyExiting).filter(_.hasRole(r))
    }

  def reachableMembers: immutable.SortedSet[Member] =
    reachableMembers(includingPossiblyUp = false, excludingPossiblyExiting = false)

  def reachableMembers(includingPossiblyUp: Boolean, excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] = {
    val mbrs = members(includingPossiblyUp, excludingPossiblyExiting)
    if (unreachable.isEmpty) mbrs
    else mbrs.filter(m => !unreachable(m))
  }

  def reachableMembersWithRole: immutable.SortedSet[Member] =
    reachableMembersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = false)

  def reachableMembersWithRole(
      includingPossiblyUp: Boolean,
      excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] =
    role match {
      case None    => reachableMembers(includingPossiblyUp, excludingPossiblyExiting)
      case Some(r) => reachableMembers(includingPossiblyUp, excludingPossiblyExiting).filter(_.hasRole(r))
    }

  def unreachableMembers: immutable.SortedSet[Member] =
    unreachableMembers(includingPossiblyUp = false, excludingPossiblyExiting = false)

  def unreachableMembers(
      includingPossiblyUp: Boolean,
      excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] = {
    if (unreachable.isEmpty) immutable.SortedSet.empty
    else members(includingPossiblyUp, excludingPossiblyExiting).filter(unreachable)
  }

  def unreachableMembersWithRole: immutable.SortedSet[Member] =
    unreachableMembersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = false)

  def unreachableMembersWithRole(
      includingPossiblyUp: Boolean,
      excludingPossiblyExiting: Boolean): immutable.SortedSet[Member] =
    role match {
      case None    => unreachableMembers(includingPossiblyUp, excludingPossiblyExiting)
      case Some(r) => unreachableMembers(includingPossiblyUp, excludingPossiblyExiting).filter(_.hasRole(r))
    }

  def addUnreachable(m: Member): Unit = {
    require(m.dataCenter == selfDc)

    add(m)
    _unreachable = _unreachable + m.uniqueAddress
  }

  def addReachable(m: Member): Unit = {
    require(m.dataCenter == selfDc)

    add(m)
    _unreachable = _unreachable - m.uniqueAddress
  }

  def add(m: Member): Unit = {
    require(m.dataCenter == selfDc)

    removeFromAllMembers(m)
    _allMembers += m
  }

  def remove(m: Member): Unit = {
    require(m.dataCenter == selfDc)

    removeFromAllMembers(m)
    _unreachable -= m.uniqueAddress
  }

  private def removeFromAllMembers(m: Member): Unit = {
    if (ordering eq Member.ordering) {
      _allMembers -= m
    } else {
      // must use filterNot for removals/replace in the SortedSet when
      // ageOrdering is using upNumber and that will change when Joining -> Up
      _allMembers = _allMembers.filterNot(_.uniqueAddress == m.uniqueAddress)
    }
  }

  @InternalStableApi
  def reachability: Reachability =
    _reachability

  private def isInSelfDc(node: UniqueAddress): Boolean = {
    _allMembers.exists(m => m.uniqueAddress == node && m.dataCenter == selfDc)
  }

  private[sbr] def setReachability(r: Reachability): Unit = {
    // skip records with Reachability.Reachable, and skip records related to other DC
    _reachability = r.filterRecords(
      record =>
        (record.status == Reachability.Unreachable || record.status == Reachability.Terminated) &&
        isInSelfDc(record.observer) && isInSelfDc(record.subject))
  }

  def seenBy: Set[Address] =
    _seenBy

  def setSeenBy(s: Set[Address]): Unit =
    _seenBy = s

  /**
   * Nodes that are marked as unreachable but can communicate with gossip via a 3rd party.
   *
   * Cycle in unreachability graph corresponds to that some node is both
   * observing another node as unreachable, and is also observed as unreachable by someone
   * else.
   *
   * Another indication of indirectly connected nodes is if a node is marked as unreachable,
   * but it has still marked current gossip state as seen.
   *
   * Those cases will not happen for clean splits and crashed nodes.
   */
  def indirectlyConnected: Set[UniqueAddress] = {
    indirectlyConnectedFromIntersectionOfObserversAndSubjects.union(indirectlyConnectedFromSeenCurrentGossip)
  }

  private def indirectlyConnectedFromIntersectionOfObserversAndSubjects: Set[UniqueAddress] = {
    // cycle in unreachability graph
    val observers = reachability.allObservers
    observers.intersect(reachability.allUnreachableOrTerminated)
  }

  private def indirectlyConnectedFromSeenCurrentGossip: Set[UniqueAddress] = {
    reachability.records.flatMap { r =>
      if (seenBy(r.subject.address)) r.observer :: r.subject :: Nil
      else Nil
    }.toSet
  }

  def hasIndirectlyConnected: Boolean = indirectlyConnected.nonEmpty

  def unreachableButNotIndirectlyConnected: Set[UniqueAddress] = unreachable.diff(indirectlyConnected)

  def nodesToDown(decision: Decision = decide()): Set[UniqueAddress] = {
    val downable = members
      .filterNot(m => m.status == MemberStatus.Down || m.status == MemberStatus.Exiting)
      .union(joining)
      .map(_.uniqueAddress)

    decision match {
      case DownUnreachable | AcquireLeaseAndDownUnreachable(_)                 => downable.intersect(unreachable)
      case DownReachable                                                       => downable.diff(unreachable)
      case DownAll                                                             => downable
      case DownIndirectlyConnected | AcquireLeaseAndDownIndirectlyConnected(_) =>
        // Down nodes that have been marked as unreachable via some network links but they are still indirectly
        // connected via other links. It will keep other "normal" nodes.
        // If there is a combination of indirectly connected nodes and a clean network partition (or node crashes)
        // it will combine the above decision with the ordinary decision, e.g. keep majority, after excluding
        // failure detection observations between the indirectly connected nodes.
        // Also include nodes that corresponds to the decision without the unreachability observations from
        // the indirectly connected nodes
        downable.intersect(indirectlyConnected.union(additionalNodesToDownWhenIndirectlyConnected(downable)))
      case ReverseDownIndirectlyConnected =>
        // indirectly connected + all reachable
        downable.intersect(indirectlyConnected).union(downable.diff(unreachable))
      case DownSelfQuarantinedByRemote =>
        if (downable.contains(selfUniqueAddress)) Set(selfUniqueAddress)
        else Set.empty
    }
  }

  private def additionalNodesToDownWhenIndirectlyConnected(downable: Set[UniqueAddress]): Set[UniqueAddress] = {
    if (unreachableButNotIndirectlyConnected.isEmpty)
      Set.empty
    else {
      val originalUnreachable = _unreachable
      val originalReachability = _reachability

      try {
        val intersectionOfObserversAndSubjects = indirectlyConnectedFromIntersectionOfObserversAndSubjects
        val haveSeenCurrentGossip = indirectlyConnectedFromSeenCurrentGossip
        // remove records between the indirectly connected
        _reachability = reachability.filterRecords { r =>
          // we only retain records for addresses that are still downable
          downable.contains(r.observer) && downable.contains(r.subject) &&
          // remove records between the indirectly connected
          !(intersectionOfObserversAndSubjects(r.observer) && intersectionOfObserversAndSubjects(r.subject) ||
          haveSeenCurrentGossip(r.observer) && haveSeenCurrentGossip(r.subject))
        }
        _unreachable = reachability.allUnreachableOrTerminated

        val additionalDecision = decide()
        if (additionalDecision.isIndirectlyConnected)
          throw new IllegalStateException(
            s"SBR double $additionalDecision decision, downing all instead. " +
            s"originalReachability: [$originalReachability], filtered reachability [$reachability], " +
            s"still indirectlyConnected: [$indirectlyConnected], seenBy: [$seenBy]")

        nodesToDown(additionalDecision)

      } finally {
        _unreachable = originalUnreachable
        _reachability = originalReachability
      }
    }
  }

  def isAllUnreachableDownOrExiting: Boolean = {
    _unreachable.isEmpty ||
    unreachableMembers.forall(m => m.status == MemberStatus.Down || m.status == MemberStatus.Exiting)
  }

  def reverseDecision(decision: AcquireLeaseDecision): Decision = {
    decision match {
      case AcquireLeaseAndDownUnreachable(_)         => DownReachable
      case AcquireLeaseAndDownIndirectlyConnected(_) => ReverseDownIndirectlyConnected
    }
  }

  def decide(): Decision

  def lease: Option[Lease] = None

}

/**
 * INTERNAL API
 *
 * Down the unreachable nodes if the number of remaining nodes are greater than or equal to the
 * given `quorumSize`. Otherwise down the reachable nodes, i.e. it will shut down that side of the partition.
 * In other words, the `quorumSize` defines the minimum number of nodes that the cluster must have to be operational.
 * If there are unreachable nodes when starting up the cluster, before reaching this limit,
 * the cluster may shutdown itself immediately. This is not an issue if you start all nodes at
 * approximately the same time.
 *
 * Note that you must not add more members to the cluster than `quorumSize * 2 - 1`, because then
 * both sides may down each other and thereby form two separate clusters. For example,
 * quorum quorumSize configured to 3 in a 6 node cluster may result in a split where each side
 * consists of 3 nodes each, i.e. each side thinks it has enough nodes to continue by
 * itself. A warning is logged if this recommendation is violated.
 *
 * If the `role` is defined the decision is based only on members with that `role`.
 *
 * It is only counting members within the own data center.
 */
@InternalApi private[sbr] final class StaticQuorum(
    selfDc: DataCenter,
    val quorumSize: Int,
    override val role: Option[String],
    selfUniqueAddress: UniqueAddress)
    extends DowningStrategy(selfDc, selfUniqueAddress) {
  import DowningStrategy._

  override def decide(): Decision = {
    if (isTooManyMembers)
      DownAll
    else if (hasIndirectlyConnected)
      DownIndirectlyConnected
    else if (membersWithRole.size - unreachableMembersWithRole.size >= quorumSize)
      DownUnreachable
    else
      DownReachable
  }

  def isTooManyMembers: Boolean =
    membersWithRole.size > (quorumSize * 2 - 1)
}

/**
 * INTERNAL API
 *
 * Down the unreachable nodes if the current node is in the majority part based the last known
 * membership information. Otherwise down the reachable nodes, i.e. the own part. If the the
 * parts are of equal size the part containing the node with the lowest address is kept.
 *
 * If the `role` is defined the decision is based only on members with that `role`.
 *
 * Note that if there are more than two partitions and none is in majority each part
 * will shutdown itself, terminating the whole cluster.
 *
 * It is only counting members within the own data center.
 */
@InternalApi private[sbr] final class KeepMajority(
    selfDc: DataCenter,
    override val role: Option[String],
    selfUniqueAddress: UniqueAddress)
    extends DowningStrategy(selfDc, selfUniqueAddress) {
  import DowningStrategy._

  override def decide(): Decision = {
    if (hasIndirectlyConnected)
      DownIndirectlyConnected
    else {
      val ms = membersWithRole
      if (ms.isEmpty)
        DownAll // no node with matching role
      else {
        val reachableSize = reachableMembersWithRole.size
        val unreachableSize = unreachableMembersWithRole.size

        majorityDecision(reachableSize, unreachableSize, ms.head) match {
          case DownUnreachable =>
            majorityDecisionWhenIncludingMembershipChangesEdgeCase() match {
              case DownUnreachable => DownUnreachable // same conclusion
              case _               => DownAll // different conclusion, safest to DownAll
            }
          case decision => decision
        }

      }
    }
  }

  private def majorityDecision(thisSide: Int, otherSide: Int, lowest: Member): Decision = {
    if (thisSide == otherSide) {
      // equal size, keep the side with the lowest address (first in members)
      if (unreachable(lowest)) DownReachable else DownUnreachable
    } else if (thisSide > otherSide) {
      // we are in majority
      DownUnreachable
    } else {
      // we are in minority
      DownReachable
    }
  }

  /**
   * Check for edge case when membership change happens at the same time as partition.
   * Count Joining and WeaklyUp on other side since those might be Up on other side.
   * Don't count Leaving on this side since those might be Exiting on other side.
   * Note that the membership changes we are looking for will only be done when all
   * members have seen previous state, i.e. when a member is moved to Up everybody
   * has seen it joining.
   */
  private def majorityDecisionWhenIncludingMembershipChangesEdgeCase(): Decision = {
    // for this side we count as few as could be possible (excluding joining, excluding leaving)
    val ms = membersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true)
    if (ms.isEmpty) {
      DownAll
    } else {
      val thisSideReachableSize =
        reachableMembersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true).size
      // for other side we count as many as could be possible (including joining, including leaving)
      val otherSideUnreachableSize =
        unreachableMembersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = false).size
      majorityDecision(thisSideReachableSize, otherSideUnreachableSize, ms.head)
    }
  }

}

/**
 * INTERNAL API
 *
 * Down the part that does not contain the oldest member (current singleton).
 *
 * There is one exception to this rule if `downIfAlone` is defined to `true`.
 * Then, if the oldest node has partitioned from all other nodes the oldest will
 * down itself and keep all other nodes running. The strategy will not down the
 * single oldest node when it is the only remaining node in the cluster.
 *
 * Note that if the oldest node crashes the others will remove it from the cluster
 * when `downIfAlone` is `true`, otherwise they will down themselves if the
 * oldest node crashes, i.e. shutdown the whole cluster together with the oldest node.
 *
 * If the `role` is defined the decision is based only on members with that `role`,
 * i.e. using the oldest member (singleton) within the nodes with that role.
 *
 * It is only using members within the own data center, i.e. oldest within the
 * data center.
 */
@InternalApi private[sbr] final class KeepOldest(
    selfDc: DataCenter,
    val downIfAlone: Boolean,
    override val role: Option[String],
    selfUniqueAddress: UniqueAddress)
    extends DowningStrategy(selfDc, selfUniqueAddress) {
  import DowningStrategy._

  // sort by age, oldest first
  override def ordering: Ordering[Member] = Member.ageOrdering

  override def decide(): Decision = {
    if (hasIndirectlyConnected)
      DownIndirectlyConnected
    else {
      val ms = membersWithRole
      if (ms.isEmpty)
        DownAll // no node with matching role
      else {
        val oldest = ms.head
        val oldestIsReachable = !unreachable(oldest)
        val reachableCount = reachableMembersWithRole.size
        val unreachableCount = unreachableMembersWithRole.size

        oldestDecision(oldestIsReachable, reachableCount, unreachableCount) match {
          case DownUnreachable =>
            oldestDecisionWhenIncludingMembershipChangesEdgeCase() match {
              case DownUnreachable => DownUnreachable // same conclusion
              case _               => DownAll // different conclusion, safest to DownAll
            }
          case decision => decision
        }

      }
    }
  }

  private def oldestDecision(oldestIsOnThisSide: Boolean, thisSide: Int, otherSide: Int): Decision = {
    if (oldestIsOnThisSide) {
      // if there are only 2 nodes in the cluster it is better to keep the oldest, even though it is alone
      // E.g. 2 nodes: thisSide=1, otherSide=1 => DownUnreachable, i.e. keep the oldest
      //               even though it is alone (because the node on the other side is no better)
      // E.g. 3 nodes: thisSide=1, otherSide=2 => DownReachable, i.e. shut down the
      //               oldest because it is alone
      if (downIfAlone && thisSide == 1 && otherSide >= 2) DownReachable
      else DownUnreachable
    } else {
      if (downIfAlone && otherSide == 1 && thisSide >= 2) DownUnreachable
      else DownReachable
    }
  }

  /**
   * Check for edge case when membership change happens at the same time as partition.
   * Exclude Leaving on this side because those could be Exiting on other side.
   *
   * When `downIfAlone` also consider Joining and WeaklyUp since those might be Up on other side,
   * and thereby flip the alone test.
   */
  private def oldestDecisionWhenIncludingMembershipChangesEdgeCase(): Decision = {
    val ms = membersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true)
    if (ms.isEmpty) {
      DownAll
    } else {
      val oldest = ms.head
      val oldestIsReachable = !unreachable(oldest)
      // Joining and WeaklyUp are only relevant when downIfAlone = true
      val includingPossiblyUp = downIfAlone
      val reachableCount = reachableMembersWithRole(includingPossiblyUp, excludingPossiblyExiting = true).size
      val unreachableCount = unreachableMembersWithRole(includingPossiblyUp, excludingPossiblyExiting = true).size

      oldestDecision(oldestIsReachable, reachableCount, unreachableCount)
    }
  }
}

/**
 * INTERNAL API
 *
 * Down all nodes unconditionally.
 */
@InternalApi private[sbr] final class DownAllNodes(selfDc: DataCenter, selfUniqueAddress: UniqueAddress)
    extends DowningStrategy(selfDc, selfUniqueAddress) {
  import DowningStrategy._

  override def decide(): Decision =
    DownAll

  override def role: Option[String] = None
}

/**
 * INTERNAL API
 *
 * Keep the part that can acquire the lease, and down the other part.
 *
 * Best effort is to keep the side that has most nodes, i.e. the majority side.
 * This is achieved by adding a delay before trying to acquire the lease on the
 * minority side.
 *
 * If the `role` is defined the majority/minority is based only on members with that `role`.
 * It is only counting members within the own data center.
 */
@InternalApi private[sbr] final class LeaseMajority(
    selfDc: DataCenter,
    override val role: Option[String],
    _lease: Lease,
    acquireLeaseDelayForMinority: FiniteDuration,
    val releaseAfter: FiniteDuration,
    selfUniqueAddress: UniqueAddress)
    extends DowningStrategy(selfDc, selfUniqueAddress) {
  import DowningStrategy._

  override val lease: Option[Lease] = Some(_lease)

  override def decide(): Decision = {
    if (hasIndirectlyConnected)
      AcquireLeaseAndDownIndirectlyConnected(Duration.Zero)
    else
      AcquireLeaseAndDownUnreachable(acquireLeaseDelay)
  }

  private def acquireLeaseDelay: FiniteDuration =
    if (isInMinority) acquireLeaseDelayForMinority else Duration.Zero

  private def isInMinority: Boolean = {
    val ms = membersWithRole
    if (ms.isEmpty)
      false // no node with matching role
    else {
      val unreachableSize = unreachableMembersWithRole.size
      val membersSize = ms.size

      if (unreachableSize * 2 == membersSize) {
        // equal size, try to keep the side with the lowest address (first in members)
        unreachable(ms.head)
      } else if (unreachableSize * 2 < membersSize) {
        // we are in majority
        false
      } else {
        // we are in minority
        true
      }
    }
  }
}
