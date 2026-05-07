/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.SortedSet
import scala.collection.immutable
import scala.util.Random

import akka.annotation.InternalApi
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.MemberStatus._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object MembershipState {
  import MemberStatus._
  private val leaderMemberStatus = Set[MemberStatus](Up, Leaving, PreparingForShutdown, ReadyForShutdown)
  private val convergenceMemberStatus = Set[MemberStatus](Up, Leaving, PreparingForShutdown, ReadyForShutdown)
  val convergenceSkipUnreachableWithMemberStatus = Set[MemberStatus](Down, Exiting)
  val removeUnreachableWithMemberStatus = Set[MemberStatus](Down, Exiting)
  // If a member hasn't join yet or has already started leaving don't mark it as shutting down
  val allowedToPrepareToShutdown = Set[MemberStatus](Up)
  val prepareForShutdownStates = Set[MemberStatus](PreparingForShutdown, ReadyForShutdown)
}

/**
 * INTERNAL API
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[akka] final case class MembershipState(
    latestGossip: Gossip,
    selfUniqueAddress: UniqueAddress,
    selfDc: DataCenter,
    crossDcConnections: Int) {

  import MembershipState._

  lazy val selfMember = latestGossip.member(selfUniqueAddress)

  def members: immutable.SortedSet[Member] = latestGossip.members

  def overview: GossipOverview = latestGossip.overview

  def seen(): MembershipState = copy(latestGossip = latestGossip.seen(selfUniqueAddress))

  /**
   * Checks if we have a cluster convergence. If there are any in data center node pairs that cannot reach each other
   * then we can't have a convergence until those nodes reach each other again or one of them is downed
   *
   * @return true if convergence have been reached and false if not
   */
  def convergence(exitingConfirmed: Set[UniqueAddress]): Boolean = {

    // full convergence needed for first member in a secondary DC
    val firstMemberInDc =
      !members.exists(member => member.dataCenter == selfDc && convergenceMemberStatus(member.status))

    // If another member in the data center that is UP or LEAVING and has not seen this gossip or is exiting
    // convergence cannot be reached. For the first member in a secondary DC all Joining, WeaklyUp, Up or Leaving
    // members must have seen the gossip state. The reason for the stronger requirement for a first member in a
    // secondary DC is that first member should only be moved to Up once to ensure that the first upNumber is
    // only assigned once.
    def memberHinderingConvergenceExists = {
      val memberStatus = if (firstMemberInDc) convergenceMemberStatus + Joining + WeaklyUp else convergenceMemberStatus
      members.exists(
        member =>
          (firstMemberInDc || member.dataCenter == selfDc) &&
          memberStatus(member.status) &&
          !(latestGossip.seenByNode(member.uniqueAddress) || exitingConfirmed(member.uniqueAddress)))
    }

    // Find cluster members in the data center that are unreachable from other members of the data center
    // excluding observations from members outside of the data center, that have status DOWN or is passed in as confirmed exiting.
    val unreachableInDc = dcReachabilityExcludingDownedObservers.allUnreachableOrTerminated.collect {
      case node if node != selfUniqueAddress && !exitingConfirmed(node) => latestGossip.member(node)
    }
    // unreachables outside of the data center or with status DOWN or EXITING does not affect convergence
    val allUnreachablesCanBeIgnored =
      unreachableInDc.forall(unreachable => convergenceSkipUnreachableWithMemberStatus(unreachable.status))

    allUnreachablesCanBeIgnored && !memberHinderingConvergenceExists
  }

  /**
   * @return Reachability excluding observations from nodes outside of the data center, but including observed unreachable
   *         nodes outside of the data center
   */
  lazy val dcReachability: Reachability =
    overview.reachability.removeObservers(members.collect { case m if m.dataCenter != selfDc => m.uniqueAddress })

  /**
   * @return Reachability excluding observations from nodes outside of the data center and observations within self data center,
   *        but including observed unreachable nodes outside of the data center
   */
  lazy val dcReachabilityWithoutObservationsWithin: Reachability =
    dcReachability.filterRecords { r =>
      latestGossip.member(r.subject).dataCenter != selfDc
    }

  /**
   * @return reachability for data center nodes, with observations from outside the data center or from downed nodes filtered out
   */
  lazy val dcReachabilityExcludingDownedObservers: Reachability = {
    val membersToExclude = members.collect { case m if m.status == Down || m.dataCenter != selfDc => m.uniqueAddress }
    overview.reachability
      .removeObservers(membersToExclude)
      .remove(members.collect { case m if m.dataCenter != selfDc => m.uniqueAddress })
  }

  lazy val dcReachabilityNoOutsideNodes: Reachability =
    overview.reachability.remove(members.collect { case m if m.dataCenter != selfDc => m.uniqueAddress })

  /**
   * @return Up to `crossDcConnections` oldest members for each DC
   */
  lazy val ageSortedTopOldestMembersPerDc: Map[DataCenter, immutable.SortedSet[Member]] = {
    latestGossip.members.foldLeft(Map.empty[DataCenter, immutable.SortedSet[Member]]) { (acc, member) =>
      acc.get(member.dataCenter) match {
        case Some(set) =>
          if (set.size < crossDcConnections) {
            acc + (member.dataCenter -> (set + member))
          } else {
            if (set.exists(member.isOlderThan)) {
              acc + (member.dataCenter -> (set + member).take(crossDcConnections))
            } else {
              acc
            }
          }
        case None =>
          acc + (member.dataCenter -> (immutable.SortedSet.empty(Member.ageOrdering) + member))
      }
    }
  }

  /**
   * @return true if toAddress should be reachable from the fromDc in general, within a data center
   *         this means only caring about data center local observations, across data centers it
   *         means caring about all observations for the toAddress.
   */
  def isReachableExcludingDownedObservers(toAddress: UniqueAddress): Boolean =
    if (!latestGossip.hasMember(toAddress)) false
    else {
      val to = latestGossip.member(toAddress)

      // if member is in the same data center, we ignore cross data center unreachability
      if (selfDc == to.dataCenter) dcReachabilityExcludingDownedObservers.isReachable(toAddress)
      // if not it is enough that any non-downed node observed it as unreachable
      else latestGossip.reachabilityExcludingDownedObservers.isReachable(toAddress)
    }

  def dcMembers: SortedSet[Member] =
    if (latestGossip.isMultiDc) members.filter(_.dataCenter == selfDc)
    else members

  def isLeader(node: UniqueAddress): Boolean =
    leader.contains(node)

  def leader: Option[UniqueAddress] =
    leaderOf(members)

  def roleLeader(role: String): Option[UniqueAddress] =
    leaderOf(members.filter(_.hasRole(role)))

  def leaderOf(mbrs: immutable.SortedSet[Member]): Option[UniqueAddress] = {
    val reachability = dcReachability

    val reachableMembersInDc =
      if (reachability.isAllReachable) mbrs.filter(m => m.dataCenter == selfDc && m.status != Down)
      else
        mbrs.filter(
          m =>
            m.dataCenter == selfDc &&
            m.status != Down &&
            (reachability.isReachable(m.uniqueAddress) || m.uniqueAddress == selfUniqueAddress))
    if (reachableMembersInDc.isEmpty) None
    else
      reachableMembersInDc
        .find(m => leaderMemberStatus(m.status))
        .orElse(Some(reachableMembersInDc.min(Member.leaderStatusOrdering)))
        .map(_.uniqueAddress)
  }

  def isInSameDc(node: UniqueAddress): Boolean =
    node == selfUniqueAddress || latestGossip.member(node).dataCenter == selfDc

  /**
   * Never gossip to self and not to node marked as unreachable by self (heartbeat
   * messages are not getting through so no point in trying to gossip).
   * Nodes marked as unreachable by others are still valid targets for gossip.
   */
  def validNodeForGossip(node: UniqueAddress): Boolean =
    node != selfUniqueAddress && overview.reachability.isReachable(selfUniqueAddress, node)

  def youngestMember: Member = {
    val mbrs = dcMembers
    require(mbrs.nonEmpty, "No youngest when no members")
    mbrs.maxBy(m => if (m.upNumber == Int.MaxValue) 0 else m.upNumber)
  }

  /**
   * The Leaving and Exiting change is gossiped to the two oldest nodes for quick dissemination to potential Singleton nodes
   */
  def gossipTargetsForLeavingAndExitingMembers(changedMembers: Set[Member]): Set[Member] = {
    if (changedMembers.nonEmpty) {
      val roles = changedMembers.flatten(_.roles).filterNot(_.startsWith(ClusterSettings.DcRolePrefix))
      val membersSortedByAge = latestGossip.members.toList.filter(_.dataCenter == selfDc).sorted(Member.ageOrdering)
      var targets = Set.empty[Member]
      if (membersSortedByAge.nonEmpty) {
        targets += membersSortedByAge.head // oldest of all nodes (in DC)
        if (membersSortedByAge.tail.nonEmpty)
          targets += membersSortedByAge.tail.head // second oldest of all nodes (in DC)
        roles.foreach { role =>
          membersSortedByAge.find(_.hasRole(role)).foreach { first =>
            targets += first // oldest with the role (in DC)
            membersSortedByAge.find(m => m != first && m.hasRole(role)).foreach { next =>
              targets += next // second oldest with the role (in DC)
            }
          }
        }
      }
      targets
    } else
      Set.empty
  }

}

/**
 * INTERNAL API
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[akka] class GossipTargetSelector(
    reduceGossipDifferentViewProbability: Double,
    crossDcGossipProbability: Double) {

  final def gossipTarget(state: MembershipState): Option[UniqueAddress] = {
    selectRandomNode(gossipTargets(state))
  }

  final def gossipTargets(state: MembershipState): Vector[UniqueAddress] =
    if (state.latestGossip.isMultiDc) multiDcGossipTargets(state)
    else localDcGossipTargets(state)

  /**
   * Select `n` random nodes to gossip to (used to quickly inform the rest of the cluster when leaving for example)
   */
  def randomNodesForFullGossip(state: MembershipState, n: Int): Vector[UniqueAddress] =
    if (state.latestGossip.isMultiDc && state.ageSortedTopOldestMembersPerDc(state.selfDc).contains(state.selfMember)) {
      // this node is one of the N oldest in the cluster, gossip to one cross-dc but mostly locally
      val randomLocalNodes = Random.shuffle(state.members.toVector.collect {
        case m if m.dataCenter == state.selfDc && state.validNodeForGossip(m.uniqueAddress) => m.uniqueAddress
      })

      @tailrec
      def selectOtherDcNode(randomizedDcs: List[DataCenter]): Option[UniqueAddress] =
        randomizedDcs match {
          case Nil => None // couldn't find a single cross-dc-node to talk to
          case dc :: tail =>
            state.ageSortedTopOldestMembersPerDc(dc).collectFirst {
              case m if state.validNodeForGossip(m.uniqueAddress) => m.uniqueAddress
            } match {
              case Some(addr) => Some(addr)
              case None       => selectOtherDcNode(tail)
            }

        }
      val otherDcs = Random.shuffle((state.ageSortedTopOldestMembersPerDc.keySet - state.selfDc).toList)

      selectOtherDcNode(otherDcs) match {
        case Some(node) => randomLocalNodes.take(n - 1) :+ node
        case None       => randomLocalNodes.take(n)
      }

    } else {
      // single dc or not among the N oldest - select local nodes
      val selectedNodes = state.members.toVector.collect {
        case m if m.dataCenter == state.selfDc && state.validNodeForGossip(m.uniqueAddress) => m.uniqueAddress
      }

      if (selectedNodes.size <= n) selectedNodes
      else Random.shuffle(selectedNodes).take(n)
    }

  /**
   * Chooses a set of possible gossip targets that is in the same dc. If the cluster is not multi dc this
   * means it is a choice among all nodes of the cluster.
   */
  protected def localDcGossipTargets(state: MembershipState): Vector[UniqueAddress] = {
    val latestGossip = state.latestGossip
    val firstSelection: Vector[UniqueAddress] =
      if (preferNodesWithDifferentView(state)) {
        // If it's time to try to gossip to some nodes with a different view
        // gossip to a random alive same dc member with preference to a member with older gossip version
        latestGossip.members.iterator
          .collect {
            case m
                if m.dataCenter == state.selfDc && !latestGossip.seenByNode(m.uniqueAddress) && state
                  .validNodeForGossip(m.uniqueAddress) =>
              m.uniqueAddress
          }
          .to(Vector)
      } else Vector.empty

    // Fall back to localGossip
    if (firstSelection.isEmpty) {
      latestGossip.members.toVector.collect {
        case m if m.dataCenter == state.selfDc && state.validNodeForGossip(m.uniqueAddress) => m.uniqueAddress
      }
    } else firstSelection

  }

  /**
   * Choose cross-dc nodes if this one of the N oldest nodes, and if not fall back to gossip locally in the dc
   */
  protected def multiDcGossipTargets(state: MembershipState): Vector[UniqueAddress] = {
    // only a fraction of the time across data centers
    if (selectDcLocalNodes(state))
      localDcGossipTargets(state)
    else {
      val nodesPerDc = state.ageSortedTopOldestMembersPerDc

      // only do cross DC gossip if this node is among the N oldest

      if (!nodesPerDc(state.selfDc).contains(state.selfMember)) localDcGossipTargets(state)
      else {
        @tailrec
        def findFirstDcWithValidNodes(left: List[DataCenter]): Vector[UniqueAddress] =
          left match {
            case dc :: tail =>
              val validNodes = nodesPerDc(dc).collect {
                case member if state.validNodeForGossip(member.uniqueAddress) =>
                  member.uniqueAddress
              }

              if (validNodes.nonEmpty) validNodes.toVector
              else findFirstDcWithValidNodes(tail) // no valid nodes in dc, try next

            case Nil =>
              Vector.empty
          }

        // chose another DC at random
        val otherDcsInRandomOrder = dcsInRandomOrder((nodesPerDc - state.selfDc).keys.toList)
        val nodes = findFirstDcWithValidNodes(otherDcsInRandomOrder)
        if (nodes.nonEmpty) nodes
        // no other dc with reachable nodes, fall back to local gossip
        else localDcGossipTargets(state)
      }
    }
  }

  /**
   * For large clusters we should avoid shooting down individual
   * nodes. Therefore the probability is reduced for large clusters.
   */
  protected def adjustedGossipDifferentViewProbability(clusterSize: Int): Double = {
    val low = reduceGossipDifferentViewProbability
    val high = low * 3
    // start reduction when cluster is larger than configured ReduceGossipDifferentViewProbability
    if (clusterSize <= low)
      reduceGossipDifferentViewProbability
    else {
      // don't go lower than 1/10 of the configured GossipDifferentViewProbability
      val minP = reduceGossipDifferentViewProbability / 10
      if (clusterSize >= high)
        minP
      else {
        // linear reduction of the probability with increasing number of nodes
        // from ReduceGossipDifferentViewProbability at ReduceGossipDifferentViewProbability nodes
        // to ReduceGossipDifferentViewProbability / 10 at ReduceGossipDifferentViewProbability * 3 nodes
        // i.e. default from 0.8 at 400 nodes, to 0.08 at 1600 nodes
        val k = (minP - reduceGossipDifferentViewProbability) / (high - low)
        reduceGossipDifferentViewProbability + (clusterSize - low) * k
      }
    }
  }

  /**
   * For small DCs prefer cross DC gossip. This speeds up the bootstrapping of
   * new DCs as adding an initial node means it has no local peers.
   * Once the DC is at 5 members use the configured crossDcGossipProbability, before
   * that for a single node cluster use 1.0, two nodes use 0.75 etc
   */
  protected def selectDcLocalNodes(state: MembershipState): Boolean = {
    val localMembers = state.dcMembers.size
    val probability =
      if (localMembers > 4)
        crossDcGossipProbability
      else {
        // don't go below the configured probability
        math.max((5 - localMembers) * 0.25, crossDcGossipProbability)
      }
    ThreadLocalRandom.current.nextDouble() > probability
  }

  protected def preferNodesWithDifferentView(state: MembershipState): Boolean =
    ThreadLocalRandom.current.nextDouble() < adjustedGossipDifferentViewProbability(state.latestGossip.members.size)

  protected def dcsInRandomOrder(dcs: List[DataCenter]): List[DataCenter] =
    Random.shuffle(dcs)

  protected def selectRandomNode(nodes: IndexedSeq[UniqueAddress]): Option[UniqueAddress] =
    if (nodes.isEmpty) None
    else Some(nodes(ThreadLocalRandom.current.nextInt(nodes.size)))
}
