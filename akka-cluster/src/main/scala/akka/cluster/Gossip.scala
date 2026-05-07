/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration.Deadline

import ClusterSettings.DataCenter
import MemberStatus._
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
private[cluster] object Gossip {
  type Timestamp = Long
  val emptyMembers: immutable.SortedSet[Member] = immutable.SortedSet.empty
  val empty: Gossip = new Gossip(Gossip.emptyMembers)

  def apply(members: immutable.SortedSet[Member]): Gossip =
    if (members.isEmpty) empty else empty.copy(members = members)

  def vclockName(node: UniqueAddress): String = s"${node.address}-${node.longUid}"

}

/**
 * INTERNAL API
 *
 * Represents the state of the cluster; cluster ring membership, ring convergence -
 * all versioned by a vector clock.
 *
 * When a node is joining the `Member`, with status `Joining`, is added to `members`.
 * If the joining node was downed it is moved from `overview.unreachable` (status `Down`)
 * to `members` (status `Joining`). It cannot rejoin if not first downed.
 *
 * When convergence is reached the leader change status of `members` from `Joining`
 * to `Up`.
 *
 * When failure detector consider a node as unavailable it will be moved from
 * `members` to `overview.unreachable`.
 *
 * When a node is downed, either manually or automatically, its status is changed to `Down`.
 * It is also removed from `overview.seen` table. The node will reside as `Down` in the
 * `overview.unreachable` set until joining again and it will then go through the normal
 * joining procedure.
 *
 * When a `Gossip` is received the version (vector clock) is used to determine if the
 * received `Gossip` is newer or older than the current local `Gossip`. The received `Gossip`
 * and local `Gossip` is merged in case of conflicting version, i.e. vector clocks without
 * same history.
 *
 * When a node is told by the user to leave the cluster the leader will move it to `Leaving`
 * and then rebalance and repartition the cluster and start hand-off by migrating the actors
 * from the leaving node to the new partitions. Once this process is complete the leader will
 * move the node to the `Exiting` state and once a convergence is complete move the node to
 * `Removed` by removing it from the `members` set and sending a `Removed` command to the
 * removed node telling it to shut itself down.
 */
@SerialVersionUID(1L)
@InternalApi
@nowarn("msg=Use Akka Distributed Cluster")
private[cluster] final case class Gossip(
    members: immutable.SortedSet[Member], // sorted set of members with their status, sorted by address
    overview: GossipOverview = GossipOverview(),
    version: VectorClock = VectorClock(), // vector clock version
    tombstones: Map[UniqueAddress, Gossip.Timestamp] = Map.empty) {

  if (Cluster.isAssertInvariantsEnabled) assertInvariants()

  private def assertInvariants(): Unit = {

    def ifTrueThrow(func: => Boolean, expected: String, actual: String): Unit =
      if (func) throw new IllegalArgumentException(s"$expected, but found [$actual]")

    ifTrueThrow(
      members.exists(_.status == Removed),
      expected = s"Live members must not have status [$Removed]",
      actual = s"${members.filter(_.status == Removed)}")

    val inReachabilityButNotMember = overview.reachability.allObservers.diff(members.map(_.uniqueAddress))
    ifTrueThrow(
      inReachabilityButNotMember.nonEmpty,
      expected = "Nodes not part of cluster in reachability table",
      actual = inReachabilityButNotMember.mkString(", "))

    val inReachabilityVersionsButNotMember = overview.reachability.versions.keySet.diff(members.map(_.uniqueAddress))
    ifTrueThrow(
      inReachabilityVersionsButNotMember.nonEmpty,
      expected = "Nodes not part of cluster in reachability versions table",
      actual = inReachabilityVersionsButNotMember.mkString(", "))

    val seenButNotMember = overview.seen.diff(members.map(_.uniqueAddress))
    ifTrueThrow(
      seenButNotMember.nonEmpty,
      expected = "Nodes not part of cluster have marked the Gossip as seen",
      actual = seenButNotMember.mkString(", "))
  }

  @transient private lazy val membersMap: Map[UniqueAddress, Member] =
    members.iterator.map(m => m.uniqueAddress -> m).toMap

  @transient lazy val isMultiDc =
    if (members.size <= 1) false
    else {
      val dc1 = members.head.dataCenter
      members.exists(_.dataCenter != dc1)
    }

  /**
   * Increments the version for this 'Node'.
   */
  def :+(node: VectorClock.Node): Gossip = copy(version = version :+ node)

  /**
   * Adds a member to the member node ring.
   */
  def :+(member: Member): Gossip = {
    if (members contains member) this
    else this.copy(members = members + member)
  }

  /**
   * Marks the gossip as seen by this node (address) by updating the address entry in the 'gossip.overview.seen'
   */
  def seen(node: UniqueAddress): Gossip = {
    if (seenByNode(node)) this
    else this.copy(overview = overview.copy(seen = overview.seen + node))
  }

  /**
   * Marks the gossip as seen by only this node (address) by replacing the 'gossip.overview.seen'
   */
  def onlySeen(node: UniqueAddress): Gossip = {
    this.copy(overview = overview.copy(seen = Set(node)))
  }

  /**
   * Remove all seen entries
   */
  def clearSeen(): Gossip = {
    this.copy(overview = overview.copy(seen = Set.empty))
  }

  /**
   * The nodes that have seen the current version of the Gossip.
   */
  def seenBy: Set[UniqueAddress] = overview.seen

  /**
   * Has this Gossip been seen by this node.
   */
  def seenByNode(node: UniqueAddress): Boolean = overview.seen(node)

  /**
   * Merges the seen table of two Gossip instances.
   */
  def mergeSeen(that: Gossip): Gossip =
    this.copy(overview = overview.copy(seen = overview.seen.union(that.overview.seen)))

  /**
   * Merges two Gossip instances including membership tables, tombstones, and the VectorClock histories.
   */
  def merge(that: Gossip): Gossip = {

    // 1. merge sets of tombstones
    val mergedTombstones = tombstones ++ that.tombstones

    // 2. merge vector clocks (but remove entries for tombstoned nodes)
    val mergedVClock = mergedTombstones.keys.foldLeft(this.version.merge(that.version)) { (vclock, node) =>
      vclock.prune(VectorClock.Node(Gossip.vclockName(node)))
    }

    // 3. merge members by selecting the single Member with highest MemberStatus out of the Member groups
    val mergedMembers =
      Gossip.emptyMembers.union(Member.pickHighestPriority(this.members, that.members, mergedTombstones))

    // 4. merge reachability table by picking records with highest version
    val mergedReachability =
      this.overview.reachability.merge(mergedMembers.map(_.uniqueAddress), that.overview.reachability)

    // 5. Nobody can have seen this new gossip yet
    val mergedSeen = Set.empty[UniqueAddress]

    Gossip(mergedMembers, GossipOverview(mergedSeen, mergedReachability), mergedVClock, mergedTombstones)
  }

  lazy val reachabilityExcludingDownedObservers: Reachability = {
    val downed = members.collect { case m if m.status == Down => m }
    overview.reachability.removeObservers(downed.map(_.uniqueAddress))
  }

  def allDataCenters: Set[DataCenter] = members.map(_.dataCenter)

  def allRoles: Set[String] = members.flatMap(_.roles)

  def isSingletonCluster: Boolean = members.size == 1

  /**
   * @return true if fromAddress should be able to reach toAddress based on the unreachability data and their
   *         respective data centers
   */
  def isReachable(fromAddress: UniqueAddress, toAddress: UniqueAddress): Boolean =
    if (!hasMember(toAddress)) false
    else {
      // as it looks for specific unreachable entires for the node pair we don't have to filter on data center
      overview.reachability.isReachable(fromAddress, toAddress)
    }

  def member(node: UniqueAddress): Member = {
    membersMap.getOrElse(node, Member.removed(node)) // placeholder for removed member
  }

  def hasMember(node: UniqueAddress): Boolean = membersMap.contains(node)

  def removeAll(nodes: Iterable[UniqueAddress], removalTimestamp: Long): Gossip = {
    nodes.foldLeft(this)((gossip, node) => gossip.remove(node, removalTimestamp))
  }

  def update(updatedMembers: immutable.SortedSet[Member]): Gossip = {
    copy(members = updatedMembers.union(members.diff(updatedMembers)))
  }

  /**
   * Remove the given member from the set of members and mark it's removal with a tombstone to avoid having it
   * reintroduced when merging with another gossip that has not seen the removal.
   */
  def remove(node: UniqueAddress, removalTimestamp: Long): Gossip = {
    // removing REMOVED nodes from the `seen` table
    val newSeen = overview.seen - node
    // removing REMOVED nodes from the `reachability` table
    val newReachability = overview.reachability.remove(node :: Nil)
    val newOverview = overview.copy(seen = newSeen, reachability = newReachability)

    // Clear the VectorClock when member is removed. The change made by the leader is stamped
    // and will propagate as is if there are no other changes on other nodes.
    // If other concurrent changes on other nodes (e.g. join) the pruning is also
    // taken care of when receiving gossips.
    val newVersion = version.prune(VectorClock.Node(Gossip.vclockName(node)))
    val newMembers = members.filterNot(_.uniqueAddress == node)
    val newTombstones = tombstones + (node -> removalTimestamp)
    copy(version = newVersion, members = newMembers, overview = newOverview, tombstones = newTombstones)
  }

  def markAsDown(member: Member): Gossip = {
    // replace member (changed status)
    val newMembers = members - member + member.copy(status = Down)
    // remove nodes marked as DOWN from the `seen` table
    val newSeen = overview.seen - member.uniqueAddress

    // update gossip overview
    val newOverview = overview.copy(seen = newSeen)
    copy(members = newMembers, overview = newOverview) // update gossip
  }

  def prune(removedNode: VectorClock.Node): Gossip = {
    val newVersion = version.prune(removedNode)
    if (newVersion eq version) this
    else copy(version = newVersion)
  }

  def pruneTombstones(removeEarlierThan: Gossip.Timestamp): Gossip = {
    val newTombstones = tombstones.filter { case (_, timestamp) => timestamp > removeEarlierThan }
    if (newTombstones.size == tombstones.size) this
    else copy(tombstones = newTombstones)
  }

  def seenDigest: Array[Byte] =
    overview.seenDigest

  override def toString =
    s"Gossip(members = [${members.mkString(", ")}], overview = $overview, version = $version, tombstones = $tombstones)"
}

/**
 * INTERNAL API
 * Represents the overview of the cluster, holds the cluster convergence table and set with unreachable nodes.
 */
@SerialVersionUID(1L)
private[cluster] final case class GossipOverview(
    seen: Set[UniqueAddress] = Set.empty,
    reachability: Reachability = Reachability.empty) {

  lazy val seenDigest: Array[Byte] = {
    val bytes = seen.toVector.sorted.map(node => node.address).mkString(",").getBytes(StandardCharsets.UTF_8)
    MessageDigest.getInstance("SHA-1").digest(bytes)
  }

  override def toString =
    s"GossipOverview(reachability = [$reachability], seen = [${seen.mkString(", ")}])"
}

object GossipEnvelope {
  def apply(from: UniqueAddress, to: UniqueAddress, gossip: Gossip): GossipEnvelope =
    new GossipEnvelope(from, to, gossip, null, null)

  def apply(from: UniqueAddress, to: UniqueAddress, serDeadline: Deadline, ser: () => Gossip): GossipEnvelope =
    new GossipEnvelope(from, to, null, serDeadline, ser)
}

/**
 * INTERNAL API
 * Envelope adding a sender and receiver address to the gossip.
 * The reason for including the receiver address is to be able to
 * ignore messages that were intended for a previous incarnation of
 * the node with same host:port. The `uid` in the `UniqueAddress` is
 * different in that case.
 */
@SerialVersionUID(2L)
private[cluster] class GossipEnvelope private (
    val from: UniqueAddress,
    val to: UniqueAddress,
    @volatile var g: Gossip,
    serDeadline: Deadline,
    @transient @volatile var ser: () => Gossip)
    extends ClusterMessage {

  def gossip: Gossip = {
    deserialize()
    g
  }

  private def deserialize(): Unit = {
    if ((g eq null) && (ser ne null)) {
      if (serDeadline.hasTimeLeft())
        g = ser()
      else
        g = Gossip.empty
      ser = null
    }
  }
}

/**
 * INTERNAL API
 * When there are no known changes to the node ring a `GossipStatus`
 * initiates a gossip chat between two members. If the receiver has a newer
 * version it replies with a `GossipEnvelope`. If receiver has older version
 * it replies with its `GossipStatus`. Same versions ends the chat immediately.
 */
@SerialVersionUID(1L)
private[cluster] final case class GossipStatus(from: UniqueAddress, version: VectorClock, seenDigest: Array[Byte])
    extends ClusterMessage {
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: GossipStatus =>
        from == other.from && version == other.version && java.util.Arrays.equals(seenDigest, other.seenDigest)
      case _ => false
    }
  }

  override def toString: String =
    f"GossipStatus($from,$version,${seenDigest.map(byte => f"$byte%02x").mkString("")})"
}
