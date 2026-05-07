/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import java.lang.{ Boolean => JBoolean }
import java.lang.{ Integer => JInteger }

import scala.annotation.nowarn
import scala.collection.immutable

import akka.actor.ActorRef
import akka.actor.Address
import akka.annotation.InternalApi
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.JoiningCluster
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.RegionEntry

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ClusterShardAllocationMixin {

  type AllocationMap = Map[ActorRef, immutable.IndexedSeq[ShardId]]

  import MemberStatus._

  val JoiningCluster: Set[MemberStatus] = Set(Joining, WeaklyUp)
  val LeavingClusterStatuses: Set[MemberStatus] = Set(Leaving, Exiting, Down)

  final case class RegionEntry(region: ActorRef, member: Member, shardIds: immutable.IndexedSeq[ShardId])

  implicit object ShardSuitabilityOrdering extends ShardSuitabilityOrdering(preferLeastShards = true)

  class ShardSuitabilityOrdering(preferLeastShards: Boolean) extends Ordering[RegionEntry] {
    override def compare(x: RegionEntry, y: RegionEntry): Int = {
      val RegionEntry(_, memberX, allocatedShardsX) = x
      val RegionEntry(_, memberY, allocatedShardsY) = y
      if (memberX.status != memberY.status) {
        // prefer allocating to nodes that are not on their way out of the cluster
        val xIsLeaving = LeavingClusterStatuses(memberX.status)
        val yIsLeaving = LeavingClusterStatuses(memberY.status)
        JBoolean.compare(xIsLeaving, yIsLeaving)
      } else if (memberX.appVersion != memberY.appVersion) {
        // prefer nodes with the highest rolling update app version
        memberY.appVersion.compareTo(memberX.appVersion)
      } else if (preferLeastShards && allocatedShardsX.size != allocatedShardsY.size) {
        // prefer the node with the least allocated shards
        JInteger.compare(allocatedShardsX.size, allocatedShardsY.size)
      } else if (x.member.upNumber != y.member.upNumber) {
        // prefer older
        Member.ageOrdering.compare(x.member, y.member)
      } else {
        Member.addressOrdering.compare(x.member.address, y.member.address)
      }
    }

  }
}

/**
 * INTERNAL API
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[akka] trait ClusterShardAllocationMixin {
  import ClusterShardAllocationMixin.AllocationMap

  protected def clusterState: CurrentClusterState

  protected def selfMember: Member

  final protected def isAGoodTimeToRebalance(regionEntries: Iterable[RegionEntry]): Boolean = {
    // Avoid rebalance when rolling update is in progress
    // (This will ignore versions on members with no shard regions, because of sharding role or not yet completed joining)
    regionEntries.headOption match {
      case None => false // empty list of regions, probably not a good time to rebalance...
      case Some(firstRegion) =>
        def allNodesSameVersion =
          regionEntries.forall(_.member.appVersion == firstRegion.member.appVersion)

        // Rebalance requires ack from regions and proxies - no need to rebalance if it cannot be completed
        // FIXME #29589, we currently only look at same dc but proxies in other dcs may delay complete as well right now
        def neededMembersReachable =
          !clusterState.members.exists(m => m.dataCenter == selfMember.dataCenter && clusterState.unreachable(m))

        // No members in same dc joining, we want that to complete before rebalance, such nodes should reach Up soon
        def membersInProgressOfJoining =
          clusterState.members.exists(m => m.dataCenter == selfMember.dataCenter && JoiningCluster(m.status))

        allNodesSameVersion && neededMembersReachable && !membersInProgressOfJoining
    }
  }

  final protected def regionEntriesFor(currentShardAllocations: AllocationMap): Iterable[RegionEntry] = {
    val addressToMember: Map[Address, Member] = clusterState.members.iterator.map(m => m.address -> m).toMap
    currentShardAllocations.flatMap {
      case (region, shardIds) =>
        val regionAddress = {
          if (region.path.address.hasLocalScope) selfMember.address
          else region.path.address
        }

        val memberForRegion = addressToMember.get(regionAddress)
        // if the member is unknown (very unlikely but not impossible) because of view not updated yet
        // that node is ignored for this invocation
        memberForRegion.map(member => RegionEntry(region, member, shardIds))
    }
  }
}
