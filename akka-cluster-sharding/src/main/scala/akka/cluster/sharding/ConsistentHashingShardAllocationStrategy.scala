/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.immutable
import scala.concurrent.Future

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.annotation.DoNotInherit
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.cluster.sharding.ShardCoordinator.ActorSystemDependentAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.ClusterShardAllocationMixin
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.ShardSuitabilityOrdering
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.routing.ConsistentHash

object ConsistentHashingShardAllocationStrategy {
  private val emptyRebalanceResult = Future.successful(Set.empty[ShardId])
}

/**
 * [[akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy]] that is using consistent
 * hashing. This can be useful when shards with the same shard id for different entity types
 * should be best effort colocated to the same nodes.
 *
 * When adding or removing nodes it will rebalance according to the new consistent hashing,
 * but that means that only a few shards will be rebalanced and others remain on the same
 * location.
 *
 * A good explanation of Consistent Hashing:
 * https://tom-e-white.com/2007/11/consistent-hashing.html
 *
 * Create a new instance of this for each entity types, i.e. a `ConsistentHashingShardAllocationStrategy`
 * instance must not be shared between different entity types.
 *
 * Not intended for public inheritance/implementation
 */
@DoNotInherit
class ConsistentHashingShardAllocationStrategy(rebalanceLimit: Int)
    extends ActorSystemDependentAllocationStrategy
    with ClusterShardAllocationMixin {
  import ConsistentHashingShardAllocationStrategy.emptyRebalanceResult

  private var cluster: Cluster = _
  private var _log: LoggingAdapter = _

  private val virtualNodesFactor = 10
  private var hashedByNodes: Vector[Address] = Vector.empty
  private var consistentHashing: ConsistentHash[Address] = ConsistentHash(Nil, virtualNodesFactor)

  override def start(system: ActorSystem): Unit = {
    cluster = Cluster(system)
    _log = Logging(system, classOf[ConsistentHashingShardAllocationStrategy])
  }

  // can be overridden for tests without real Cluster, i.e. without `start(system)`
  protected def log: LoggingAdapter =
    _log

  override protected def clusterState: CurrentClusterState = cluster.state
  override protected def selfMember: Member = cluster.selfMember

  override def allocateShard(
      requester: ActorRef,
      shardId: ShardId,
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {
    val nodes = nodesForRegions(currentShardAllocations)
    updateHashing(nodes)
    val node = consistentHashing.nodeFor(shardId)
    currentShardAllocations.keysIterator.find(region => nodeForRegion(region) == node) match {
      case Some(region) => Future.successful(region)
      case None =>
        throw new IllegalStateException(s"currentShardAllocations should include region for node [$node]")
    }
  }

  override def rebalance(
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {

    val sortedRegionEntries = regionEntriesFor(currentShardAllocations).toVector.sorted(ShardSuitabilityOrdering)
    if (!isAGoodTimeToRebalance(sortedRegionEntries)) {
      emptyRebalanceResult
    } else {

      val nodes = nodesForRegions(currentShardAllocations)
      updateHashing(nodes)

      val regionByNode = currentShardAllocations.keysIterator.map(region => nodeForRegion(region) -> region).toMap

      var result = Set.empty[String]

      def lessThanLimit: Boolean =
        rebalanceLimit <= 0 || result.size < rebalanceLimit

      currentShardAllocations
      // deterministic order, at least easier to test
      .toVector.sortBy { case (region, _) => nodeForRegion(region) }(Address.addressOrdering).foreach {
        case (currentRegion, shardIds) =>
          shardIds.foreach { shardId =>
            if (lessThanLimit && !rebalanceInProgress.contains(shardId)) {
              val node = consistentHashing.nodeFor(shardId)
              regionByNode.get(node) match {
                case Some(region) =>
                  if (region != currentRegion) {
                    log.debug(
                      "Rebalance needed for shard [{}], from [{}] to [{}]",
                      shardId,
                      nodeForRegion(currentRegion),
                      node)
                    result += shardId
                  }
                case None =>
                  throw new IllegalStateException(s"currentShardAllocations should include region for node [$node]")
              }
            }
          }
      }

      Future.successful(result)
    }
  }

  private def nodesForRegions(
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Vector[Address] = {
    currentShardAllocations.keysIterator.map(nodeForRegion).toVector
  }

  private def nodeForRegion(region: ActorRef): Address =
    if (region.path.address.hasLocalScope) selfMember.address
    else region.path.address

  private def updateHashing(nodes: Vector[Address]): Unit = {
    val sortedNodes = nodes.sorted
    if (sortedNodes != hashedByNodes) {
      if (log.isDebugEnabled)
        log.debug("Update consistent hashing nodes [{}]", sortedNodes.mkString(", "))
      hashedByNodes = sortedNodes
      consistentHashing = ConsistentHash(sortedNodes, virtualNodesFactor)
    }
  }

}
