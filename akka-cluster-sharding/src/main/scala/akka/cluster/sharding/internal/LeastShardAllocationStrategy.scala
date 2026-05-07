/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.collection.immutable
import scala.concurrent.Future

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.RegionEntry
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.ShardSuitabilityOrdering

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LeastShardAllocationStrategy {
  private val emptyRebalanceResult = Future.successful(Set.empty[ShardId])
}

/**
 * INTERNAL API: Use `ShardCoordinator.ShardAllocationStrategy.leastShardAllocationStrategy` factory method.
 *
 * `ShardAllocationStrategy` that  allocates new shards to the `ShardRegion` (node) with least
 * number of previously allocated shards.
 *
 * When a node is added to the cluster the shards on the existing nodes will be rebalanced to the new node.
 * The `LeastShardAllocationStrategy` picks shards for rebalancing from the `ShardRegion`s with most number
 * of previously allocated shards. They will then be allocated to the `ShardRegion` with least number of
 * previously allocated shards, i.e. new members in the cluster. The amount of shards to rebalance in each
 * round can be limited to make it progress slower since rebalancing too many shards at the same time could
 * result in additional load on the system. For example, causing many Event Sourced entites to be started
 * at the same time.
 *
 * It will not rebalance when there is already an ongoing rebalance in progress.
 *
 * @param absoluteLimit the maximum number of shards that will be rebalanced in one rebalance round
 * @param relativeLimit fraction (< 1.0) of total number of (known) shards that will be rebalanced
 *                      in one rebalance round
 */
@InternalApi private[akka] class LeastShardAllocationStrategy(absoluteLimit: Int, relativeLimit: Double)
    extends AbstractLeastShardAllocationStrategy {
  import LeastShardAllocationStrategy.emptyRebalanceResult

  override def rebalance(
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {
    import math.max
    import math.min

    def limit(numberOfShards: Int): Int =
      max(1, min((relativeLimit * numberOfShards).toInt, absoluteLimit))

    def rebalancePhase1(
        numberOfShards: Int,
        optimalPerRegion: Int,
        sortedEntries: Iterable[RegionEntry]): Set[ShardId] = {
      val selected = Vector.newBuilder[ShardId]
      sortedEntries.foreach {
        case RegionEntry(_, _, shardIds) =>
          if (shardIds.size > optimalPerRegion) {
            selected ++= shardIds.take(shardIds.size - optimalPerRegion)
          }
      }
      val result = selected.result()
      result.take(limit(numberOfShards)).toSet
    }

    def rebalancePhase2(
        numberOfShards: Int,
        optimalPerRegion: Int,
        sortedEntries: Iterable[RegionEntry]): Future[Set[ShardId]] = {
      // In the first phase the optimalPerRegion is rounded up, and depending on number of shards per region and number
      // of regions that might not be the exact optimal.
      // In second phase we look for diff of >= 2 below optimalPerRegion and rebalance that number of shards.
      val countBelowOptimal =
        sortedEntries.iterator.map(entry => max(0, (optimalPerRegion - 1) - entry.shardIds.size)).sum
      if (countBelowOptimal == 0) {
        emptyRebalanceResult
      } else {
        val selected = Vector.newBuilder[ShardId]
        sortedEntries.foreach {
          case RegionEntry(_, _, shardIds) =>
            if (shardIds.size >= optimalPerRegion) {
              selected += shardIds.head
            }
        }
        val result = selected.result().take(min(countBelowOptimal, limit(numberOfShards))).toSet
        Future.successful(result)
      }
    }

    if (rebalanceInProgress.nonEmpty) {
      // one rebalance at a time
      emptyRebalanceResult
    } else {
      val sortedRegionEntries = regionEntriesFor(currentShardAllocations).toVector.sorted(ShardSuitabilityOrdering)
      if (!isAGoodTimeToRebalance(sortedRegionEntries)) {
        emptyRebalanceResult
      } else {
        val numberOfShards = sortedRegionEntries.map(_.shardIds.size).sum
        val numberOfRegions = sortedRegionEntries.size
        if (numberOfRegions == 0 || numberOfShards == 0) {
          emptyRebalanceResult
        } else {
          val optimalPerRegion = numberOfShards / numberOfRegions + (if (numberOfShards % numberOfRegions == 0) 0
                                                                     else 1)

          val result1 = rebalancePhase1(numberOfShards, optimalPerRegion, sortedRegionEntries)

          if (result1.nonEmpty) {
            Future.successful(result1)
          } else {
            rebalancePhase2(numberOfShards, optimalPerRegion, sortedRegionEntries)
          }
        }
      }
    }
  }

  override def toString: ShardId =
    s"LeastShardAllocationStrategy($absoluteLimit,$relativeLimit)"
}
