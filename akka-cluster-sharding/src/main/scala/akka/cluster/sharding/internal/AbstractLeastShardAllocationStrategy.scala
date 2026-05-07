/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.cluster.sharding.ShardCoordinator.ActorSystemDependentAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId
import akka.pattern.after

/**
 * INTERNAL API: Common logic for the least shard allocation strategy implementations
 */
@InternalApi
private[akka] abstract class AbstractLeastShardAllocationStrategy
    extends ActorSystemDependentAllocationStrategy
    with ClusterShardAllocationMixin {
  import ClusterShardAllocationMixin._

  @volatile private var system: ActorSystem = _
  @volatile private var cluster: Cluster = _

  override def start(system: ActorSystem): Unit = {
    this.system = system
    cluster = Cluster(system)
  }

  override protected def clusterState: CurrentClusterState = cluster.state
  override protected def selfMember: Member = cluster.selfMember

  override def allocateShard(
      requester: ActorRef,
      shardId: ShardId,
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {
    val regionEntries = regionEntriesFor(currentShardAllocations)
    if (regionEntries.isEmpty) {
      // very unlikely to ever happen but possible because of cluster state view not yet updated when collecting
      // region entries, view should be updated after a very short time
      after(50.millis)(allocateShard(requester, shardId, currentShardAllocations))(system)
    } else {
      val (region, _) = mostSuitableRegion(regionEntries)
      Future.successful(region)
    }
  }

  final protected def mostSuitableRegion(
      regionEntries: Iterable[RegionEntry]): (ActorRef, immutable.IndexedSeq[ShardId]) = {
    val mostSuitableEntry = regionEntries.min(ShardSuitabilityOrdering)
    mostSuitableEntry.region -> mostSuitableEntry.shardIds
  }

}
