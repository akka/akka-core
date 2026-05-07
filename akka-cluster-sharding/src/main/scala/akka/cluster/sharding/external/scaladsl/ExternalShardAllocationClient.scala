/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.external.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.actor.Address
import akka.annotation.DoNotInherit
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.external.ShardLocations

/**
 * Not for user extension
 */
@DoNotInherit
trait ExternalShardAllocationClient {

  /**
   * Update the given shard's location. The [[Address]] should
   * match one of the nodes in the cluster. If the node has not joined
   * the cluster yet it will be moved to that node after the first cluster
   * sharding rebalance it does.
   *
   * @param shard The shard identifier
   * @param location Location (akka node) to allocate the shard to
   * @return Confirmation that the update has been propagated to a majority of cluster nodes
   */
  def updateShardLocation(shard: ShardId, location: Address): Future[Done]

  /**
   * Update all of the provided ShardLocations.
   * The [[Address]] should match one of the nodes in the cluster. If the node has not joined
   * the cluster yet it will be moved to that node after the first cluster
   * sharding rebalance it does.
   *
   * @param locations to update
   * @return Confirmation that the update has been propagates to a majority of cluster nodes
   */
  def updateShardLocations(locations: Map[ShardId, Address]): Future[Done]

  /**
   * Get all the current shard locations that have been set via updateShardLocation
   */
  def shardLocations(): Future[ShardLocations]
}
