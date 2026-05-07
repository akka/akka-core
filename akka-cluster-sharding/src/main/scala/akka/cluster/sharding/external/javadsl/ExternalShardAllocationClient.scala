/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.external.javadsl

import java.util.concurrent.CompletionStage

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
   * sharding rebalance.
   *
   * @param shard    The shard identifier
   * @param location Location (akka node) to allocate the shard to
   * @return Conformation that the update has been written to the local node
   */
  def setShardLocation(shard: ShardId, location: Address): CompletionStage[Done]

  /**
   * Update all of the provided ShardLocations.
   * The [[Address]] should match one of the nodes in the cluster. If the node has not joined
   * the cluster yet it will be moved to that node after the first cluster
   * sharding rebalance it does.
   *
   * @param locations to update
   * @return Confirmation that the update has been written to the local node
   */
  def setShardLocations(locations: java.util.Map[ShardId, Address]): CompletionStage[Done]

  /**
   * Get all the current shard locations that have been set via setShardLocation
   */
  def getShardLocations(): CompletionStage[ShardLocations]
}
