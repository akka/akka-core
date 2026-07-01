/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ShardRegion.EntityId
import akka.cluster.sharding.ShardRegion.ShardId

/**
 * INTERNAL API
 *
 * Created once for the shard guardian
 */
@InternalApi
private[akka] trait RememberEntitiesProvider {

  /**
   * Called once per started shard coordinator to create the remember entities coordinator store.
   *
   * Note that this is not used for the deprecated persistent coordinator which has its own impl for keeping track of
   * remembered shards.
   *
   * @return an actor that handles the protocol defined in [[RememberEntitiesCoordinatorStore]]
   */
  def coordinatorStoreProps(): Props

  /**
   * Called once per started shard to create the remember entities shard store
   *
   * The store is created as a child of the shard and the shard death-watches it. On any failure the store must
   * stop itself rather than stay alive without responding, see [[RememberEntitiesShardStore]] for the contract.
   *
   * @return an actor that handles the protocol defined in [[RememberEntitiesShardStore]]
   */
  def shardStoreProps(shardId: ShardId): Props
}

/**
 * INTERNAL API
 *
 * Could potentially become an open SPI in the future.
 *
 * Protocol contract for implementations (the store is a local child actor of the shard, which death-watches it):
 *  - In response to [[GetEntities]] the store must eventually reply with [[RememberedEntities]]. The shard does
 *    not put a timeout on this, because the load can legitimately be slow (e.g. when many shards recover their
 *    stores concurrently during a rolling restart). Therefore, if the store cannot load its state (e.g. failed
 *    recovery) it must stop itself; the shard detects the termination via death watch and is restarted after a
 *    backoff. A store that stays alive without replying would leave the shard stuck.
 *  - In response to [[Update]] the store must eventually reply with [[UpdateDone]] (the shard does bound this
 *    with `updating-state-timeout`) or stop itself on failure.
 */
@InternalApi
private[akka] object RememberEntitiesShardStore {
  // SPI protocol for a remember entities shard store
  sealed trait Command

  // Note: the store is not expected to receive and handle new update before it has responded to the previous one
  final case class Update(started: Set[EntityId], stopped: Set[EntityId]) extends Command
  // responses for Update
  final case class UpdateDone(started: Set[EntityId], stopped: Set[EntityId])

  // The store must reply with RememberedEntities, or stop itself if it cannot load the state (e.g. failed
  // recovery). The shard does not time out this request; it relies on the death watch of the store instead.
  case object GetEntities extends Command
  final case class RememberedEntities(entities: Set[EntityId])

}

/**
 * INTERNAL API
 *
 * Could potentially become an open SPI in the future.
 */
@InternalApi
private[akka] object RememberEntitiesCoordinatorStore {
  // SPI protocol for a remember entities coordinator store
  sealed trait Command

  /**
   * Sent once for every started shard (but could be retried), should result in a response of either
   * UpdateDone or UpdateFailed
   */
  final case class AddShard(entityId: ShardId) extends Command
  final case class UpdateDone(entityId: ShardId)
  final case class UpdateFailed(entityId: ShardId)

  /**
   * Sent once when the coordinator starts (but could be retried), should result in a response of
   * RememberedShards
   */
  case object GetShards extends Command
  final case class RememberedShards(entities: Set[ShardId])
  // No message for failed load since we eager lod the set of shards, may need to change in the future
}
