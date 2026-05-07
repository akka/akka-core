/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.actor.Address
import akka.annotation.InternalApi
import akka.event.LogMarker

/**
 * This is public with the purpose to document the used markers and properties of log events.
 * No guarantee that it will remain binary compatible, but the marker names and properties
 * are considered public API and will not be changed without notice.
 */
object ShardingLogMarker {

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] object Properties {
    val ShardTypeName = "akkaShardTypeName"
    val ShardId = "akkaShardId"
  }

  /**
   * Marker "akkaShardAllocated" of log event when `ShardCoordinator` allocates a shard to a region.
   * @param shardTypeName The `typeName` of the shard. Included as property "akkaShardTypeName".
   * @param shardId The id of the shard. Included as property "akkaShardId".
   * @param node The address of the node where the shard is allocated. Included as property "akkaRemoteAddress".
   */
  def shardAllocated(shardTypeName: String, shardId: String, node: Address): LogMarker =
    LogMarker(
      "akkaShardAllocated",
      Map(
        Properties.ShardTypeName -> shardTypeName,
        Properties.ShardId -> shardId,
        LogMarker.Properties.RemoteAddress -> node))

  /**
   * Marker "akkaShardStarted" of log event when `ShardRegion` starts a shard.
   * @param shardTypeName The `typeName` of the shard. Included as property "akkaShardTypeName".
   * @param shardId The id of the shard. Included as property "akkaShardId".
   */
  def shardStarted(shardTypeName: String, shardId: String): LogMarker =
    LogMarker("akkaShardStarted", Map(Properties.ShardTypeName -> shardTypeName, Properties.ShardId -> shardId))

}
