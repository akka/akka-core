/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.ActorRef
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.ShardId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class DDataRememberEntitiesProvider(
    typeName: String,
    settings: ClusterShardingSettings,
    majorityMinCap: Int,
    replicator: ActorRef)
    extends RememberEntitiesProvider {

  override def coordinatorStoreProps(): Props =
    DDataRememberEntitiesCoordinatorStore.props(typeName, settings, replicator, majorityMinCap)

  override def shardStoreProps(shardId: ShardId): Props =
    DDataRememberEntitiesShardStore.props(shardId, typeName, settings, replicator, majorityMinCap)
}
