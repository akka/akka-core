/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.ShardId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class EventSourcedRememberEntitiesProvider(typeName: String, settings: ClusterShardingSettings)
    extends RememberEntitiesProvider {

  // this is backed by an actor using the same events, at the serialization level, as the now removed PersistentShard when state-store-mode=persistence
  // new events can be added but the old events should continue to be handled
  override def shardStoreProps(shardId: ShardId): Props =
    EventSourcedRememberEntitiesShardStore.props(typeName, shardId, settings)

  // Note that this one is never used for the deprecated persistent state store mode, only when state store is ddata
  // combined with eventsourced remember entities storage
  override def coordinatorStoreProps(): Props =
    EventSourcedRememberEntitiesCoordinatorStore.props(typeName, settings)
}
