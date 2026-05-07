/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.typed.ClusterShardingQuery
import akka.cluster.sharding.typed.GetClusterShardingStats
import akka.cluster.sharding.typed.GetShardRegionState

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ShardingState {

  def behavior(classicSharding: ClusterSharding): Behavior[ClusterShardingQuery] = {
    Behaviors
      .supervise[ClusterShardingQuery] {
        Behaviors.setup { context =>
          Behaviors.receiveMessage {
            case GetShardRegionState(key, replyTo) =>
              if (classicSharding.getShardTypeNames.contains(key.name)) {
                try {
                  classicSharding.shardRegion(key.name).tell(ShardRegion.GetShardRegionState, replyTo.toClassic)
                } catch {
                  case e: IllegalStateException =>
                    // classicSharding.shardRegion may throw if not initialized
                    context.log.warn(e.getMessage)
                    replyTo ! CurrentShardRegionState(Set.empty)
                }
              } else {
                replyTo ! CurrentShardRegionState(Set.empty)
              }
              Behaviors.same

            case GetClusterShardingStats(key, timeout, replyTo) =>
              if (classicSharding.getShardTypeNames.contains(key.name)) {
                try {
                  classicSharding
                    .shardRegion(key.name)
                    .tell(ShardRegion.GetClusterShardingStats(timeout), replyTo.toClassic)
                } catch {
                  case e: IllegalStateException =>
                    // classicSharding.shardRegion may throw if not initialized
                    context.log.warn(e.getMessage)
                    replyTo ! ShardRegion.ClusterShardingStats(Map.empty)
                }
              } else {
                replyTo ! ShardRegion.ClusterShardingStats(Map.empty)
              }
              Behaviors.same
          }
        }
      }
      .onFailure(SupervisorStrategy.restart)
  }

}
