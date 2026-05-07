/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorRef
import akka.cluster.sharding.ShardRegion.ClusterShardingStats
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

/**
 * Protocol for querying sharding state e.g. A ShardRegion's state
 */
sealed trait ClusterShardingQuery

/**
 * Query the ShardRegion state for the given entity type key. This will get the state of the
 * local ShardRegion's state.
 *
 * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
 * the state of the shard regions.
 *
 * For the statistics for the entire cluster, see [[GetClusterShardingStats]].
 */
final case class GetShardRegionState(entityTypeKey: EntityTypeKey[_], replyTo: ActorRef[CurrentShardRegionState])
    extends ClusterShardingQuery {

  /**
   * Java API
   *
   * Query the ShardRegion state for the given entity type key. This will get the state of the
   * local ShardRegion's state.
   */
  def this(entityTypeKey: javadsl.EntityTypeKey[_], replyTo: ActorRef[CurrentShardRegionState]) =
    this(entityTypeKey.asScala, replyTo)
}

/**
 * Query the statistics about the currently running sharded entities in the
 * entire cluster. If the given `timeout` is reached without answers from all
 * shard regions the reply will contain an empty map of regions.
 *
 * Intended for testing purpose to see when cluster sharding is "ready" or to monitor
 * the state of the shard regions.
 *
 * @param timeout the timeout applied to querying all alive regions
 * @param replyTo the actor to send the result to
 */
final case class GetClusterShardingStats(
    entityTypeKey: EntityTypeKey[_],
    timeout: FiniteDuration,
    replyTo: ActorRef[ClusterShardingStats])
    extends ClusterShardingQuery {

  /**
   * Java API
   *
   * Query the statistics about the currently running sharded entities in the
   * entire cluster. If the given `timeout` is reached without answers from all
   * shard regions the reply will contain an empty map of regions.
   */
  def this(
      entityTypeKey: javadsl.EntityTypeKey[_],
      timeout: java.time.Duration,
      replyTo: ActorRef[ClusterShardingStats]) =
    this(entityTypeKey.asScala, timeout.toScala, replyTo)
}
