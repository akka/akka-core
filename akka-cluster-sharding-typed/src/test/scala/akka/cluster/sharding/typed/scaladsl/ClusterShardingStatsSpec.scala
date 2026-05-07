/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.cluster.sharding.ShardRegion.ClusterShardingStats
import akka.cluster.sharding.ShardRegion.ShardRegionStats
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.GetClusterShardingStats
import akka.cluster.sharding.typed.scaladsl.ClusterShardingSpec._
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.cluster.typed.SelfUp

class ClusterShardingStatsSpec
    extends ScalaTestWithActorTestKit(ClusterShardingSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  private val sharding = ClusterSharding(system)

  private val typeKey: EntityTypeKey[IdTestProtocol] = ClusterShardingSpec.typeKeyWithoutEnvelopes

  private val shardExtractor = ClusterShardingSpec.idTestProtocolMessageExtractor

  // no need to scale this up here for the cluster query versus one region
  private val queryTimeout = ClusterShardingSettings(system).shardRegionQueryTimeout

  "Cluster Sharding ClusterShardingStats query" must {
    "return empty statistics if there are no running sharded entities" in {
      val cluster = Cluster(system)
      val upProbe = createTestProbe[SelfUp]()

      cluster.subscriptions ! akka.cluster.typed.Subscribe(upProbe.ref, classOf[SelfUp])
      cluster.manager ! Join(cluster.selfMember.address)
      upProbe.expectMessageType[SelfUp]

      val replyProbe = createTestProbe[ClusterShardingStats]()
      sharding.shardState ! GetClusterShardingStats(typeKey, queryTimeout, replyProbe.ref)
      replyProbe.expectMessage(ClusterShardingStats(Map.empty))
    }

    "allow querying of statistics of the currently running sharded entities in the entire cluster" in {
      val shardingRef: ActorRef[IdTestProtocol] = sharding.init(
        Entity(typeKey)(_ => ClusterShardingSpec.behaviorWithId())
          .withStopMessage(IdStopPlz())
          .withMessageExtractor(idTestProtocolMessageExtractor))

      val replyProbe = createTestProbe[String]()
      val id1 = "id1"
      shardingRef ! IdReplyPlz(id1, replyProbe.ref)
      replyProbe.expectMessage("Hello!")

      val replyToProbe = createTestProbe[ClusterShardingStats]()
      val replyTo = replyToProbe.ref

      ClusterSharding(system).shardState ! GetClusterShardingStats(typeKey, queryTimeout, replyTo)
      val stats = replyToProbe.receiveMessage()

      val expect = ClusterShardingStats(
        Map(Cluster(system).selfMember.address -> ShardRegionStats(Map(shardExtractor.shardId(id1) -> 1), Set.empty)))

      stats shouldEqual expect
    }

  }

}
