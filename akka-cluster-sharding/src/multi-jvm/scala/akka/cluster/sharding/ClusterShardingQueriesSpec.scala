/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor.Props
import akka.testkit.TestProbe

object ClusterShardingQueriesSpec {
  import MultiNodeClusterShardingSpec.PingPongActor

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ PingPongActor.Ping(id) => (id.toString, msg)
  }

  val numberOfShards = 6

  val extractShardId: ShardRegion.ExtractShardId = {
    case PingPongActor.Ping(id) => (id % numberOfShards).toString
    case _                      => throw new RuntimeException()
  }

  val shardTypeName = "DatatypeA"
}

object ClusterShardingQueriesSpecConfig
    extends MultiNodeClusterShardingConfig(additionalConfig = """
        akka.log-dead-letters-during-shutdown = off
        akka.cluster.sharding {
          shard-region-query-timeout = 2ms
          updating-state-timeout = 2s
          waiting-for-state-timeout = 2s
        }
        """) {

  val controller = role("controller")
  val busy = role("busy")
  val second = role("second")
  val third = role("third")

  val shardRoles = ConfigFactory.parseString("""akka.cluster.roles=["shard"]""")

  nodeConfig(busy)(
    ConfigFactory.parseString("akka.cluster.sharding.shard-region-query-timeout = 0ms").withFallback(shardRoles))
  nodeConfig(second, third)(shardRoles)

}

class ClusterShardingQueriesSpecMultiJvmNode1 extends ClusterShardingQueriesSpec
class ClusterShardingQueriesSpecMultiJvmNode2 extends ClusterShardingQueriesSpec
class ClusterShardingQueriesSpecMultiJvmNode3 extends ClusterShardingQueriesSpec
class ClusterShardingQueriesSpecMultiJvmNode4 extends ClusterShardingQueriesSpec

abstract class ClusterShardingQueriesSpec
    extends MultiNodeClusterShardingSpec(ClusterShardingQueriesSpecConfig)
    with ScalaFutures {

  import ClusterShardingQueriesSpec._
  import ClusterShardingQueriesSpecConfig._
  import MultiNodeClusterShardingSpec.PingPongActor

  lazy val region = ClusterSharding(system).shardRegion(shardTypeName)

  "Querying cluster sharding" must {

    "join cluster, initialize sharding" in {
      awaitClusterUp(controller, busy, second, third)

      runOn(controller) {
        startProxy(
          system,
          typeName = shardTypeName,
          role = Some("shard"),
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)
      }

      runOn(busy, second, third) {
        startSharding(
          system,
          typeName = shardTypeName,
          entityProps = Props(new PingPongActor),
          settings = settings.withRole("shard"),
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)
      }

      enterBarrier("sharding started")
    }

    "trigger sharded actors" in {
      runOn(controller) {
        within(10.seconds) {
          awaitAssert {
            val pingProbe = TestProbe()
            (0 to 20).foreach(n => region.tell(PingPongActor.Ping(n), pingProbe.ref))
            pingProbe.receiveWhile(messages = 20) {
              case PingPongActor.Pong => ()
            }
          }
        }
      }
      enterBarrier("sharded actors started")
    }

    "return shard stats of cluster sharding regions if one or more shards timeout, versus all as empty" in {
      runOn(busy, second, third) {
        val probe = TestProbe()
        val region = ClusterSharding(system).shardRegion(shardTypeName)
        awaitAssert({
          region.tell(ShardRegion.GetClusterShardingStats(10.seconds), probe.ref)
          val regions = probe.expectMsgType[ShardRegion.ClusterShardingStats].regions
          regions.size shouldEqual 3
          val timeouts = numberOfShards / regions.size

          // 3 regions, 2 shards per region, all 2 shards/region were unresponsive
          // within shard-region-query-timeout, which only on first is 0ms
          regions.values.map(_.stats.size).sum shouldEqual 4
          regions.values.map(_.failed.size).sum shouldEqual timeouts
        }, max = 10.seconds)
      }
      enterBarrier("received failed stats from timed out shards vs empty")
    }

    "return shard state of sharding regions if one or more shards timeout, versus all as empty" in {
      runOn(busy) {
        val probe = TestProbe()
        val region = ClusterSharding(system).shardRegion(shardTypeName)
        awaitAssert({
          region.tell(ShardRegion.GetShardRegionState, probe.ref)
          val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState]
          state.shards.isEmpty shouldEqual true
          state.failed.size shouldEqual 2
        }, max = 10.seconds)
      }
      enterBarrier("query-timeout-on-busy-node")

      runOn(second, third) {
        val probe = TestProbe()
        val region = ClusterSharding(system).shardRegion(shardTypeName)
        awaitAssert({
          region.tell(ShardRegion.GetShardRegionState, probe.ref)
          val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState]
          state.shards.size shouldEqual 2
          state.failed.isEmpty shouldEqual true
        }, max = 10.seconds)
      }
      enterBarrier("done")
    }

  }

}
