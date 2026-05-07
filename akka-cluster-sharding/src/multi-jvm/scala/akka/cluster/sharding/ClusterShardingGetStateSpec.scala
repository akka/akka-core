/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.testkit.TestProbe

object ClusterShardingGetStateSpec {
  import MultiNodeClusterShardingSpec.PingPongActor

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ PingPongActor.Ping(id) => (id.toString, msg)
  }

  val numberOfShards = 2

  val extractShardId: ShardRegion.ExtractShardId = {
    case PingPongActor.Ping(id) => (id % numberOfShards).toString
    case _                      => throw new IllegalArgumentException()
  }

  val shardTypeName = "Ping"
}

object ClusterShardingGetStateSpecConfig
    extends MultiNodeClusterShardingConfig(additionalConfig = """
    akka.cluster.sharding {
      coordinator-failure-backoff = 3s
      shard-failure-backoff = 3s
    }
    """) {

  val controller = role("controller")
  val first = role("first")
  val second = role("second")

  nodeConfig(first, second)(ConfigFactory.parseString("""akka.cluster.roles=["shard"]"""))

}

class ClusterShardingGetStateSpecMultiJvmNode1 extends ClusterShardingGetStateSpec
class ClusterShardingGetStateSpecMultiJvmNode2 extends ClusterShardingGetStateSpec
class ClusterShardingGetStateSpecMultiJvmNode3 extends ClusterShardingGetStateSpec

abstract class ClusterShardingGetStateSpec extends MultiNodeClusterShardingSpec(ClusterShardingGetStateSpecConfig) {

  import ClusterShardingGetStateSpec._
  import ClusterShardingGetStateSpecConfig._
  import MultiNodeClusterShardingSpec.PingPongActor

  "Inspecting cluster sharding state" must {

    "join cluster" in {
      join(controller, controller)
      join(first, controller)
      join(second, controller)

      // make sure all nodes has joined
      awaitAssert {
        Cluster(system).sendCurrentClusterState(testActor)
        expectMsgType[CurrentClusterState].members.size === 3
      }

      runOn(controller) {
        startProxy(
          system,
          typeName = shardTypeName,
          role = Some("shard"),
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)
      }

      runOn(first, second) {
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

    "return empty state when no sharded actors has started" in {

      awaitAssert {
        val probe = TestProbe()
        val region = ClusterSharding(system).shardRegion(shardTypeName)
        region.tell(ShardRegion.GetCurrentRegions, probe.ref)
        probe.expectMsgType[ShardRegion.CurrentRegions].regions.size === 0
      }

      enterBarrier("empty sharding")
    }

    "trigger sharded actors" in {
      runOn(controller) {
        val region = ClusterSharding(system).shardRegion(shardTypeName)

        within(10.seconds) {
          awaitAssert {
            val pingProbe = TestProbe()
            // trigger starting of 4 entities
            (1 to 4).foreach(n => region.tell(PingPongActor.Ping(n), pingProbe.ref))
            pingProbe.receiveWhile(messages = 4) {
              case PingPongActor.Pong => ()
            }
          }
        }
      }

      enterBarrier("sharded actors started")

    }

    "get shard state" in {
      within(10.seconds) {
        awaitAssert {
          val probe = TestProbe()
          val region = ClusterSharding(system).shardRegion(shardTypeName)
          region.tell(ShardRegion.GetCurrentRegions, probe.ref)
          val regions = probe.expectMsgType[ShardRegion.CurrentRegions].regions
          regions.size === 2
          regions.foreach { region =>
            val path = RootActorPath(region) / "system" / "sharding" / shardTypeName

            system.actorSelection(path).tell(ShardRegion.GetShardRegionState, probe.ref)
          }
          val states = probe.receiveWhile(messages = regions.size) {
            case msg: ShardRegion.CurrentShardRegionState => msg
          }
          val allEntityIds = for {
            state <- states
            shard <- state.shards
            entityId <- shard.entityIds
          } yield entityId

          allEntityIds.toSet === Set("1", "2", "3", "4")
        }
      }

      enterBarrier("done")

    }
  }
}
