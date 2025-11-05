/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.ShardRegion.ClusterShardingStats
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.MultiNodeTypedClusterSpec
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.jackson.CborSerializable

object ClusterShardingStatsSpecConfig extends MultiNodeConfig {

  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
        akka.log-dead-letters-during-shutdown = off
        akka.cluster.sharding.updating-state-timeout = 2s
        akka.cluster.sharding.waiting-for-state-timeout = 2s
      """).withFallback(MultiNodeClusterSpec.clusterConfig))

}

class ClusterShardingStatsSpecMultiJvmNode1 extends ClusterShardingStatsSpec
class ClusterShardingStatsSpecMultiJvmNode2 extends ClusterShardingStatsSpec
class ClusterShardingStatsSpecMultiJvmNode3 extends ClusterShardingStatsSpec

object Pinger {
  sealed trait Command extends CborSerializable
  case class Ping(id: Int, ref: ActorRef[Pong]) extends Command
  case class Pong(id: Int) extends CborSerializable

  def apply(): Behavior[Command] = {
    Behaviors.receiveMessage[Command] {
      case Ping(id: Int, ref) =>
        ref ! Pong(id)
        Behaviors.same
    }
  }

}

abstract class ClusterShardingStatsSpec
    extends MultiNodeSpec(ClusterShardingStatsSpecConfig)
    with MultiNodeTypedClusterSpec
    with ScalaFutures {

  import ClusterShardingStatsSpecConfig._
  import Pinger._

  private val typeKey = EntityTypeKey[Command]("ping")
  private val sharding = ClusterSharding(typedSystem)
  private val settings = ClusterShardingSettings(typedSystem)
  private val queryTimeout = settings.shardRegionQueryTimeout * roles.size.toLong //numeric widening y'all

  "Cluster sharding stats" must {
    "form cluster" in {
      formCluster(first, second, third)
    }

    "get shard stats" in {
      sharding.init(Entity(typeKey)(_ => Pinger()))
      enterBarrier("sharding started")

      val pongProbe = TestProbe[Pong]()

      val entityRef1 = ClusterSharding(typedSystem).entityRefFor(typeKey, "ping-1")
      entityRef1 ! Ping(1, pongProbe.ref)
      pongProbe.receiveMessage()

      val entityRef2 = ClusterSharding(typedSystem).entityRefFor(typeKey, "ping-2")
      entityRef2 ! Ping(2, pongProbe.ref)
      pongProbe.receiveMessage()
      enterBarrier("sharding-initialized")

      val statsProbe = TestProbe[ClusterShardingStats]()
      sharding.shardState ! GetClusterShardingStats(typeKey, queryTimeout, statsProbe.ref)

      val stats = statsProbe.receiveMessage(queryTimeout)
      stats.regions.size shouldEqual 3 // 3 nodes
      stats.regions.values.flatMap(_.stats.values).sum shouldEqual 2 // number of entities
      stats.regions.values.forall(_.failed.isEmpty) shouldBe true

      enterBarrier("done")
    }

  }

}
