/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.external

import scala.concurrent.duration._

import akka.cluster.sharding.external.ExternalShardAllocationStrategy.GetShardLocation
import akka.cluster.sharding.external.ExternalShardAllocationStrategy.GetShardLocationResponse
import akka.cluster.sharding.external.ExternalShardAllocationStrategy.GetShardLocations
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.util.Timeout

class ExternalShardAllocationStrategySpec extends AkkaSpec("""
    akka.actor.provider = cluster 
    akka.loglevel = INFO 
    akka.remote.artery.canonical.port = 0
    """) {

  val requester = TestProbe()

  "ExternalShardAllocationClient" must {
    "default to no locations if sharding never started" in {
      ExternalShardAllocation(system)
        .clientFor("not found")
        .shardLocations()
        .futureValue
        .locations shouldEqual Map.empty
    }
  }

  "ExternalShardAllocation allocate" must {
    "default to requester if query times out" in {
      val (strat, _) = createStrategy()
      strat.allocateShard(requester.ref, "shard-1", Map.empty).futureValue shouldEqual requester.ref
    }
    "default to requester if no allocation" in {
      val (strat, probe) = createStrategy()
      val allocation = strat.allocateShard(requester.ref, "shard-1", Map.empty)
      probe.expectMsg(GetShardLocation("shard-1"))
      probe.reply(GetShardLocationResponse(None))
      allocation.futureValue shouldEqual requester.ref
    }
  }

  "ExternalShardAllocation rebalance" must {
    "default to no rebalance if query times out" in {
      val (strat, probe) = createStrategy()
      val rebalance = strat.rebalance(Map.empty, Set.empty)
      probe.expectMsg(GetShardLocations)
      rebalance.futureValue shouldEqual Set.empty
    }
  }

  def createStrategy(): (ExternalShardAllocationStrategy, TestProbe) = {
    val probe = TestProbe()
    val strategy = new ExternalShardAllocationStrategy(system, "type")(Timeout(250.millis)) {
      override private[akka] def createShardStateActor() = probe.ref
    }
    strategy.start()
    (strategy, probe)
  }

}
