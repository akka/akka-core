/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object ClusterShardingHealthCheckSpec {
  val config = ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    """)
}

class ClusterShardingHealthCheckSpec
    extends AkkaSpec(ClusterShardingHealthCheckSpec.config)
    with WithLogCapturing
    with ScalaFutures {

  "Sharding health check" should {
    "pass if no checks configured" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set.empty, 1.second, 10.seconds),
        _ => shardRegionProbe.ref)
      check().futureValue shouldEqual true
    }
    "pass if all region return true" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat"), 1.second, 10.seconds),
        _ => shardRegionProbe.ref)
      val response = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", true))
      response.futureValue shouldEqual true
    }
    "fail if all region returns false" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat"), 1.second, 10.seconds),
        _ => shardRegionProbe.ref)
      val response = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", false))
      response.futureValue shouldEqual false
    }
    "fail if a subset region returns false" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat", "dog"), 1.second, 10.seconds),
        _ => shardRegionProbe.ref)
      val response = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", true))
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("dog", false))
      response.futureValue shouldEqual false
    }
    "times out" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat"), 100.millis, 10.seconds),
        _ => shardRegionProbe.ref)
      val response = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      // don't reply
      response.futureValue shouldEqual false
    }
    "always pass after all regions have reported registered" in {
      val shardRegionProbe = TestProbe()
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat"), 1.second, 10.seconds),
        _ => shardRegionProbe.ref)
      val response = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", true))
      response.futureValue shouldEqual true

      val secondResponse = check()
      shardRegionProbe.expectNoMessage()
      secondResponse.futureValue shouldEqual true
    }

    "always pass after disabled-after" in {
      val shardRegionProbe = TestProbe()
      val disabledAfter = 100.millis
      val check = new ClusterShardingHealthCheck(
        system,
        new ClusterShardingHealthCheckSettings(Set("cat"), 1.second, disabledAfter),
        _ => shardRegionProbe.ref)
      // first check will always be performed
      val response1 = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", false))
      response1.futureValue shouldEqual false

      Thread.sleep(disabledAfter.toMillis + 100)

      // and it will not start the clock until member up
      val response2 = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", false))
      response2.futureValue shouldEqual false

      Thread.sleep(disabledAfter.toMillis + 100)

      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert {
        Cluster(system).selfMember.status shouldEqual MemberStatus.Up
      }

      // first check after member up will trigger start of clock
      val response3 = check()
      shardRegionProbe.expectMsg(ShardRegion.GetShardRegionStatus)
      shardRegionProbe.reply(new ShardRegion.ShardRegionStatus("cat", false))
      response3.futureValue shouldEqual false

      Thread.sleep(disabledAfter.toMillis + 100)

      // and now it has exceeded the disabled-after duration
      val response4 = check()
      shardRegionProbe.expectNoMessage()
      response4.futureValue shouldEqual true
    }
  }

}
