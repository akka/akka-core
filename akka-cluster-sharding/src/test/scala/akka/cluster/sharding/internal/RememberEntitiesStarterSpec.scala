/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.Shard
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.EntityId
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

class RememberEntitiesStarterSpec extends AkkaSpec {

  var shardIdCounter = 1
  def nextShardId(): ShardId = {
    val id = s"ShardId$shardIdCounter"
    shardIdCounter += 1
    id
  }

  "The RememberEntitiesStarter" must {
    "try start all entities directly with entity-recovery-strategy = all (default)" in {
      val regionProbe = TestProbe()
      val shardProbe = TestProbe()
      val shardId = nextShardId()

      val defaultSettings = ClusterShardingSettings(system)

      val rememberEntityStarter = system.actorOf(
        RememberEntityStarter.props(
          regionProbe.ref,
          shardProbe.ref,
          shardId,
          Set("1", "2", "3"),
          isConstantStrategy = false,
          defaultSettings))

      watch(rememberEntityStarter)
      val startedEntityIds = (1 to 3).map { _ =>
        val start = regionProbe.expectMsgType[ShardRegion.StartEntity]
        regionProbe.lastSender ! ShardRegion.StartEntityAck(start.entityId, shardId)
        start.entityId
      }.toSet
      startedEntityIds should ===(Set("1", "2", "3"))

      // the starter should then stop itself, not sending anything more to the shard or region
      expectTerminated(rememberEntityStarter)
      shardProbe.expectNoMessage()
      regionProbe.expectNoMessage()
    }

    "retry start all entities with no ack with entity-recovery-strategy = all (default)" in {
      val regionProbe = TestProbe()
      val shardProbe = TestProbe()
      val shardId = nextShardId()

      val customSettings = ClusterShardingSettings(
        ConfigFactory
          .parseString(
            // the restarter somewhat surprisingly uses this for no-ack-retry. Tune it down to speed up test
            """
             retry-interval = 1 second
            """)
          .withFallback(system.settings.config.getConfig("akka.cluster.sharding")))

      val rememberEntityStarter = system.actorOf(
        RememberEntityStarter.props(
          regionProbe.ref,
          shardProbe.ref,
          shardId,
          Set("1", "2", "3"),
          isConstantStrategy = false,
          customSettings))

      watch(rememberEntityStarter)
      (1 to 3).foreach { _ =>
        regionProbe.expectMsgType[ShardRegion.StartEntity]
      }
      val startedOnSecondTry = (1 to 3).map { _ =>
        val start = regionProbe.expectMsgType[ShardRegion.StartEntity]
        regionProbe.lastSender ! ShardRegion.StartEntityAck(start.entityId, shardId)
        start.entityId
      }.toSet
      startedOnSecondTry should ===(Set("1", "2", "3"))

      // should stop itself, not sending anything to the shard
      expectTerminated(rememberEntityStarter)
      shardProbe.expectNoMessage()
    }

    "inform the shard when entities has been reallocated to different shard id" in {
      val regionProbe = TestProbe()
      val shardProbe = TestProbe()
      val shardId = nextShardId()

      val customSettings = ClusterShardingSettings(
        ConfigFactory
          .parseString(
            // the restarter somewhat surprisingly uses this for no-ack-retry. Tune it down to speed up test
            """
             retry-interval = 1 second
            """)
          .withFallback(system.settings.config.getConfig("akka.cluster.sharding")))

      val rememberEntityStarter = system.actorOf(
        RememberEntityStarter.props(
          regionProbe.ref,
          shardProbe.ref,
          shardId,
          Set("1", "2", "3"),
          isConstantStrategy = false,
          customSettings))

      watch(rememberEntityStarter)
      val start1 = regionProbe.expectMsgType[ShardRegion.StartEntity]
      regionProbe.lastSender ! ShardRegion.StartEntityAck(start1.entityId, shardId) // keep on current shard

      val start2 = regionProbe.expectMsgType[ShardRegion.StartEntity]
      regionProbe.lastSender ! ShardRegion.StartEntityAck(start2.entityId, shardId = "Relocated1")

      val start3 = regionProbe.expectMsgType[ShardRegion.StartEntity]
      regionProbe.lastSender ! ShardRegion.StartEntityAck(start3.entityId, shardId = "Relocated2")

      shardProbe.expectMsg(Shard.EntitiesMovedToOtherShard(Set("2", "3")))
      expectTerminated(rememberEntityStarter)
    }

    "try start all entities in a throttled way with entity-recovery-strategy = constant" in {
      val regionProbe = TestProbe()
      val shardProbe = TestProbe()
      val shardId = nextShardId()

      val customSettings = ClusterShardingSettings(
        ConfigFactory
          .parseString(
            // slow constant restart
            """
             entity-recovery-strategy = constant
             entity-recovery-constant-rate-strategy {
               frequency = 2 s
               number-of-entities = 2
             }
             retry-interval = 1 second
            """)
          .withFallback(system.settings.config.getConfig("akka.cluster.sharding")))

      val rememberEntityStarter = system.actorOf(
        RememberEntityStarter.props(
          regionProbe.ref,
          shardProbe.ref,
          shardId,
          Set("1", "2", "3", "4", "5"),
          isConstantStrategy = true,
          customSettings))

      def recieveStartAndAck() = {
        val start = regionProbe.expectMsgType[ShardRegion.StartEntity]
        regionProbe.lastSender ! ShardRegion.StartEntityAck(start.entityId, shardId)
      }

      watch(rememberEntityStarter)
      // first batch should be immediate
      recieveStartAndAck()
      recieveStartAndAck()
      // second batch holding off (with some room for unstable test env)
      regionProbe.expectNoMessage(600.millis)

      // second batch should be immediate
      recieveStartAndAck()
      recieveStartAndAck()
      // third batch holding off
      regionProbe.expectNoMessage(600.millis)

      recieveStartAndAck()

      // the starter should then stop itself, not sending anything more to the shard or region
      expectTerminated(rememberEntityStarter)
      shardProbe.expectNoMessage()
      regionProbe.expectNoMessage()
    }

  }

  "The RememberEntitiesStarterManager" must {
    "try start all entities directly with entity-recovery-strategy = all (default)" in {
      val regionProbe = TestProbe()
      val shard1Probe = TestProbe()
      val shardId1 = nextShardId()
      val shard2Probe = TestProbe()
      val shardId2 = nextShardId()

      val defaultSettings = ClusterShardingSettings(system)

      val rememberEntityStarterManager =
        system.actorOf(RememberEntityStarterManager.props(regionProbe.ref, defaultSettings))

      rememberEntityStarterManager ! RememberEntityStarterManager.StartEntities(
        shard1Probe.ref,
        shardId1,
        Set("1", "2", "3"))
      rememberEntityStarterManager ! RememberEntityStarterManager.StartEntities(
        shard2Probe.ref,
        shardId2,
        Set("4", "5", "6"))

      val startedEntityIds = (1 to 6).map { _ =>
        val start = regionProbe.expectMsgType[ShardRegion.StartEntity]
        val shardId = if (start.entityId.toInt <= 3) shardId1 else shardId2
        regionProbe.lastSender ! ShardRegion.StartEntityAck(start.entityId, shardId)
        start.entityId
      }.toSet
      startedEntityIds should ===(Set("1", "2", "3", "4", "5", "6"))
    }

    "try start all entities in a throttled way with entity-recovery-strategy = constant" in {
      val regionProbe = TestProbe()
      val shard1Probe = TestProbe()
      val shardId1 = nextShardId()
      val shard2Probe = TestProbe()
      val shardId2 = nextShardId()

      val customSettings = ClusterShardingSettings(
        ConfigFactory
          .parseString(
            // slow constant restart
            """
             entity-recovery-strategy = constant
             entity-recovery-constant-rate-strategy {
               frequency = 2 s
               number-of-entities = 2
             }
             retry-interval = 1 second
            """)
          .withFallback(system.settings.config.getConfig("akka.cluster.sharding")))

      val rememberEntityStarterManager =
        system.actorOf(RememberEntityStarterManager.props(regionProbe.ref, customSettings))

      rememberEntityStarterManager ! RememberEntityStarterManager.StartEntities(
        shard1Probe.ref,
        shardId1,
        Set("1", "2", "3", "4", "5"))
      rememberEntityStarterManager ! RememberEntityStarterManager.StartEntities(
        shard2Probe.ref,
        shardId2,
        Set("6", "7", "8"))

      def recieveStartAndAck(): EntityId = {
        val start = regionProbe.expectMsgType[ShardRegion.StartEntity]
        val shardId = if (start.entityId.toInt <= 5) shardId1 else shardId2
        regionProbe.lastSender ! ShardRegion.StartEntityAck(start.entityId, shardId)
        start.entityId
      }

      var startedEntityIds = Set.empty[EntityId]

      // first batch should be immediate
      startedEntityIds += recieveStartAndAck()
      startedEntityIds += recieveStartAndAck()

      // second batch holding off (with some room for unstable test env)
      regionProbe.expectNoMessage(600.millis)
      startedEntityIds += recieveStartAndAck()
      startedEntityIds += recieveStartAndAck()

      // third batch holding off
      regionProbe.expectNoMessage(600.millis)
      startedEntityIds += recieveStartAndAck()

      startedEntityIds should ===(Set("1", "2", "3", "4", "5"))

      // now the second StartEntities messages for shard2
      // batch holding off
      regionProbe.expectNoMessage(600.millis)
      startedEntityIds += recieveStartAndAck()
      startedEntityIds += recieveStartAndAck()

      regionProbe.expectNoMessage(600.millis)
      startedEntityIds += recieveStartAndAck()

      startedEntityIds should ===(Set("1", "2", "3", "4", "5", "6", "7", "8"))
    }
  }
}
