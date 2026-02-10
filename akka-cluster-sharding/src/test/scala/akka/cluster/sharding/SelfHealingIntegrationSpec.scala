/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.io.File

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.cluster.{ Cluster, MemberStatus }
import akka.testkit.{ AkkaSpec, TestProbe, WithLogCapturing }

object SelfHealingIntegrationSpec {

  val config = ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.hostname = "127.0.0.1"
    akka.remote.artery.canonical.port = 0
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = 5s
    akka.cluster.jmx.enabled = off
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingIntegrationSpec/sharding-ddata"
      map-size = 10 MiB
    }

    # Enable self-healing with short timeouts for testing
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 3s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = off
    }
  """)

  val configDryRun = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing.dry-run = on
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingIntegrationDryRunSpec/sharding-ddata"
    }
  """).withFallback(config)

  val shardTypeName = "IntegrationTestEntity"
  val numberOfShards = 10

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: String => (msg, msg)
    case _           => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: String                 => (math.abs(msg.hashCode) % numberOfShards).toString
    case ShardRegion.StartEntity(id) => (math.abs(id.hashCode) % numberOfShards).toString
    case _                           => throw new IllegalArgumentException()
  }

  class TestEntity extends Actor {
    override def receive: Receive = {
      case msg => sender() ! s"received: $msg"
    }
  }

  def configForNode(nodeNum: Int): com.typesafe.config.Config = {
    ConfigFactory.parseString(s"""
      akka.cluster.sharding.distributed-data.durable.lmdb {
        dir = "target/SelfHealingIntegrationSpec$nodeNum/sharding-ddata"
      }
    """).withFallback(config)
  }
}

class SelfHealingIntegrationSpec extends AkkaSpec(SelfHealingIntegrationSpec.config) with WithLogCapturing {

  import SelfHealingIntegrationSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile,
    new File("target/SelfHealingIntegrationSpec2"),
    new File("target/SelfHealingIntegrationSpec3"))

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  def startSharding(sys: ActorSystem): ActorRef = {
    ClusterSharding(sys).start(
      shardTypeName,
      Props[TestEntity](),
      ClusterShardingSettings(sys),
      extractEntityId,
      extractShardId)
  }

  "Self-healing integration" must {

    "form cluster with self-healing enabled and verify configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.staleRegionTimeout shouldBe 3.seconds
      settings.selfHealingSettings.checkInterval shouldBe 500.millis
      settings.selfHealingSettings.startupGracePeriod shouldBe 1.second
      settings.selfHealingSettings.dryRun shouldBe false
    }

    "start single-node cluster and allocate shards" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create entities across multiple shards
      (1 to 10).foreach { i =>
        region.tell(s"entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: entity-$i")
      }

      // Verify shards are allocated
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
      state.shards.size should be > 0
    }

    "route messages correctly after cluster formation" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Send messages to same entity multiple times
      region.tell("consistent-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: consistent-entity")

      region.tell("consistent-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: consistent-entity")

      // Different entities
      region.tell("another-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: another-entity")
    }
  }
}

class SelfHealingIntegrationMultiNodeSpec extends AkkaSpec(SelfHealingIntegrationSpec.config) with WithLogCapturing {

  import SelfHealingIntegrationSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile,
    new File("target/SelfHealingIntegrationSpec2"))

  // Create second actor system
  lazy val sys2: ActorSystem = ActorSystem(system.name, configForNode(2))

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override protected def beforeTermination(): Unit = {
    if (sys2 != null && !sys2.whenTerminated.isCompleted) {
      shutdown(sys2)
    }
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  def startSharding(sys: ActorSystem): ActorRef = {
    ClusterSharding(sys).start(
      shardTypeName,
      Props[TestEntity](),
      ClusterShardingSettings(sys),
      extractEntityId,
      extractShardId)
  }

  "Self-healing with multiple nodes" must {

    "form two-node cluster with self-healing enabled" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)

      within(20.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
          Cluster(sys2).state.members.size shouldEqual 2
          Cluster(sys2).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      // Verify self-healing is enabled on both nodes
      ClusterShardingSettings(system).selfHealingSettings.enabled shouldBe true
      ClusterShardingSettings(sys2).selfHealingSettings.enabled shouldBe true
    }

    "allocate shards across both nodes" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      within(20.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      val region1 = startSharding(system)
      val region2 = startSharding(sys2)

      val probe1 = TestProbe()(system)
      val probe2 = TestProbe()(sys2)

      // Create multiple entities to spread across shards
      (1 to 20).foreach { i =>
        region1.tell(s"entity-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: entity-$i")
      }

      // Verify from the other node
      region2.tell("cross-node-test", probe2.ref)
      probe2.expectMsg(10.seconds, "received: cross-node-test")
    }

    "continue operation when second node joins after sharding started" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      // Start sharding on first node
      val region1 = startSharding(system)
      val probe1 = TestProbe()(system)

      // Create some entities
      (1 to 5).foreach { i =>
        region1.tell(s"early-entity-$i", probe1.ref)
        probe1.expectMsg(5.seconds, s"received: early-entity-$i")
      }

      // Now join second node
      Cluster(sys2).join(Cluster(system).selfAddress)
      within(20.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      // Start sharding on second node
      val region2 = startSharding(sys2)
      val probe2 = TestProbe()(sys2)

      // Verify existing entities still work
      region1.tell("early-entity-1", probe1.ref)
      probe1.expectMsg(5.seconds, "received: early-entity-1")

      // Verify new entities work from both nodes
      region2.tell("late-entity", probe2.ref)
      probe2.expectMsg(10.seconds, "received: late-entity")
    }
  }
}

class SelfHealingIntegrationThreeNodeSpec extends AkkaSpec(SelfHealingIntegrationSpec.config) with WithLogCapturing {

  import SelfHealingIntegrationSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile,
    new File("target/SelfHealingIntegrationSpec2"),
    new File("target/SelfHealingIntegrationSpec3"))

  lazy val sys2: ActorSystem = ActorSystem(system.name, configForNode(2))
  lazy val sys3: ActorSystem = ActorSystem(system.name, configForNode(3))

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override protected def beforeTermination(): Unit = {
    if (sys2 != null && !sys2.whenTerminated.isCompleted) shutdown(sys2)
    if (sys3 != null && !sys3.whenTerminated.isCompleted) shutdown(sys3)
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  def startSharding(sys: ActorSystem): ActorRef = {
    ClusterSharding(sys).start(
      shardTypeName,
      Props[TestEntity](),
      ClusterShardingSettings(sys),
      extractEntityId,
      extractShardId)
  }

  "Self-healing with three nodes" must {

    "form three-node cluster successfully" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      Cluster(sys3).join(Cluster(system).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 3
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }
    }

    "distribute shards across three nodes and handle messaging" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      Cluster(sys3).join(Cluster(system).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 3
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      val region1 = startSharding(system)
      val region2 = startSharding(sys2)
      val region3 = startSharding(sys3)

      val probe1 = TestProbe()(system)
      val probe2 = TestProbe()(sys2)
      val probe3 = TestProbe()(sys3)

      // Create entities from different nodes
      (1 to 10).foreach { i =>
        region1.tell(s"from-node1-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: from-node1-$i")
      }

      (1 to 10).foreach { i =>
        region2.tell(s"from-node2-$i", probe2.ref)
        probe2.expectMsg(10.seconds, s"received: from-node2-$i")
      }

      (1 to 10).foreach { i =>
        region3.tell(s"from-node3-$i", probe3.ref)
        probe3.expectMsg(10.seconds, s"received: from-node3-$i")
      }

      // Cross-node access
      region1.tell("from-node2-1", probe1.ref)
      probe1.expectMsg(10.seconds, "received: from-node2-1")
    }

    "handle node leaving gracefully" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      Cluster(sys3).join(Cluster(system).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 3
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      val region1 = startSharding(system)
      startSharding(sys2) // Start on second node
      startSharding(sys3) // Start on third node

      val probe1 = TestProbe()(system)

      // Create some entities
      (1 to 5).foreach { i =>
        region1.tell(s"entity-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: entity-$i")
      }

      // Have node 3 leave gracefully
      Cluster(sys3).leave(Cluster(sys3).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
        }
      }

      // Verify remaining nodes still work
      region1.tell("after-leave-entity", probe1.ref)
      probe1.expectMsg(10.seconds, "received: after-leave-entity")
    }
  }
}

class SelfHealingIntegrationDryRunSpec extends AkkaSpec(SelfHealingIntegrationSpec.configDryRun) with WithLogCapturing {

  import SelfHealingIntegrationSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile)

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  def startSharding(sys: ActorSystem): ActorRef = {
    ClusterSharding(sys).start(
      shardTypeName,
      Props[TestEntity](),
      ClusterShardingSettings(sys),
      extractEntityId,
      extractShardId)
  }

  "Self-healing in dry-run mode integration" must {

    "be configured with dry-run enabled" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.dryRun shouldBe true
    }

    "still allow normal sharding operations" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("dry-run-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: dry-run-entity")
    }
  }
}
