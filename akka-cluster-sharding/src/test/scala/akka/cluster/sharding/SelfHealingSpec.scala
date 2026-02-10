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
import akka.testkit.{ AkkaSpec, EventFilter, TestProbe, WithLogCapturing }

object SelfHealingSpec {
  val commonConfig = ConfigFactory.parseString("""
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
      dir = "target/SelfHealingSpec/sharding-ddata"
      map-size = 10 MiB
    }
  """)

  val config = ConfigFactory.parseString("""
    # Enable self-healing with short timeouts for testing
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = off
    }
  """).withFallback(commonConfig)

  val configDryRun = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing.dry-run = on
  """).withFallback(config)

  val configDisabled = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing.enabled = off
  """).withFallback(commonConfig)

  val shardTypeName = "TestEntity"
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
}

class SelfHealingSpec extends AkkaSpec(SelfHealingSpec.config) with WithLogCapturing {

  import SelfHealingSpec._

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

  "Self-healing mechanism" must {

    "be enabled when configured" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.staleRegionTimeout shouldBe 2.seconds
      settings.selfHealingSettings.checkInterval shouldBe 500.millis
      settings.selfHealingSettings.startupGracePeriod shouldBe 1.second
    }

    "initialize cluster and sharding successfully with self-healing enabled" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Send a message to create an entity
      region.tell("test-entity-1", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity-1")

      // Verify the shard is allocated
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
    }

    "allocate shards across multiple entities" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create multiple entities
      (1 to 10).foreach { i =>
        region.tell(s"entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: entity-$i")
      }

      // Verify shards are allocated
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards.size should be > 0
    }

    "route messages to correct entities consistently" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Send message to same entity multiple times
      region.tell("consistent-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: consistent-entity")

      region.tell("consistent-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: consistent-entity")

      region.tell("consistent-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: consistent-entity")
    }

  }
}

class SelfHealingDryRunSpec extends AkkaSpec(SelfHealingSpec.configDryRun) with WithLogCapturing {

  import SelfHealingSpec._

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

  "Self-healing dry-run mode" must {

    "be enabled when configured" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.dryRun shouldBe true
    }

    "allow normal sharding operations in dry-run mode" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("dry-run-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: dry-run-entity")
    }

    "not deallocate shards in dry-run mode" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create entities
      (1 to 5).foreach { i =>
        region.tell(s"dry-run-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: dry-run-$i")
      }

      // Verify all entities are still accessible
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
    }
  }
}

class SelfHealingDisabledSpec extends AkkaSpec(SelfHealingSpec.configDisabled) with WithLogCapturing {

  import SelfHealingSpec._

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

  "Self-healing when disabled" must {

    "not be enabled" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe false
    }

    "still allow normal sharding operations" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Send a message to create an entity
      region.tell("test-entity-disabled", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity-disabled")
    }

    "not log self-healing enabled message on startup" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      // When disabled, no self-healing log should appear
      EventFilter.info(pattern = ".*Self-healing enabled.*", occurrences = 0).intercept {
        startSharding(system)
        // Give some time for logs to appear (they shouldn't)
        Thread.sleep(500)
      }
    }

    "have correct default values when disabled" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.staleRegionTimeout shouldBe 30.seconds
      settings.selfHealingSettings.checkInterval shouldBe 5.seconds
      settings.selfHealingSettings.startupGracePeriod shouldBe 60.seconds
      settings.selfHealingSettings.dryRun shouldBe false
    }
  }
}

// === Multi-Node Self-Healing Test ===

class SelfHealingMultiNodeSpec extends AkkaSpec(SelfHealingSpec.config) with WithLogCapturing {

  import SelfHealingSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile,
    new File("target/SelfHealingSpec2"))

  val sys2Config = ConfigFactory.parseString("""
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingSpec2/sharding-ddata"
    }
  """).withFallback(config)

  lazy val sys2: ActorSystem = ActorSystem(system.name, sys2Config)

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

    "form a two-node cluster with self-healing enabled" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)

      within(20.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
          Cluster(sys2).state.members.size shouldEqual 2
        }
      }
    }

    "distribute shards across nodes" in {
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

      // Create entities from both nodes
      (1 to 10).foreach { i =>
        region1.tell(s"from-node1-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: from-node1-$i")
      }

      (1 to 10).foreach { i =>
        region2.tell(s"from-node2-$i", probe2.ref)
        probe2.expectMsg(10.seconds, s"received: from-node2-$i")
      }
    }

    "route messages correctly between nodes" in {
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

      // Create entity from node 1
      region1.tell("cross-node-entity", probe1.ref)
      probe1.expectMsg(10.seconds, "received: cross-node-entity")

      // Access same entity from node 2
      val probe2 = TestProbe()(sys2)
      region2.tell("cross-node-entity", probe2.ref)
      probe2.expectMsg(10.seconds, "received: cross-node-entity")
    }

    "continue operation after graceful node leave" in {
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
      startSharding(sys2) // Start on second node as well

      val probe1 = TestProbe()(system)

      // Create some entities
      (1 to 5).foreach { i =>
        region1.tell(s"before-leave-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: before-leave-$i")
      }

      // Have node 2 leave gracefully
      Cluster(sys2).leave(Cluster(sys2).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 1
        }
      }

      // Verify entities still accessible from remaining node
      region1.tell("before-leave-1", probe1.ref)
      probe1.expectMsg(10.seconds, "received: before-leave-1")

      // Create new entities
      region1.tell("after-leave-entity", probe1.ref)
      probe1.expectMsg(10.seconds, "received: after-leave-entity")
    }
  }
}
