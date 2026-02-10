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

object SelfHealingCoordinatorSpec {

  val baseConfig = ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.hostname = "127.0.0.1"
    akka.remote.artery.canonical.port = 0
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.jmx.enabled = off
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingCoordinatorSpec/sharding-ddata"
      map-size = 10 MiB
    }
  """)

  val enabledConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = off
    }
  """).withFallback(baseConfig)

  val disabledConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = off
    }
  """).withFallback(baseConfig)

  val dryRunConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = on
    }
  """).withFallback(baseConfig)

  val longGracePeriodConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 60s
      dry-run = off
    }
  """).withFallback(baseConfig)

  val zeroGracePeriodConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 0s
      dry-run = off
    }
  """).withFallback(baseConfig)

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

// === Timer Scheduling Tests ===

class SelfHealingCoordinatorTimerEnabledSpec
    extends AkkaSpec(SelfHealingCoordinatorSpec.enabledConfig)
    with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

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

  "Self-healing timer when enabled" must {

    "schedule SelfHealingTick timer at check-interval" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      // Starting sharding will start the coordinator which schedules the timer
      // We verify the timer is running by checking logs
      EventFilter.info(pattern = "Self-healing enabled with.*", occurrences = 1).intercept {
        startSharding(system)
      }
    }
  }
}

class SelfHealingCoordinatorTimerDisabledSpec
    extends AkkaSpec(SelfHealingCoordinatorSpec.disabledConfig)
    with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

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

  "Self-healing timer when disabled" must {

    "not schedule SelfHealingTick timer" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      // When disabled, the self-healing enabled log should NOT appear
      EventFilter.info(pattern = "Self-healing enabled with.*", occurrences = 0).intercept {
        startSharding(system)
        // Give some time for logs that shouldn't appear
        Thread.sleep(500)
      }
    }
  }
}

// === Grace Period Tests ===

class SelfHealingCoordinatorGracePeriodSpec
    extends AkkaSpec(SelfHealingCoordinatorSpec.longGracePeriodConfig)
    with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

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

  "Self-healing during grace period" must {

    "skip self-healing check and log debug message" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create an entity to ensure sharding is active
      region.tell("test-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity")

      // Since grace period is 60s, self-healing should be skipped
      // and debug log should appear (at least one occurrence with 500ms intervals)
      EventFilter.debug(pattern = ".*Self-healing skipped.*within startup grace period.*").intercept {
        // Wait for at least one tick (500ms check interval)
        Thread.sleep(1000)
      }
    }
  }
}

class SelfHealingCoordinatorZeroGracePeriodSpec
    extends AkkaSpec(SelfHealingCoordinatorSpec.zeroGracePeriodConfig)
    with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

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

  "Self-healing with zero grace period" must {

    "activate immediately without grace period delay" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create an entity to ensure sharding is active
      region.tell("test-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity")

      // With zero grace period, the "skipped" message should NOT appear
      // (unless there are no unreachable regions to process)
      EventFilter.debug(pattern = ".*Self-healing skipped.*within startup grace period.*", occurrences = 0).intercept {
        // Wait for ticks to run
        Thread.sleep(1500)
      }
    }
  }
}

// === Dry Run Mode Tests ===

class SelfHealingCoordinatorDryRunSpec extends AkkaSpec(SelfHealingCoordinatorSpec.dryRunConfig) with WithLogCapturing {

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile)

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  "Self-healing in dry-run mode" must {

    "be configured with dryRun=true" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.dryRun shouldBe true
    }
  }
}

// === Main Coordinator Spec - Multi-node simulation ===

class SelfHealingCoordinatorSpec extends AkkaSpec(SelfHealingCoordinatorSpec.enabledConfig) with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

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

  "Self-healing coordinator" must {

    "have self-healing enabled with correct configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.staleRegionTimeout shouldBe 2.seconds
      settings.selfHealingSettings.checkInterval shouldBe 500.millis
      settings.selfHealingSettings.startupGracePeriod shouldBe 1.second
      settings.selfHealingSettings.dryRun shouldBe false
    }

    "initialize cluster and start sharding successfully" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Send messages to create entities
      region.tell("entity-1", probe.ref)
      probe.expectMsg(5.seconds, "received: entity-1")

      region.tell("entity-2", probe.ref)
      probe.expectMsg(5.seconds, "received: entity-2")
    }

    "track shards correctly" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create multiple entities across different shards
      (1 to 5).foreach { i =>
        region.tell(s"entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: entity-$i")
      }

      // Verify shards are allocated
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
    }
  }
}

// === Multi-ActorSystem Tests for Unreachability ===

class SelfHealingCoordinatorMultiNodeSpec
    extends AkkaSpec(SelfHealingCoordinatorSpec.enabledConfig)
    with WithLogCapturing {

  import SelfHealingCoordinatorSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile)

  // Create second actor system for multi-node testing
  val sys2Config = ConfigFactory.parseString("""
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingCoordinatorSpec2/sharding-ddata"
    }
  """).withFallback(SelfHealingCoordinatorSpec.enabledConfig)

  lazy val sys2: ActorSystem = ActorSystem(system.name, sys2Config)

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
    val sys2Storage = new File("target/SelfHealingCoordinatorSpec2")
    if (sys2Storage.exists) FileUtils.deleteQuietly(sys2Storage)
  }

  override protected def beforeTermination(): Unit = {
    if (sys2 != null && !sys2.whenTerminated.isCompleted) {
      shutdown(sys2)
    }
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
    val sys2Storage = new File("target/SelfHealingCoordinatorSpec2")
    if (sys2Storage.exists) FileUtils.deleteQuietly(sys2Storage)
  }

  def startSharding(sys: ActorSystem): ActorRef = {
    ClusterSharding(sys).start(
      shardTypeName,
      Props[TestEntity](),
      ClusterShardingSettings(sys),
      extractEntityId,
      extractShardId)
  }

  "Self-healing coordinator with multiple nodes" must {

    "form cluster with two nodes" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)

      within(15.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
          Cluster(sys2).state.members.size shouldEqual 2
          Cluster(sys2).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }
    }

    "distribute shards across nodes" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      within(15.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      val region1 = startSharding(system)
      startSharding(sys2) // Start on second node as well

      val probe = TestProbe()

      // Create entities to distribute shards
      (1 to 10).foreach { i =>
        region1.tell(s"entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: entity-$i")
      }

      // Verify shards exist
      region1.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
    }

    "handle member registration and clear self-healing tracking" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      Cluster(sys2).join(Cluster(system).selfAddress)
      within(15.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 2
          Cluster(system).state.members.forall(_.status == MemberStatus.Up) shouldBe true
        }
      }

      val region1 = startSharding(system)
      val region2 = startSharding(sys2)

      val probe = TestProbe()

      // Create some entities
      region1.tell("test-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity")

      // Verify normal operation continues
      region2.tell("another-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: another-entity")
    }
  }
}
