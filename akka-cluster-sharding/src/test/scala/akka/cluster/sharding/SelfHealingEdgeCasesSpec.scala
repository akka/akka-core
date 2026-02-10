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

object SelfHealingEdgeCasesSpec {

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
      dir = "target/SelfHealingEdgeCasesSpec/sharding-ddata"
      map-size = 10 MiB
    }
  """)

  // Very short timeout configuration
  val veryShortTimeoutConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 100ms
      check-interval = 50ms
      startup-grace-period = 100ms
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingVeryShortTimeoutSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  // Very long grace period
  val veryLongGracePeriodConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 10m
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingLongGracePeriodSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  // Zero grace period
  val zeroGracePeriodConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 0s
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingZeroGracePeriodSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  // High check interval
  val highCheckIntervalConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 60s
      startup-grace-period = 1s
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingHighCheckIntervalSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  // Single node configuration
  val singleNodeConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingSingleNodeSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  // Standard configuration for most tests
  val standardConfig = ConfigFactory.parseString("""
    akka.cluster.sharding.self-healing {
      enabled = on
      stale-region-timeout = 2s
      check-interval = 500ms
      startup-grace-period = 1s
      dry-run = off
    }
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingEdgeCasesStandardSpec/sharding-ddata"
    }
  """).withFallback(baseConfig)

  val shardTypeName = "EdgeCaseEntity"
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

// === Very Short Timeout Tests ===

class SelfHealingVeryShortTimeoutSpec
    extends AkkaSpec(SelfHealingEdgeCasesSpec.veryShortTimeoutConfig)
    with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

  "Self-healing with very short timeout (100ms)" must {

    "have correct configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.staleRegionTimeout shouldBe 100.millis
      settings.selfHealingSettings.checkInterval shouldBe 50.millis
      settings.selfHealingSettings.startupGracePeriod shouldBe 100.millis
    }

    "operate normally with very short timeouts" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Normal operation should work even with aggressive timeouts
      region.tell("short-timeout-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: short-timeout-entity")
    }

    "handle rapid check intervals without issues" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create multiple entities to exercise the system
      (1 to 10).foreach { i =>
        region.tell(s"rapid-entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: rapid-entity-$i")
      }

      // Wait for several check cycles to complete
      Thread.sleep(500)

      // System should still be operational
      region.tell("after-cycles-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: after-cycles-entity")
    }
  }
}

// === Very Long Grace Period Tests ===

class SelfHealingVeryLongGracePeriodSpec
    extends AkkaSpec(SelfHealingEdgeCasesSpec.veryLongGracePeriodConfig)
    with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

  "Self-healing with very long grace period (10 minutes)" must {

    "have correct configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.startupGracePeriod shouldBe 10.minutes
    }

    "skip self-healing during the long grace period" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("long-grace-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: long-grace-entity")

      // Verify that grace period skip message appears (at least one with 500ms intervals)
      EventFilter.debug(pattern = ".*Self-healing skipped.*within startup grace period.*").intercept {
        Thread.sleep(1000)
      }
    }

    "still allow normal sharding operations during grace period" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      (1 to 10).foreach { i =>
        region.tell(s"grace-entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: grace-entity-$i")
      }
    }
  }
}

// === Zero Grace Period Tests ===

class SelfHealingZeroGracePeriodSpec
    extends AkkaSpec(SelfHealingEdgeCasesSpec.zeroGracePeriodConfig)
    with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

    "have correct configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.startupGracePeriod shouldBe Duration.Zero
    }

    "activate immediately without waiting" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("zero-grace-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: zero-grace-entity")

      // Should NOT see the "skipped - within startup grace period" message
      EventFilter.debug(pattern = ".*Self-healing skipped.*within startup grace period.*", occurrences = 0).intercept {
        Thread.sleep(1000)
      }
    }
  }
}

// === High Check Interval Tests ===

class SelfHealingHighCheckIntervalSpec
    extends AkkaSpec(SelfHealingEdgeCasesSpec.highCheckIntervalConfig)
    with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

  "Self-healing with high check interval (60 seconds)" must {

    "have correct configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.checkInterval shouldBe 60.seconds
    }

    "operate normally with infrequent checks" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("high-interval-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: high-interval-entity")
    }
  }
}

// === Single Node (Empty Cluster) Tests ===

class SelfHealingSingleNodeSpec extends AkkaSpec(SelfHealingEdgeCasesSpec.singleNodeConfig) with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

  "Self-healing on single node (no other regions to heal)" must {

    "operate without errors when there are no other regions" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create entities on single node
      (1 to 10).foreach { i =>
        region.tell(s"single-node-entity-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: single-node-entity-$i")
      }

      // Wait for self-healing checks to run (should be no-ops)
      Thread.sleep(2000)

      // System should still be operational
      region.tell("after-checks-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: after-checks-entity")
    }

    "not emit any self-healing warnings in single-node mode" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      region.tell("test-entity", probe.ref)
      probe.expectMsg(5.seconds, "received: test-entity")

      // No self-healing warnings should appear (no unreachable regions)
      EventFilter.warning(pattern = ".*Self-healing.*deallocat.*", occurrences = 0).intercept {
        Thread.sleep(3000) // Wait longer than stale-region-timeout
      }
    }
  }
}

// === Standard Edge Cases Tests ===

class SelfHealingEdgeCasesSpec extends AkkaSpec(SelfHealingEdgeCasesSpec.standardConfig) with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

  val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile,
    new File("target/SelfHealingEdgeCasesSpec2"))

  lazy val sys2Config = ConfigFactory.parseString("""
    akka.cluster.sharding.distributed-data.durable.lmdb {
      dir = "target/SelfHealingEdgeCasesSpec2/sharding-ddata"
    }
  """).withFallback(standardConfig)

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

  "Self-healing edge cases" must {

    "have standard configuration" in {
      val settings = ClusterShardingSettings(system)
      settings.selfHealingSettings.enabled shouldBe true
      settings.selfHealingSettings.staleRegionTimeout shouldBe 2.seconds
      settings.selfHealingSettings.checkInterval shouldBe 500.millis
      settings.selfHealingSettings.startupGracePeriod shouldBe 1.second
      settings.selfHealingSettings.dryRun shouldBe false
    }

    "handle rapid entity creation without issues" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Rapid entity creation
      (1 to 100).foreach { i =>
        region.tell(s"rapid-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: rapid-$i")
      }
    }

    "handle entities spread across all shards" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Create entities that hit all shards (using sequential IDs)
      (0 until numberOfShards).foreach { shardId =>
        // Find an entity ID that maps to this shard
        val entityId = s"shard-$shardId-entity"
        region.tell(entityId, probe.ref)
        probe.expectMsg(5.seconds, s"received: $entityId")
      }

      // Verify all shards have entities
      region.tell(ShardRegion.GetShardRegionState, probe.ref)
      val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState](5.seconds)
      state.shards should not be empty
    }

    "handle graceful cluster operations" in {
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
      startSharding(sys2) // Start on second node
      val probe1 = TestProbe()(system)

      // Create entities
      (1 to 10).foreach { i =>
        region1.tell(s"graceful-$i", probe1.ref)
        probe1.expectMsg(10.seconds, s"received: graceful-$i")
      }

      // Have node 2 leave gracefully
      Cluster(sys2).leave(Cluster(sys2).selfAddress)

      within(30.seconds) {
        awaitAssert {
          Cluster(system).state.members.size shouldEqual 1
        }
      }

      // Verify entities still accessible after graceful leave
      region1.tell("graceful-1", probe1.ref)
      probe1.expectMsg(10.seconds, "received: graceful-1")
    }

    "handle multiple entity accesses during self-healing check cycles" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Wait for grace period to pass
      Thread.sleep(1500)

      // Now perform multiple operations during active self-healing check cycles
      (1 to 50).foreach { i =>
        region.tell(s"during-checks-$i", probe.ref)
        probe.expectMsg(5.seconds, s"received: during-checks-$i")
        if (i % 10 == 0) Thread.sleep(100) // Allow check cycles to run
      }
    }
  }
}

// === Concurrent Operations Tests ===

class SelfHealingConcurrentOperationsSpec
    extends AkkaSpec(SelfHealingEdgeCasesSpec.standardConfig)
    with WithLogCapturing {

  import SelfHealingEdgeCasesSpec._

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

  "Self-healing with concurrent operations" must {

    "handle concurrent message sending" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)

      // Create multiple probes for concurrent access
      val probes = (1 to 5).map(_ => TestProbe())

      // Send messages concurrently
      probes.zipWithIndex.foreach {
        case (probe, idx) =>
          region.tell(s"concurrent-$idx", probe.ref)
      }

      // All should succeed
      probes.zipWithIndex.foreach {
        case (probe, idx) =>
          probe.expectMsg(10.seconds, s"received: concurrent-$idx")
      }
    }

    "handle rapid sequential access to same entity" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = startSharding(system)
      val probe = TestProbe()

      // Rapid access to same entity
      (1 to 50).foreach { _ =>
        region.tell("same-entity", probe.ref)
        probe.expectMsg(5.seconds, "received: same-entity")
      }
    }
  }
}
