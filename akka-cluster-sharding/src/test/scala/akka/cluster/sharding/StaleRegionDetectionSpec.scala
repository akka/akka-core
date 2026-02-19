/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.Replicator._
import akka.cluster.sharding.ShardCoordinator.Internal._
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit._

object StaleRegionDetectionSpec {

  def config(enabled: Boolean = true, dryRun: Boolean = false) = ConfigFactory.parseString(s"""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.updating-state-timeout = 60s
    akka.cluster.sharding.rebalance-interval = 120s
    akka.cluster.sharding.shard-start-timeout = 120s
    akka.cluster.min-nr-of-members = 1
    akka.cluster.sharding.stale-region-detection {
      enabled = $enabled
      check-interval = 500ms
      region-staleness-timeout = 1s
      startup-grace-period = 0s
      dry-run = $dryRun
    }
  """)

  class TestAllocationStrategy extends ShardCoordinator.ShardAllocationStrategy {
    @volatile var targetRegion: Option[ActorRef] = None

    override def allocateShard(
        requester: ActorRef,
        shardId: ShardId,
        currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]]): Future[ActorRef] =
      Future.successful(targetRegion.getOrElse(currentShardAllocations.minBy(_._2.size)._1))

    override def rebalance(
        currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]],
        rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] =
      Future.successful(Set.empty)
  }
}

class StaleRegionDetectionSpec extends AkkaSpec(StaleRegionDetectionSpec.config()) with WithLogCapturing {

  import StaleRegionDetectionSpec._

  private type CoordinatorUpdate = Update[LWWRegister[State]]

  private case class Fixture(coordinator: ActorRef, replicatorProbe: TestProbe, strategy: TestAllocationStrategy)

  private var testCounter = 0

  private def nextTypeName(): String = {
    testCounter += 1
    s"TestEntity$testCounter"
  }

  override def atStartup(): Unit = {
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert {
      cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1)
    }
  }

  private def createFixture(): Fixture = {
    val typeName = nextTypeName()
    val strategy = new TestAllocationStrategy
    val replicatorProbe = TestProbe()
    val coordinator = system.actorOf(
      ShardCoordinator.props(
        typeName,
        ClusterShardingSettings(system),
        strategy,
        replicatorProbe.ref,
        majorityMinCap = 0,
        rememberEntitiesStoreProvider = None))

    // Bootstrap: respond to the initial ddata Get with empty state
    replicatorProbe.expectMsgType[Get[_]](5.seconds)
    replicatorProbe.reply(NotFound(LWWRegisterKey[State](s"${typeName}CoordinatorState"), None))
    replicatorProbe.expectNoMessage(100.millis)

    Fixture(coordinator, replicatorProbe, strategy)
  }

  private def completeNextUpdate(replicatorProbe: TestProbe): DomainEvent = {
    val update = replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)
    val evt = update.request.get.asInstanceOf[DomainEvent]
    replicatorProbe.reply(UpdateSuccess(update.key, update.request))
    evt
  }

  private def completeNextUpdateExpecting[T <: DomainEvent: ClassTag](replicatorProbe: TestProbe): T = {
    val evt = completeNextUpdate(replicatorProbe)
    evt shouldBe a[T]
    evt.asInstanceOf[T]
  }

  private def registerRegion(coordinator: ActorRef, replicatorProbe: TestProbe): TestProbe = {
    val region = TestProbe()
    coordinator.tell(Register(region.ref), region.ref)
    completeNextUpdate(replicatorProbe)
    region.expectMsgType[RegisterAck](5.seconds)
    region
  }

  private def allocateShard(coordinator: ActorRef, shardId: String, replicatorProbe: TestProbe): ShardHome = {
    val probe = TestProbe()
    coordinator.tell(GetShardHome(shardId), probe.ref)
    completeNextUpdate(replicatorProbe)
    probe.expectMsgType[ShardHome](5.seconds)
  }

  "StaleRegionDetection" must {

    "detect and clean up a stale region after timeout" in {
      val f = createFixture()
      val Fixture(coordinator, replicatorProbe, strategy) = f

      try {
        val regionA = registerRegion(coordinator, replicatorProbe)

        strategy.targetRegion = Some(regionA.ref)
        allocateShard(coordinator, "s1", replicatorProbe)

        // Stop regionA without DeathWatch delivering Terminated to coordinator
        // (simulating the bug where Terminated is lost)
        watch(regionA.ref)
        system.stop(regionA.ref)
        expectTerminated(regionA.ref, 5.seconds)

        // Wait for stale region detection to kick in
        // startup-grace-period = 0s, check-interval = 500ms, staleness-timeout = 1s
        // So after ~1.5s the region should be detected as stale and cleaned up
        val terminated = replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)
        val evt = terminated.request.get.asInstanceOf[DomainEvent]
        evt shouldBe a[ShardRegionTerminated]
        evt.asInstanceOf[ShardRegionTerminated].region should ===(regionA.ref)
      } finally system.stop(coordinator)
    }

    "not act on regions whose member is still in the cluster" in {
      val f = createFixture()
      val Fixture(coordinator, replicatorProbe, strategy) = f

      try {
        val regionA = registerRegion(coordinator, replicatorProbe)

        strategy.targetRegion = Some(regionA.ref)
        allocateShard(coordinator, "s1", replicatorProbe)

        // regionA is local (same node), so its address hasLocalScope — never flagged as stale
        // Wait long enough for multiple check cycles
        replicatorProbe.expectNoMessage(3.seconds)
      } finally system.stop(coordinator)
    }
  }
}

class StaleRegionDetectionDryRunSpec
    extends AkkaSpec(StaleRegionDetectionSpec.config(enabled = true, dryRun = true))
    with WithLogCapturing {

  import StaleRegionDetectionSpec._

  private type CoordinatorUpdate = Update[LWWRegister[State]]

  private var testCounter = 100

  private def nextTypeName(): String = {
    testCounter += 1
    s"DryRunEntity$testCounter"
  }

  override def atStartup(): Unit = {
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert {
      cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1)
    }
  }

  private def completeNextUpdate(replicatorProbe: TestProbe): DomainEvent = {
    val update = replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)
    val evt = update.request.get.asInstanceOf[DomainEvent]
    replicatorProbe.reply(UpdateSuccess(update.key, update.request))
    evt
  }

  "StaleRegionDetection in dry-run mode" must {

    "log but not deallocate stale regions" in {
      val typeName = nextTypeName()
      val strategy = new TestAllocationStrategy
      val replicatorProbe = TestProbe()
      val coordinator = system.actorOf(
        ShardCoordinator.props(
          typeName,
          ClusterShardingSettings(system),
          strategy,
          replicatorProbe.ref,
          majorityMinCap = 0,
          rememberEntitiesStoreProvider = None))

      try {
        replicatorProbe.expectMsgType[Get[_]](5.seconds)
        replicatorProbe.reply(NotFound(LWWRegisterKey[State](s"${typeName}CoordinatorState"), None))
        replicatorProbe.expectNoMessage(100.millis)

        val region = TestProbe()
        coordinator.tell(Register(region.ref), region.ref)
        completeNextUpdate(replicatorProbe)
        region.expectMsgType[RegisterAck](5.seconds)

        strategy.targetRegion = Some(region.ref)
        val probe = TestProbe()
        coordinator.tell(GetShardHome("s1"), probe.ref)
        completeNextUpdate(replicatorProbe)
        probe.expectMsgType[ShardHome](5.seconds)

        // Stop the region
        watch(region.ref)
        system.stop(region.ref)
        expectTerminated(region.ref, 5.seconds)

        // In dry-run mode, no ShardRegionTerminated update should be sent
        // Wait longer than staleness-timeout + check-interval
        replicatorProbe.expectNoMessage(3.seconds)
      } finally system.stop(coordinator)
    }
  }
}

class StaleRegionDetectionDisabledSpec
    extends AkkaSpec(StaleRegionDetectionSpec.config(enabled = false))
    with WithLogCapturing {

  import StaleRegionDetectionSpec._

  private type CoordinatorUpdate = Update[LWWRegister[State]]

  override def atStartup(): Unit = {
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert {
      cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1)
    }
  }

  private def completeNextUpdate(replicatorProbe: TestProbe): DomainEvent = {
    val update = replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)
    val evt = update.request.get.asInstanceOf[DomainEvent]
    replicatorProbe.reply(UpdateSuccess(update.key, update.request))
    evt
  }

  "StaleRegionDetection when disabled" must {

    "not perform any stale region checks" in {
      val strategy = new TestAllocationStrategy
      val replicatorProbe = TestProbe()
      val coordinator = system.actorOf(
        ShardCoordinator.props(
          "DisabledEntity",
          ClusterShardingSettings(system),
          strategy,
          replicatorProbe.ref,
          majorityMinCap = 0,
          rememberEntitiesStoreProvider = None))

      try {
        replicatorProbe.expectMsgType[Get[_]](5.seconds)
        replicatorProbe.reply(NotFound(LWWRegisterKey[State]("DisabledEntityCoordinatorState"), None))
        replicatorProbe.expectNoMessage(100.millis)

        val region = TestProbe()
        coordinator.tell(Register(region.ref), region.ref)
        completeNextUpdate(replicatorProbe)
        region.expectMsgType[RegisterAck](5.seconds)

        strategy.targetRegion = Some(region.ref)
        val probe = TestProbe()
        coordinator.tell(GetShardHome("s1"), probe.ref)
        completeNextUpdate(replicatorProbe)
        probe.expectMsgType[ShardHome](5.seconds)

        // Stop region — with detection disabled, no cleanup should happen
        watch(region.ref)
        system.stop(region.ref)
        expectTerminated(region.ref, 5.seconds)

        // No stale region check should trigger
        replicatorProbe.expectNoMessage(3.seconds)
      } finally system.stop(coordinator)
    }
  }
}
