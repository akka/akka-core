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

object DDataCoordinatorTerminatedSpec {
  val config = ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.updating-state-timeout = 60s
    akka.cluster.sharding.rebalance-interval = 120s
    akka.cluster.sharding.shard-start-timeout = 120s
    akka.cluster.min-nr-of-members = 1
  """)

  /**
   * Simple allocation strategy that routes to a configurable target region.
   * Falls back to picking the region with fewest shards.
   */
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

/**
 * Integration tests for DDataShardCoordinator's Terminated handling.
 *
 * Each test uses a mock ddata replicator (TestProbe) for full control over when
 * ddata updates complete, allowing precise simulation of Terminated messages
 * arriving during the waitingForUpdate state.
 */
class DDataCoordinatorTerminatedSpec extends AkkaSpec(DDataCoordinatorTerminatedSpec.config) with WithLogCapturing {

  import DDataCoordinatorTerminatedSpec._

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

  /**
   * Create a coordinator with `regionCount` registered regions, run the test
   * body, and stop the coordinator on exit.
   */
  private def withFixture(regionCount: Int)(body: (Fixture, IndexedSeq[TestProbe]) => Unit): Unit = {
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

    val regions = (1 to regionCount).map { _ =>
      val region = TestProbe()
      coordinator.tell(Register(region.ref), region.ref)
      completeNextUpdate(replicatorProbe)
      region.expectMsgType[RegisterAck](5.seconds)
      region
    }

    try body(Fixture(coordinator, replicatorProbe, strategy), regions)
    finally system.stop(coordinator)
  }

  /** Complete the next pending ddata Update and return the raw event. */
  private def completeNextUpdate(replicatorProbe: TestProbe): DomainEvent = {
    val update = replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)
    val evt = update.request.get.asInstanceOf[DomainEvent]
    replicatorProbe.reply(UpdateSuccess(update.key, update.request))
    evt
  }

  /** Complete the next pending ddata Update, assert the event type, and return it typed. */
  private def completeNextUpdateExpecting[T <: DomainEvent: ClassTag](replicatorProbe: TestProbe): T = {
    val evt = completeNextUpdate(replicatorProbe)
    evt shouldBe a[T]
    evt.asInstanceOf[T]
  }

  /** Wait for the next ddata Update without completing it. */
  private def interceptNextUpdate(replicatorProbe: TestProbe): CoordinatorUpdate =
    replicatorProbe.expectMsgType[CoordinatorUpdate](5.seconds)

  /** Complete a previously intercepted Update. */
  private def completeInterceptedUpdate(replicatorProbe: TestProbe, update: CoordinatorUpdate): Unit =
    replicatorProbe.reply(UpdateSuccess(update.key, update.request))

  /** Allocate a shard and complete the ddata update. */
  private def allocateShard(coordinator: ActorRef, shardId: String, replicatorProbe: TestProbe): ShardHome = {
    val probe = TestProbe()
    coordinator.tell(GetShardHome(shardId), probe.ref)
    completeNextUpdate(replicatorProbe)
    probe.expectMsgType[ShardHome](5.seconds)
  }

  /** Stop an actor and wait for its Terminated to be dispatched to the coordinator. */
  private def stopAndWaitForTerminated(ref: ActorRef): Unit = {
    watch(ref)
    system.stop(ref)
    expectTerminated(ref, 5.seconds)
    Thread.sleep(200) // allow DeathWatch to deliver Terminated to coordinator
  }

  /**
   * Construct a ShardCoordinator.RebalanceDone message via reflection
   * (it is private to the ShardCoordinator object).
   */
  private def createRebalanceDone(shard: String, ok: Boolean): Any = {
    val clazz = Class.forName("akka.cluster.sharding.ShardCoordinator$RebalanceDone")
    val ctor = clazz.getDeclaredConstructors.head
    ctor.setAccessible(true)
    ctor.newInstance(shard, java.lang.Boolean.valueOf(ok))
  }

  "DDataShardCoordinator Terminated handling" must {

    "process Terminated during ddata update -- not silently dropped" in
    withFixture(regionCount = 2) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)

      // Allocate s1, s2 on regionA
      strategy.targetRegion = Some(regionA.ref)
      allocateShard(coordinator, "s1", replicatorProbe)
      allocateShard(coordinator, "s2", replicatorProbe)

      // Start s3 allocation on regionB -- enter waitingForUpdate
      strategy.targetRegion = Some(regionB.ref)
      val s3Probe = TestProbe()
      coordinator.tell(GetShardHome("s3"), s3Probe.ref)
      val pendingUpdate = interceptNextUpdate(replicatorProbe)

      // Kill regionA while coordinator is busy with ddata
      stopAndWaitForTerminated(regionA.ref)

      // Complete s3 -> coordinator unstashes Terminated(regionA)
      completeInterceptedUpdate(replicatorProbe, pendingUpdate)
      s3Probe.expectMsgType[ShardHome](5.seconds)

      val terminated = completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)
      terminated.region should ===(regionA.ref)

      // Verify s1 is re-allocated to regionB (not stuck on dead regionA)
      strategy.targetRegion = Some(regionB.ref)
      val verifyProbe = TestProbe()
      coordinator.tell(GetShardHome("s1"), verifyProbe.ref)
      val alloc = completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      alloc.region should ===(regionB.ref)
      verifyProbe.expectMsgType[ShardHome](5.seconds).ref should ===(regionB.ref)
    }

    "give Terminated priority over stashed RebalanceDone during waitingForUpdate" in
    withFixture(regionCount = 2) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)

      // Allocate s1, s2 on regionA
      strategy.targetRegion = Some(regionA.ref)
      allocateShard(coordinator, "s1", replicatorProbe)
      allocateShard(coordinator, "s2", replicatorProbe)

      // Start s3 allocation on regionB -- enter waitingForUpdate
      strategy.targetRegion = Some(regionB.ref)
      val s3Probe = TestProbe()
      coordinator.tell(GetShardHome("s3"), s3Probe.ref)
      val pendingUpdate = interceptNextUpdate(replicatorProbe)

      // RebalanceDone("s2") during waitingForUpdate -> stashed at tail
      coordinator.tell(createRebalanceDone("s2", true), ActorRef.noSender)
      // Kill regionA -> Terminated stashed at head (before RebalanceDone)
      stopAndWaitForTerminated(regionA.ref)

      // Complete s3 -> unstash: Terminated first, then RebalanceDone
      completeInterceptedUpdate(replicatorProbe, pendingUpdate)
      s3Probe.expectMsgType[ShardHome](5.seconds)

      // Terminated(regionA) processed first -- removes s2 from state
      val terminated = completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)
      terminated.region should ===(regionA.ref)

      // RebalanceDone("s2") finds s2 already gone -> no redundant ddata write
      replicatorProbe.expectNoMessage(500.millis)
    }

    "handle multiple Terminated messages during a single waitingForUpdate" in
    withFixture(regionCount = 3) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)
      val regionC = regions(2)

      // Allocate s1, s2 on regionA; s3, s4 on regionB
      strategy.targetRegion = Some(regionA.ref)
      allocateShard(coordinator, "s1", replicatorProbe)
      allocateShard(coordinator, "s2", replicatorProbe)
      strategy.targetRegion = Some(regionB.ref)
      allocateShard(coordinator, "s3", replicatorProbe)
      allocateShard(coordinator, "s4", replicatorProbe)

      // Start s5 allocation on regionC -- enter waitingForUpdate
      strategy.targetRegion = Some(regionC.ref)
      val s5Probe = TestProbe()
      coordinator.tell(GetShardHome("s5"), s5Probe.ref)
      val pendingUpdate = interceptNextUpdate(replicatorProbe)

      // Kill both regionA and regionB during waitingForUpdate
      stopAndWaitForTerminated(regionA.ref)
      stopAndWaitForTerminated(regionB.ref)

      // Complete s5 -> both Terminated messages unstashed
      completeInterceptedUpdate(replicatorProbe, pendingUpdate)
      s5Probe.expectMsgType[ShardHome](5.seconds)

      // Both regions should be terminated -- neither was lost
      val evt1 = completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)
      val evt2 = completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)
      Set(evt1.region, evt2.region) should ===(Set(regionA.ref, regionB.ref))
    }

    "handle Terminated for region with zero shards via stashAtHead" in
    withFixture(regionCount = 2) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)

      // Allocate s1 on regionA only -- regionB has zero shards
      strategy.targetRegion = Some(regionA.ref)
      allocateShard(coordinator, "s1", replicatorProbe)

      // Start s2 allocation on regionA -- enter waitingForUpdate
      val s2Probe = TestProbe()
      coordinator.tell(GetShardHome("s2"), s2Probe.ref)
      val pendingUpdate = interceptNextUpdate(replicatorProbe)

      // Kill regionB (0 shards) -> stashed at head (known region)
      stopAndWaitForTerminated(regionB.ref)

      // Complete s2 -> Terminated(regionB) unstashed
      completeInterceptedUpdate(replicatorProbe, pendingUpdate)
      s2Probe.expectMsgType[ShardHome](5.seconds)

      val terminated = completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)
      terminated.region should ===(regionB.ref)

      // regionA and its shards are unaffected
      val verifyProbe = TestProbe()
      coordinator.tell(GetShardHome("s1"), verifyProbe.ref)
      verifyProbe.expectMsgType[ShardHome](5.seconds).ref should ===(regionA.ref)
    }

    "process messages normally when no Terminated arrives during waitingForUpdate" in
    withFixture(regionCount = 1) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)

      strategy.targetRegion = Some(regionA.ref)
      allocateShard(coordinator, "s1", replicatorProbe)

      // Start s2 allocation -- enter waitingForUpdate
      val s2Probe = TestProbe()
      coordinator.tell(GetShardHome("s2"), s2Probe.ref)
      val pendingUpdate = interceptNextUpdate(replicatorProbe)

      // Send GetShardHome("s3") while in waitingForUpdate -> stashed separately
      val s3Probe = TestProbe()
      coordinator.tell(GetShardHome("s3"), s3Probe.ref)

      // Complete s2 -> s3 unstashed and allocated
      completeInterceptedUpdate(replicatorProbe, pendingUpdate)
      s2Probe.expectMsgType[ShardHome](5.seconds)

      val alloc = completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      alloc.shard should ===("s3")
      s3Probe.expectMsgType[ShardHome](5.seconds)

      // Verify s1 is still correctly allocated
      val verifyProbe = TestProbe()
      coordinator.tell(GetShardHome("s1"), verifyProbe.ref)
      val response = verifyProbe.expectMsgType[ShardHome](5.seconds)
      response.shard should ===("s1")
      response.ref should ===(regionA.ref)
    }
  }
}
