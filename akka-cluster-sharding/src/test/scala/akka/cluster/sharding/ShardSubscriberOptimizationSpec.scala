/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.actor.ExtendedActorSystem
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.Replicator._
import akka.cluster.sharding.ShardCoordinator.Internal._
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit._

object ShardSubscriberOptimizationSpec {
  val config = ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.updating-state-timeout = 60s
    akka.cluster.sharding.rebalance-interval = 120s
    akka.cluster.sharding.shard-start-timeout = 120s
    akka.cluster.sharding.handoff-timeout = 10s
    akka.cluster.min-nr-of-members = 1
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

  case class Fixture(coordinator: ActorRef, replicatorProbe: TestProbe, strategy: TestAllocationStrategy)
}

/**
 * Tests for the shardSubscribers optimization: BeginHandOff is sent to only
 * the regions that have the shard's location cached, rather than broadcasting
 * to every region in the cluster.
 */
class ShardSubscriberOptimizationSpec extends AkkaSpec(ShardSubscriberOptimizationSpec.config) with WithLogCapturing {

  import ShardSubscriberOptimizationSpec._

  private type CoordinatorUpdate = Update[LWWRegister[State]]
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

    // fake initial state from replicator
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

  "shardSubscribers optimization" must {

    "scope BeginHandOff to only regions that have requested the shard location" in
    withFixture(regionCount = 3) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)
      val regionC = regions(2)

      // s1 allocated to regionA; regionB is the requester and becomes a subscriber
      strategy.targetRegion = Some(regionA.ref)
      coordinator.tell(GetShardHome("s1"), regionB.ref)
      completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      regionB.expectMsgType[ShardHome](5.seconds)

      // regionC never requests GetShardHome for s1

      // Initiate graceful shutdown of regionA — triggers BeginHandOff fan-out
      coordinator.tell(GracefulShutdownReq(regionA.ref), regionA.ref)

      // Only regionB (a subscriber) should receive BeginHandOff; regionC should not
      regionB.expectMsg(3.seconds, BeginHandOff("s1"))
      regionC.expectNoMessage(500.millis)
    }

    "include a region that learns the shard location via informAboutCurrentShards on registration" in
    withFixture(regionCount = 2) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)

      // s1 allocated to regionA; regionB becomes a subscriber via the initial request
      strategy.targetRegion = Some(regionA.ref)
      coordinator.tell(GetShardHome("s1"), regionB.ref)
      completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      regionB.expectMsgType[ShardHome](5.seconds)

      // regionC registers after s1 is allocated: the coordinator sends it ShardHomes
      // (via informAboutCurrentShards), which should add it to the subscriber set
      val regionC = TestProbe()
      coordinator.tell(Register(regionC.ref), regionC.ref)
      // informAboutCurrentShards fires before the DData update, so ShardHomes arrives first
      regionC.expectMsgType[ShardHomes](5.seconds)
      completeNextUpdateExpecting[ShardRegionRegistered](replicatorProbe)
      regionC.expectMsgType[RegisterAck](5.seconds)

      // Initiate graceful shutdown of regionA
      coordinator.tell(GracefulShutdownReq(regionA.ref), regionA.ref)

      // Both regionB (GetShardHome subscriber) and regionC (informAboutCurrentShards subscriber)
      // should receive BeginHandOff
      regionB.expectMsg(3.seconds, BeginHandOff("s1"))
      regionC.expectMsg(3.seconds, BeginHandOff("s1"))
    }

    "deliver BeginHandOff to a second region that requests the shard location after initial allocation" in
    withFixture(regionCount = 3) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)
      val regionC = regions(2)

      // s1 allocated to regionA via regionB's request
      strategy.targetRegion = Some(regionA.ref)
      coordinator.tell(GetShardHome("s1"), regionB.ref)
      completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      regionB.expectMsgType[ShardHome](5.seconds)

      // regionC also requests the location later — already allocated, no DData update needed
      coordinator.tell(GetShardHome("s1"), regionC.ref)
      regionC.expectMsg(3.seconds, ShardHome("s1", regionA.ref))

      // Initiate graceful shutdown of regionA
      coordinator.tell(GracefulShutdownReq(regionA.ref), regionA.ref)

      // Both B and C are subscribers and should receive BeginHandOff
      regionB.expectMsg(3.seconds, BeginHandOff("s1"))
      regionC.expectMsg(3.seconds, BeginHandOff("s1"))
    }

    "prune a terminated region from subscriber sets before BeginHandOff fan-out" in
    withFixture(regionCount = 3) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)
      val regionC = regions(2)

      // Both regionB and regionC subscribe to s1
      strategy.targetRegion = Some(regionA.ref)
      coordinator.tell(GetShardHome("s1"), regionB.ref)
      completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)
      regionB.expectMsgType[ShardHome](5.seconds)

      coordinator.tell(GetShardHome("s1"), regionC.ref)
      regionC.expectMsg(3.seconds, ShardHome("s1", regionA.ref))

      // regionC terminates, coordinator should prune it from shardSubscribers
      watch(regionC.ref)
      system.stop(regionC.ref)
      expectTerminated(regionC.ref, 5.seconds)
      Thread.sleep(200) // allow DeathWatch to deliver Terminated to coordinator
      completeNextUpdateExpecting[ShardRegionTerminated](replicatorProbe)

      // Initiate graceful shutdown of regionA
      coordinator.tell(GracefulShutdownReq(regionA.ref), regionA.ref)

      // regionB (still alive subscriber) receives BeginHandOff
      regionB.expectMsg(3.seconds, BeginHandOff("s1"))
      // The handoff proceeds to completion without stalling on the dead regionC:
      // regionB acks, HandOff goes to regionA, which replies ShardStopped
      regionB.reply(BeginHandOffAck("s1"))
      regionA.fishForMessage(3.seconds, "HandOff") { case HandOff("s1") => true; case _ => false }
    }

    // Regression test: shards allocated via the coordinator's internal ignoreRef self-tells
    // (rememberEntities, regionTerminated recovery, post-handoff re-allocation) must not get
    // ignoreRef added as a subscriber.  If they did, BeginHandOff would be sent only to ignoreRef,
    // which discards the message, causing the handoff to time out and entities never to stop.
    "fall back to full BeginHandOff fan-out for shards allocated via ignoreRef self-tells" in
    withFixture(regionCount = 2) { (f, regions) =>
      val Fixture(coordinator, replicatorProbe, strategy) = f
      val regionA = regions(0)
      val regionB = regions(1)

      // Simulate an internal coordinator self-tell (as done by allocateShardHomesForRememberEntities,
      // regionTerminated recovery, etc.) by using the same ignoreRef the coordinator uses internally.
      // The shard must NOT end up tracked with ignoreRef as its only subscriber.
      val ignoreRef = system.asInstanceOf[ExtendedActorSystem].provider.ignoreRef
      strategy.targetRegion = Some(regionA.ref)
      coordinator.tell(GetShardHome("s1"), ignoreRef)
      completeNextUpdateExpecting[ShardHomeAllocated](replicatorProbe)

      // Trigger graceful shutdown of regionA
      coordinator.tell(GracefulShutdownReq(regionA.ref), regionA.ref)

      // Both regions must receive BeginHandOff — the full fan-out fallback applies because
      // the shard was allocated with ignoreRef and is therefore not in shardSubscribers.
      // regionA also receives HostShard during allocation, so fish past it.
      regionA.fishForMessage(3.seconds, "BeginHandOff(s1)") { case BeginHandOff("s1") => true; case _ => false }
      regionB.expectMsg(3.seconds, BeginHandOff("s1"))
    }
  }
}
