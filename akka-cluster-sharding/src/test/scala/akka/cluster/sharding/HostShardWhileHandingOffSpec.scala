/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardCoordinator.Internal.BeginHandOff
import akka.cluster.sharding.ShardCoordinator.Internal.BeginHandOffAck
import akka.cluster.sharding.ShardCoordinator.Internal.HandOff
import akka.cluster.sharding.ShardCoordinator.Internal.HostShard
import akka.cluster.sharding.ShardCoordinator.Internal.ShardStarted
import akka.cluster.sharding.ShardCoordinator.Internal.ShardStopped
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.ShardRegion.GetShardRegionState
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object HostShardWhileHandingOffSpec {

  def config =
    ConfigFactory.parseString("""
        akka.loglevel = DEBUG
        akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
        akka.actor.provider = "cluster"
        akka.remote.artery.canonical.port = 0
        akka.remote.artery.canonical.hostname = "127.0.0.1"
        akka.test.single-expect-default = 5 s
        akka.cluster.sharding.distributed-data.durable.keys = []
        akka.cluster.sharding.remember-entities = off
        akka.cluster.sharding.verbose-debug-logging = on
        akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
        akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
        akka.cluster.jmx.enabled = off
        """)

  val shardTypeName = "host-shard-handoff-entities"
  val numberOfShards = 2

  // The entity ignores this, so the hand off only completes when the test stops the entity.
  case object StopEntity

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
    case _        => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int                    => (msg % numberOfShards).toString
    case ShardRegion.StartEntity(id) => (id.toLong % numberOfShards).toString
    case _                           => throw new IllegalArgumentException()
  }

  class EntityActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case StopEntity =>
        log.debug("ignoring stop message")
      case _ =>
        sender() ! context.self
    }
  }

  // Never rebalances, so the coordinator leaves the shards alone while the test drives the
  // hand off itself. allocateShard always assigns to the first known region (there is one).
  class NoRebalanceAllocationStrategy extends ShardAllocationStrategy {

    override def allocateShard(
        requester: ActorRef,
        shardId: ShardRegion.ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
      Future.successful(currentShardAllocations.keys.head)

    override def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
      Future.successful(Set.empty)
  }
}

// The coordinator re-allocates a shard as soon as its hand off is done, which can reach the region
// before the region has seen the old shard actor terminate. The region must still end up hosting
// the shard. The hand off is driven from the test instead of the coordinator so that the window is
// held open for as long as the test needs, rather than raced for.
class HostShardWhileHandingOffSpec extends AkkaSpec(HostShardWhileHandingOffSpec.config) with WithLogCapturing {
  import HostShardWhileHandingOffSpec._

  private val region = ClusterSharding(system).start(
    shardTypeName,
    Props[EntityActor](),
    ClusterShardingSettings(system),
    extractEntityId,
    extractShardId,
    new NoRebalanceAllocationStrategy,
    StopEntity)

  private val probe = TestProbe()

  private def shardsInRegion(): Map[String, Int] = {
    region.tell(GetShardRegionState, probe.ref)
    probe.expectMsgType[CurrentShardRegionState].shards.map(s => s.shardId -> s.entityIds.size).toMap
  }

  "A ShardRegion receiving HostShard while the shard is handing off" must {

    "form a single-node cluster" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)
    }

    "start the shard once the previous shard actor has stopped" in {
      region.tell(0, probe.ref) // -> shard "0"
      val entity0 = probe.expectMsgType[ActorRef]

      probe.awaitAssert(shardsInRegion() should ===(Map("0" -> 1)), 5.seconds)

      // hand off shard "0", as the coordinator does for a rebalance
      region.tell(BeginHandOff("0"), probe.ref)
      probe.expectMsg(BeginHandOffAck("0"))
      region.tell(HandOff("0"), probe.ref)

      // The entity ignores StopEntity, so the shard actor is still alive and handing off here.
      // This is where the coordinator's eager re-allocation lands.
      region.tell(HostShard("0"), probe.ref)

      // the region does not host the shard yet, so it must not claim that it does
      probe.expectNoMessage(1.second)

      // let the hand off complete
      entity0 ! PoisonPill

      // the shard is started, and only then acked, when the previous shard actor has stopped
      probe.expectMsgAllOf(10.seconds, ShardStopped("0"), ShardStarted("0"))

      probe.awaitAssert(shardsInRegion() should ===(Map("0" -> 0)), 5.seconds)
    }

    "not lose entity messages that arrive in the same window" in {
      region.tell(0, probe.ref) // -> entity in shard "0"
      val entity0 = probe.expectMsgType[ActorRef]

      region.tell(BeginHandOff("0"), probe.ref)
      probe.expectMsg(BeginHandOffAck("0"))
      region.tell(HandOff("0"), probe.ref)
      region.tell(HostShard("0"), probe.ref)

      // sent while the region has no usable shard actor for "0", so it has to be buffered
      val entityProbe = TestProbe()
      region.tell(0, entityProbe.ref)

      entity0 ! PoisonPill
      probe.expectMsgAllOf(10.seconds, ShardStopped("0"), ShardStarted("0"))

      // the buffered message reaches an entity in the restarted shard
      entityProbe.expectMsgType[ActorRef](10.seconds)
      probe.awaitAssert(shardsInRegion() should contain("0" -> 1), 5.seconds)
    }

    "ack a repeated HostShard without restarting the shard" in {
      region.tell(1, probe.ref) // -> entity in shard "1", allocated by the coordinator
      probe.expectMsgType[ActorRef]
      probe.awaitAssert(shardsInRegion() should contain("1" -> 1), 5.seconds)

      // a request for a shard that is already hosted is acked right away and the shard is left alone
      region.tell(HostShard("1"), probe.ref)
      probe.expectMsg(ShardStarted("1"))
      shardsInRegion() should contain("1" -> 1)
    }
  }
}
