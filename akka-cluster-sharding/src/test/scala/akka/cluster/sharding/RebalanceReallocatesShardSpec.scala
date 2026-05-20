/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.util.concurrent.atomic.AtomicReference

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
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.ShardRegion.GetShardRegionState
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object RebalanceReallocatesShardSpec {

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
        akka.cluster.sharding.rebalance-interval = 500ms
        akka.cluster.sharding.verbose-debug-logging = on
        akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
        akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
        akka.cluster.jmx.enabled = off
        """)

  val shardTypeName = "rebalance-realloc-entities"
  val numberOfShards = 2

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
      case _ =>
        log.debug("ping")
        sender() ! context.self
    }
  }

  // Strategy with externally controllable rebalance set. allocateShard always assigns
  // to the first known region (a single-node test has exactly one).
  class ControllableAllocationStrategy(toRebalance: AtomicReference[Set[ShardRegion.ShardId]])
      extends ShardAllocationStrategy {

    override def allocateShard(
        requester: ActorRef,
        shardId: ShardRegion.ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
      Future.successful(currentShardAllocations.keys.head)

    override def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
      Future.successful(toRebalance.getAndSet(Set.empty))
  }
}

// Regression guard for the non-StopShards path through `ShardCoordinator.RebalanceDone`:
// after a normal rebalance, the deallocated shard must be eagerly re-allocated by the
// coordinator (via the synthetic `GetShardHome`), without waiting for an entity message.
// The symmetric "do not re-allocate after StopShards" behavior is covered by
// `ShardedDaemonProcessRescaleEmptyShardsSpec`.
class RebalanceReallocatesShardSpec extends AkkaSpec(RebalanceReallocatesShardSpec.config) with WithLogCapturing {
  import RebalanceReallocatesShardSpec._

  private val toRebalance = new AtomicReference[Set[ShardRegion.ShardId]](Set.empty)

  private val region = ClusterSharding(system).start(
    shardTypeName,
    Props[EntityActor](),
    ClusterShardingSettings(system),
    extractEntityId,
    extractShardId,
    new ControllableAllocationStrategy(toRebalance),
    PoisonPill)

  private val probe = TestProbe()

  "RebalanceDone for a normal rebalance" must {

    "form a single-node cluster" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status shouldEqual MemberStatus.Up, 5.seconds)
    }

    "eagerly re-allocate the deallocated shard without entity messages arriving" in {
      // start one entity in each of two shards
      region.tell(0, probe.ref) // -> shard "0"
      val entity0 = probe.expectMsgType[ActorRef]
      probe.watch(entity0)

      region.tell(1, probe.ref) // -> shard "1"
      val entity1 = probe.expectMsgType[ActorRef]
      probe.watch(entity1)

      probe.awaitAssert({
        region.tell(GetShardRegionState, probe.ref)
        val state = probe.expectMsgType[CurrentShardRegionState]
        state.shards.map(s => s.shardId -> s.entityIds.size).toMap should ===(Map("0" -> 1, "1" -> 1))
      }, 5.seconds)

      // trigger rebalance of shard "0" via the custom strategy on the next rebalance tick
      toRebalance.set(Set("0"))

      // entity in shard "0" is terminated as part of the handoff
      probe.expectTerminated(entity0, 10.seconds)

      // No further entity messages are sent. Shard "0" must reappear in the region's state
      // (with no entities) because the coordinator's RebalanceDone handler eagerly
      // re-allocates non-StopShards rebalances via a synthetic GetShardHome.
      probe.awaitAssert({
        region.tell(GetShardRegionState, probe.ref)
        val state = probe.expectMsgType[CurrentShardRegionState]
        val byShard = state.shards.map(s => s.shardId -> s.entityIds.size).toMap
        byShard.get("0") should ===(Some(0))
        // shard "1" remains as before with its original entity
        byShard.get("1") should ===(Some(1))
      }, 10.seconds)
    }
  }
}
