/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.testkit._
import akka.util.Timeout

object ClusterShardingCustomShardAllocationSpec {

  case object AllocateReq
  case class UseRegion(region: ActorRef)
  case object UseRegionAck
  case object RebalanceReq
  case class RebalanceShards(shards: Set[String])
  case object RebalanceShardsAck

  class Allocator extends Actor {
    var useRegion: Option[ActorRef] = None
    var rebalance = Set.empty[String]
    def receive = {
      case UseRegion(region) =>
        useRegion = Some(region)
        sender() ! UseRegionAck
      case AllocateReq =>
        useRegion.foreach { sender() ! _ }
      case RebalanceShards(shards) =>
        rebalance = shards
        sender() ! RebalanceShardsAck
      case RebalanceReq =>
        sender() ! rebalance
        rebalance = Set.empty
    }
  }

  case class TestAllocationStrategy(ref: ActorRef) extends ShardAllocationStrategy {
    implicit val timeout: Timeout = Timeout(3.seconds)
    override def allocateShard(
        requester: ActorRef,
        shardId: ShardRegion.ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {
      (ref ? AllocateReq).mapTo[ActorRef]
    }

    override def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {
      (ref ? RebalanceReq).mapTo[Set[String]]
    }
  }

}

abstract class ClusterShardingCustomShardAllocationSpecConfig(mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig = """
      akka.cluster.sharding.rebalance-interval = 1 s
      akka.persistence.journal.leveldb-shared.store.native = off
      """) {

  val first = role("first")
  val second = role("second")

}

object PersistentClusterShardingCustomShardAllocationSpecConfig
    extends ClusterShardingCustomShardAllocationSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingCustomShardAllocationSpecConfig
    extends ClusterShardingCustomShardAllocationSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingCustomShardAllocationSpec
    extends ClusterShardingCustomShardAllocationSpec(PersistentClusterShardingCustomShardAllocationSpecConfig)
class DDataClusterShardingCustomShardAllocationSpec
    extends ClusterShardingCustomShardAllocationSpec(DDataClusterShardingCustomShardAllocationSpecConfig)

class PersistentClusterShardingCustomShardAllocationMultiJvmNode1
    extends PersistentClusterShardingCustomShardAllocationSpec
class PersistentClusterShardingCustomShardAllocationMultiJvmNode2
    extends PersistentClusterShardingCustomShardAllocationSpec

class DDataClusterShardingCustomShardAllocationMultiJvmNode1 extends DDataClusterShardingCustomShardAllocationSpec
class DDataClusterShardingCustomShardAllocationMultiJvmNode2 extends DDataClusterShardingCustomShardAllocationSpec

abstract class ClusterShardingCustomShardAllocationSpec(multiNodeConfig: ClusterShardingCustomShardAllocationSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import ClusterShardingCustomShardAllocationSpec._
  import multiNodeConfig._

  def join(from: RoleName, to: RoleName): Unit = {
    join(
      from,
      to,
      startSharding(
        system,
        typeName = "Entity",
        entityProps = TestActors.echoActorProps,
        extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
        extractShardId = MultiNodeClusterShardingSpec.intExtractShardId,
        allocationStrategy = TestAllocationStrategy(allocator)))
  }

  lazy val region = ClusterSharding(system).shardRegion("Entity")

  lazy val allocator = system.actorOf(Props[Allocator](), "allocator")

  s"Cluster sharding (${multiNodeConfig.mode}) with custom allocation strategy" must {

    "use specified region" in within(30.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second))

      join(first, first)

      runOn(first) {
        allocator ! UseRegion(region)
        expectMsg(UseRegionAck)
        region ! 1
        expectMsg(1)
        lastSender.path should be(region.path / "1" / "1")
      }
      enterBarrier("first-started")

      join(second, first)

      region ! 2
      expectMsg(2)
      runOn(first) {
        lastSender.path should be(region.path / "2" / "2")
      }
      runOn(second) {
        lastSender.path should be(node(first) / "system" / "sharding" / "Entity" / "2" / "2")
      }
      enterBarrier("second-started")

      runOn(first) {
        system.actorSelection(node(second) / "system" / "sharding" / "Entity") ! Identify(None)
        val secondRegion = expectMsgType[ActorIdentity].ref.get
        allocator ! UseRegion(secondRegion)
        expectMsg(UseRegionAck)
      }
      enterBarrier("second-active")

      region ! 3
      expectMsg(3)
      runOn(second) {
        lastSender.path should be(region.path / "3" / "3")
      }
      runOn(first) {
        lastSender.path should be(node(second) / "system" / "sharding" / "Entity" / "3" / "3")
      }

      enterBarrier("after-2")
    }

    "rebalance specified shards" in within(15.seconds) {
      runOn(first) {
        allocator ! RebalanceShards(Set("2"))
        expectMsg(RebalanceShardsAck)

        awaitAssert {
          val p = TestProbe()
          region.tell(2, p.ref)
          p.expectMsg(2.second, 2)
          p.lastSender.path should be(node(second) / "system" / "sharding" / "Entity" / "2" / "2")
        }

        region ! 1
        expectMsg(1)
        lastSender.path should be(region.path / "1" / "1")
      }

      enterBarrier("after-2")
    }

  }
}
