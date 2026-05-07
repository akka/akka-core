/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.testkit._

abstract class ClusterShardingRegionTerminationSpecConfig(mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig = """
      akka.cluster.downing-provider-class = ""
      akka.cluster.failure-detector.acceptable-heartbeat-pause = 1s
      akka.persistence.journal.leveldb-shared.store.native = off
      # we have 5 nodes, and one becomes unreachable
      akka.cluster.sharding.coordinator-state {
        write-majority-plus = 0
        read-majority-plus = 0
      }
      akka.cluster.sharding.distributed-data.majority-min-cap = 4
      """) {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  testTransport(on = true)

}

object PersistentClusterShardingRegionTerminationSpecConfig
    extends ClusterShardingRegionTerminationSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingRegionTerminationSpecConfig
    extends ClusterShardingRegionTerminationSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingRegionTerminationSpec
    extends ClusterShardingRegionTerminationSpec(PersistentClusterShardingRegionTerminationSpecConfig)
class DDataClusterShardingRegionTerminationSpec
    extends ClusterShardingRegionTerminationSpec(DDataClusterShardingRegionTerminationSpecConfig)

class PersistentClusterShardingRegionTerminationMultiJvmNode1 extends PersistentClusterShardingRegionTerminationSpec
class PersistentClusterShardingRegionTerminationMultiJvmNode2 extends PersistentClusterShardingRegionTerminationSpec
class PersistentClusterShardingRegionTerminationMultiJvmNode3 extends PersistentClusterShardingRegionTerminationSpec
class PersistentClusterShardingRegionTerminationMultiJvmNode4 extends PersistentClusterShardingRegionTerminationSpec
class PersistentClusterShardingRegionTerminationMultiJvmNode5 extends PersistentClusterShardingRegionTerminationSpec

class DDataClusterShardingRegionTerminationMultiJvmNode1 extends DDataClusterShardingRegionTerminationSpec
class DDataClusterShardingRegionTerminationMultiJvmNode2 extends DDataClusterShardingRegionTerminationSpec
class DDataClusterShardingRegionTerminationMultiJvmNode3 extends DDataClusterShardingRegionTerminationSpec
class DDataClusterShardingRegionTerminationMultiJvmNode4 extends DDataClusterShardingRegionTerminationSpec
class DDataClusterShardingRegionTerminationMultiJvmNode5 extends DDataClusterShardingRegionTerminationSpec

// This is a reproducer, and future test coverage, of #32756
abstract class ClusterShardingRegionTerminationSpec(multiNodeConfig: ClusterShardingRegionTerminationSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import multiNodeConfig._

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
    case _        => throw new IllegalArgumentException()
  }

  // each shardId same as entityId
  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int                    => msg.toString
    case ShardRegion.StartEntity(id) => id
    case _                           => throw new IllegalArgumentException()
  }

  def join(from: RoleName, to: RoleName): Unit = {
    join(
      from,
      to,
      startSharding(
        system,
        typeName = "Entity",
        entityProps = TestActors.echoActorProps,
        extractEntityId = extractEntityId,
        extractShardId = extractShardId))
  }

  lazy val region = ClusterSharding(system).shardRegion("Entity")

  s"Cluster sharding (${multiNodeConfig.mode})" must {

    "deallocate from terminated region" in within(30.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second))

      join(first, first)

      runOn(first) {
        region ! 1
        expectMsg(1)
        lastSender.path should be(region.path / "1" / "1")
      }
      enterBarrier("first-started")

      join(second, first)
      runOn(second) {
        region ! 2
        expectMsg(2)
        lastSender.path should be(region.path / "2" / "2")
        region ! 22
        expectMsg(22)
      }
      enterBarrier("second-started")

      join(third, first)
      join(fourth, first)
      join(fifth, first)
      runOn(third) {
        region ! 3
        expectMsg(3)
      }
      runOn(fourth) {
        region ! 4
        expectMsg(4)
      }
      runOn(fifth) {
        region ! 5
        expectMsg(5)
      }
      enterBarrier("more-started")

      // need the ref from other node
      val secondRegion =
        if (myself == second) {
          region
        } else {
          system.actorSelection(node(second) / "system" / "sharding" / "Entity") ! Identify(None)
          expectMsgType[ActorIdentity].ref.get
        }
      enterBarrier("lookup-second-region")

      // some shard allocations in flight
      runOn(third) {
        (6 to 20).foreach { n =>
          region ! n
        }
      }
      enterBarrier("requests-in-flight")

      runOn(first) {
        testConductor.blackhole(first, second, Direction.Both).await
        testConductor.blackhole(third, second, Direction.Both).await
        testConductor.blackhole(fourth, second, Direction.Both).await
        testConductor.blackhole(fifth, second, Direction.Both).await
      }
      enterBarrier("blackhole-second")

      // some more shard allocations in flight
      runOn(fourth, fifth) {
        (21 to 40).foreach { n =>
          region ! n
        }
      }

      runOn(first) {
        val secondAddress = node(second).address
        awaitAssert {
          cluster.state.unreachable.exists(m => m.address == secondAddress) shouldBe true
        }
        cluster.down(secondAddress)

        awaitAssert {
          val p = TestProbe()
          // shard 2 was already allocated at the beginning to second region
          // and should now be allocated elsewhere
          region.tell(2, p.ref)
          p.expectMsg(1.second, 2)
          lastSender.path should not be (secondRegion.path / "2" / "2")
        }
      }

      enterBarrier("second-deallocated")

      // verify all previous shards, and some new
      runOn(first, third, fourth, fifth) {
        val p = TestProbe()
        (1 to 50).foreach { n =>
          region.tell(n, p.ref)
          p.expectMsg(n)
          p.lastSender.path should not be (secondRegion.path / "2" / "2")
        }
      }

      enterBarrier("after")
    }

  }
}
