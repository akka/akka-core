/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import akka.actor._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.testkit._

/**
 * one-to-one mapping between shards and entities is not efficient but some use that anyway
 */
object ClusterShardingSingleShardPerEntitySpecConfig
    extends MultiNodeClusterShardingConfig(additionalConfig = "akka.cluster.sharding.updating-state-timeout = 1s") {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  testTransport(on = true)
}

class ClusterShardingSingleShardPerEntitySpecMultiJvmNode1 extends ClusterShardingSingleShardPerEntitySpec
class ClusterShardingSingleShardPerEntitySpecMultiJvmNode2 extends ClusterShardingSingleShardPerEntitySpec
class ClusterShardingSingleShardPerEntitySpecMultiJvmNode3 extends ClusterShardingSingleShardPerEntitySpec
class ClusterShardingSingleShardPerEntitySpecMultiJvmNode4 extends ClusterShardingSingleShardPerEntitySpec
class ClusterShardingSingleShardPerEntitySpecMultiJvmNode5 extends ClusterShardingSingleShardPerEntitySpec

abstract class ClusterShardingSingleShardPerEntitySpec
    extends MultiNodeClusterShardingSpec(ClusterShardingSingleShardPerEntitySpecConfig)
    with ImplicitSender {
  import ClusterShardingSingleShardPerEntitySpecConfig._
  import MultiNodeClusterShardingSpec.ShardedEntity

  def join(from: RoleName, to: RoleName): Unit = {
    join(
      from,
      to,
      startSharding(
        system,
        typeName = "Entity",
        entityProps = Props[ShardedEntity](),
        extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
        extractShardId = MultiNodeClusterShardingSpec.intExtractShardId))
  }

  lazy val region = ClusterSharding(system).shardRegion("Entity")

  def joinAndAllocate(node: RoleName, entityId: Int): Unit = {
    within(10.seconds) {
      join(node, first)
      runOn(node) {
        region ! entityId
        expectMsg(entityId)
        lastSender.path should be(region.path / s"$entityId" / s"$entityId")
      }
    }
    enterBarrier(s"started-$entityId")
  }

  s"Cluster sharding with single shard per entity" must {

    "use specified region" in {
      joinAndAllocate(first, 1)
      joinAndAllocate(second, 2)
      joinAndAllocate(third, 3)
      joinAndAllocate(fourth, 4)
      joinAndAllocate(fifth, 5)

      runOn(first) {
        // coordinator is on 'first', blackhole 3 other means that it can't update with WriteMajority
        testConductor.blackhole(first, third, Direction.Both).await
        testConductor.blackhole(first, fourth, Direction.Both).await
        testConductor.blackhole(first, fifth, Direction.Both).await

        // shard 6 not allocated yet and due to the blackhole it will not be completed
        region ! 6

        // shard 1 location is know by 'first' region, not involving coordinator
        region ! 1
        expectMsg(1)

        // shard 2 location not known at 'first' region yet, but coordinator is on 'first' and should
        // be able to answer GetShardHome even though previous request for shard 4 has not completed yet
        region ! 2
        expectMsg(2)
        lastSender.path should be(node(second) / "system" / "sharding" / "Entity" / "2" / "2")

        testConductor.passThrough(first, third, Direction.Both).await
        testConductor.passThrough(first, fourth, Direction.Both).await
        testConductor.passThrough(first, fifth, Direction.Both).await
        expectMsg(10.seconds, 6)
      }

      enterBarrier("after-1")
    }

  }
}
