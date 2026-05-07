/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor._
import akka.cluster.MemberStatus
import akka.testkit.{ ImplicitSender, TestProbe }

/**
 * Test for issue #28416
 */
object ClusterShardingRegistrationCoordinatedShutdownSpec extends MultiNodeClusterShardingConfig {

  val first = role("first")
  val second = role("second")
  val third = role("third")

}

class ClusterShardingRegistrationCoordinatedShutdownMultiJvmNode1
    extends ClusterShardingRegistrationCoordinatedShutdownSpec
class ClusterShardingRegistrationCoordinatedShutdownMultiJvmNode2
    extends ClusterShardingRegistrationCoordinatedShutdownSpec
class ClusterShardingRegistrationCoordinatedShutdownMultiJvmNode3
    extends ClusterShardingRegistrationCoordinatedShutdownSpec

abstract class ClusterShardingRegistrationCoordinatedShutdownSpec
    extends MultiNodeClusterShardingSpec(ClusterShardingRegistrationCoordinatedShutdownSpec)
    with ImplicitSender {

  import ClusterShardingRegistrationCoordinatedShutdownSpec._
  import MultiNodeClusterShardingSpec.ShardedEntity

  private lazy val region = ClusterSharding(system).shardRegion("Entity")

  s"Region registration during CoordinatedShutdown" must {

    "try next oldest" in within(30.seconds) {
      // second should be oldest
      join(second, second)
      join(first, second)
      join(third, second)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) should ===(3)
      }

      val csTaskDone = TestProbe()
      runOn(third) {
        CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeClusterShutdown, "test")(() => {
          Thread.sleep(200)
          region ! 1
          expectMsg(1)
          csTaskDone.ref ! Done
          Future.successful(Done)
        })

      }

      startSharding(
        system,
        typeName = "Entity",
        entityProps = Props[ShardedEntity](),
        extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
        extractShardId = MultiNodeClusterShardingSpec.intExtractShardId)

      enterBarrier("before-shutdown")

      runOn(second) {
        CoordinatedShutdown(system).run(CoordinatedShutdown.UnknownReason)
        awaitCond(cluster.isTerminated)
      }

      runOn(third) {
        CoordinatedShutdown(system).run(CoordinatedShutdown.UnknownReason)
        awaitCond(cluster.isTerminated)
        csTaskDone.expectMsg(Done)
      }

      enterBarrier("after-shutdown")

      runOn(first) {
        region ! 2
        expectMsg(2)
        lastSender.path.address.hasLocalScope should ===(true)
      }

      enterBarrier("after-1")
    }
  }
}
