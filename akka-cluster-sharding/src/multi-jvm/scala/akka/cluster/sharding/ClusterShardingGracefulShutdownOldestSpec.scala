/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.remote.testconductor.RoleName
import akka.testkit._

abstract class ClusterShardingGracefulShutdownOldestSpecConfig(mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig = "akka.persistence.journal.leveldb-shared.store.native = off") {
  val first = role("first")
  val second = role("second")
}

object ClusterShardingGracefulShutdownOldestSpec {

  object SlowStopShardedEntity {
    case object Stop
    case object ActualStop
  }

  // slow stop previously made it more likely that the coordinator would stop before the local region
  class SlowStopShardedEntity extends Actor with Timers {
    import SlowStopShardedEntity._

    def receive: Receive = {
      case id: Int => sender() ! id
      case SlowStopShardedEntity.Stop =>
        timers.startSingleTimer(ActualStop, ActualStop, 50.millis)
      case SlowStopShardedEntity.ActualStop =>
        context.stop(self)
    }
  }

}

object PersistentClusterShardingGracefulShutdownOldestSpecConfig
    extends ClusterShardingGracefulShutdownOldestSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingGracefulShutdownOldestSpecConfig
    extends ClusterShardingGracefulShutdownOldestSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingGracefulShutdownOldestSpec
    extends ClusterShardingGracefulShutdownOldestSpec(PersistentClusterShardingGracefulShutdownOldestSpecConfig)
class DDataClusterShardingGracefulShutdownOldestSpec
    extends ClusterShardingGracefulShutdownOldestSpec(DDataClusterShardingGracefulShutdownOldestSpecConfig)

class PersistentClusterShardingGracefulShutdownOldestMultiJvmNode1
    extends PersistentClusterShardingGracefulShutdownOldestSpec
class PersistentClusterShardingGracefulShutdownOldestMultiJvmNode2
    extends PersistentClusterShardingGracefulShutdownOldestSpec

class DDataClusterShardingGracefulShutdownOldestMultiJvmNode1 extends DDataClusterShardingGracefulShutdownOldestSpec
class DDataClusterShardingGracefulShutdownOldestMultiJvmNode2 extends DDataClusterShardingGracefulShutdownOldestSpec

abstract class ClusterShardingGracefulShutdownOldestSpec(
    multiNodeConfig: ClusterShardingGracefulShutdownOldestSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import ClusterShardingGracefulShutdownOldestSpec._
  import multiNodeConfig._

  private val typeName = "Entity"

  def join(from: RoleName, to: RoleName, typeName: String): Unit = {
    super.join(from, to)
    runOn(from) {
      startSharding(typeName)
    }
    enterBarrier(s"$from-started")
  }

  def startSharding(typeName: String): ActorRef =
    startSharding(
      system,
      typeName,
      entityProps = Props[SlowStopShardedEntity](),
      extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
      extractShardId = MultiNodeClusterShardingSpec.intExtractShardId,
      allocationStrategy = ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 2, relativeLimit = 1.0),
      handOffStopMessage = SlowStopShardedEntity.Stop)

  lazy val region = ClusterSharding(system).shardRegion(typeName)

  s"Cluster sharding (${multiNodeConfig.mode})" must {

    "start some shards in both regions" in within(30.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second))

      join(first, first, typeName)
      join(second, first, typeName)

      awaitAssert {
        val p = TestProbe()
        val regionAddresses = (1 to 100).map { n =>
          region.tell(n, p.ref)
          p.expectMsg(1.second, n)
          p.lastSender.path.address
        }.toSet
        regionAddresses.size should be(2)
      }
      enterBarrier("after-2")
    }

    "gracefully shutdown the oldest region" in within(30.seconds) {
      runOn(first) {
        val coordinator = awaitAssert {
          Await.result(
            system
              .actorSelection(s"/system/sharding/${typeName}Coordinator/singleton/coordinator")
              .resolveOne(remainingOrDefault),
            remainingOrDefault)
        }

        val regionTerminationProbe = TestProbe()
        regionTerminationProbe.watch(region)
        val coordinatorTerminationProbe = TestProbe()
        coordinatorTerminationProbe.watch(coordinator)

        // trigger graceful shutdown
        cluster.leave(address(first))

        regionTerminationProbe.expectTerminated(region)
        coordinatorTerminationProbe.expectTerminated(coordinator)
      }
      enterBarrier("terminated")

      runOn(second) {
        awaitAssert {
          val p = TestProbe()
          val responses = (1 to 100).map { n =>
            region.tell(n, p.ref)
            p.expectMsg(1.second, n)
          }.toSet
          responses.size should be(100)
        }
      }
      enterBarrier("done-o")
    }

  }
}
