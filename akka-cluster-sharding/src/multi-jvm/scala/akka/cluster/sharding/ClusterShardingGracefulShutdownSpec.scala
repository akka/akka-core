/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor._
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.{ CurrentRegions, GracefulShutdown }
import akka.remote.testconductor.RoleName
import akka.testkit._

abstract class ClusterShardingGracefulShutdownSpecConfig(mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig =
        """
        akka.loglevel = info
        akka.persistence.journal.leveldb-shared.store.native = off
        # We set this high to allow pausing coordinated shutdown make sure the handoff completes 'immediately' and not
        # relies on the member removal, which could make things take longer then necessary
        akka.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout = 60s
        """) {
  val first = role("first")
  val second = role("second")
}

object PersistentClusterShardingGracefulShutdownSpecConfig
    extends ClusterShardingGracefulShutdownSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingGracefulShutdownSpecConfig
    extends ClusterShardingGracefulShutdownSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingGracefulShutdownSpec
    extends ClusterShardingGracefulShutdownSpec(PersistentClusterShardingGracefulShutdownSpecConfig)
class DDataClusterShardingGracefulShutdownSpec
    extends ClusterShardingGracefulShutdownSpec(DDataClusterShardingGracefulShutdownSpecConfig)

class PersistentClusterShardingGracefulShutdownMultiJvmNode1 extends PersistentClusterShardingGracefulShutdownSpec
class PersistentClusterShardingGracefulShutdownMultiJvmNode2 extends PersistentClusterShardingGracefulShutdownSpec

class DDataClusterShardingGracefulShutdownMultiJvmNode1 extends DDataClusterShardingGracefulShutdownSpec
class DDataClusterShardingGracefulShutdownMultiJvmNode2 extends DDataClusterShardingGracefulShutdownSpec

abstract class ClusterShardingGracefulShutdownSpec(multiNodeConfig: ClusterShardingGracefulShutdownSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import MultiNodeClusterShardingSpec.ShardedEntity
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
      entityProps = Props[ShardedEntity](),
      extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
      extractShardId = MultiNodeClusterShardingSpec.intExtractShardId,
      allocationStrategy = ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 2, relativeLimit = 1.0),
      handOffStopMessage = ShardedEntity.Stop)

  lazy val region = ClusterSharding(system).shardRegion(typeName)

  s"Cluster sharding (${multiNodeConfig.mode})" must {
    "start some shards in both regions" in within(30.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second))

      join(first, first, typeName) // oldest
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

      region ! ShardRegion.GetCurrentRegions
      expectMsgType[CurrentRegions].regions.size should be(2)
    }

    "gracefully shutdown the region on the newest node" in within(30.seconds) {
      runOn(second) {
        // Make sure the 'cluster-sharding-shutdown-region' phase takes at least 40 seconds,
        // to validate region shutdown completion is propagated immediately and not postponed
        // until when the cluster member leaves
        CoordinatedShutdown(system).addTask("cluster-sharding-shutdown-region", "postpone-actual-stop")(() => {
          akka.pattern.after(40.seconds)(Future.successful(Done))
        })
        CoordinatedShutdown(system).run(CoordinatedShutdown.unknownReason)
      }

      runOn(first) {
        awaitAssert {
          val p = TestProbe()
          for (n <- 1 to 200) {
            region.tell(n, p.ref)
            p.expectMsg(1.second, n)
            p.lastSender.path should be(region.path / n.toString / n.toString)
          }
        }
      }
      enterBarrier("handoff-completed")

      // Check that the coordinator is correctly notified the region has stopped:
      runOn(first) {
        // the coordinator side should observe that the region has stopped
        awaitAssert {
          region ! ShardRegion.GetCurrentRegions
          expectMsgType[CurrentRegions].regions.size should be(1)
        }
        // without having to wait for the member to be entirely removed (as that would cause unnecessary latency)
      }

      runOn(second) {
        watch(region)
        expectTerminated(region)
      }

      enterBarrier("after-3")
    }

    "gracefully shutdown empty region" in within(30.seconds) {
      runOn(first) {
        val regionEmpty = startSharding(typeName = "EntityEmpty")

        watch(regionEmpty)
        regionEmpty ! GracefulShutdown
        expectTerminated(regionEmpty, 5.seconds)
      }
    }

  }
}
