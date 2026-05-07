/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.cluster.{ Cluster, MemberStatus }
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.testkit._

object ClusterShardingRememberEntitiesNewExtractorSpec {

  final case class Started(ref: ActorRef)

  def props(probe: Option[ActorRef]): Props = Props(new TestEntity(probe))

  class TestEntity(probe: Option[ActorRef]) extends Actor with ActorLogging {
    log.info("Entity started: " + self.path)
    probe.foreach(_ ! Started(self))

    def receive = {
      case m => sender() ! m
    }
  }

  val shardCount = 5

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case id: Int => (id.toString, id)
  }

  val extractShardId1: ShardRegion.ExtractShardId = {
    case id: Int                     => (id % shardCount).toString
    case ShardRegion.StartEntity(id) => extractShardId1(id.toInt)
    case _                           => throw new IllegalArgumentException()
  }

  val extractShardId2: ShardRegion.ExtractShardId = {
    // always bump it one shard id
    case id: Int                     => ((id + 1) % shardCount).toString
    case ShardRegion.StartEntity(id) => extractShardId2(id.toInt)
    case _                           => throw new IllegalArgumentException()
  }

}

abstract class ClusterShardingRememberEntitiesNewExtractorSpecConfig(mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig = "akka.persistence.journal.leveldb-shared.store.native = off") {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  val roleConfig = ConfigFactory.parseString("""
      akka.cluster.roles = [sharding]
    """)

  // we pretend node 4 and 5 are new incarnations of node 2 and 3 as they never run in parallel
  // so we can use the same lmdb store for them and have node 4 pick up the persisted data of node 2
  val ddataNodeAConfig = ConfigFactory.parseString("""
      akka.cluster.sharding.distributed-data.durable.lmdb {
        dir = target/ShardingRememberEntitiesNewExtractorSpec/sharding-node-a
      }
    """)
  val ddataNodeBConfig = ConfigFactory.parseString("""
      akka.cluster.sharding.distributed-data.durable.lmdb {
        dir = target/ShardingRememberEntitiesNewExtractorSpec/sharding-node-b
      }
    """)

  nodeConfig(second)(roleConfig.withFallback(ddataNodeAConfig))
  nodeConfig(third)(roleConfig.withFallback(ddataNodeBConfig))

}

object PersistentClusterShardingRememberEntitiesSpecNewExtractorConfig
    extends ClusterShardingRememberEntitiesNewExtractorSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingRememberEntitiesNewExtractorSpecConfig
    extends ClusterShardingRememberEntitiesNewExtractorSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingRememberEntitiesNewExtractorSpec
    extends ClusterShardingRememberEntitiesNewExtractorSpec(
      PersistentClusterShardingRememberEntitiesSpecNewExtractorConfig)

class PersistentClusterShardingRememberEntitiesNewExtractorMultiJvmNode1
    extends PersistentClusterShardingRememberEntitiesNewExtractorSpec
class PersistentClusterShardingRememberEntitiesNewExtractorMultiJvmNode2
    extends PersistentClusterShardingRememberEntitiesNewExtractorSpec
class PersistentClusterShardingRememberEntitiesNewExtractorMultiJvmNode3
    extends PersistentClusterShardingRememberEntitiesNewExtractorSpec

class DDataClusterShardingRememberEntitiesNewExtractorSpec
    extends ClusterShardingRememberEntitiesNewExtractorSpec(DDataClusterShardingRememberEntitiesNewExtractorSpecConfig)

class DDataClusterShardingRememberEntitiesNewExtractorMultiJvmNode1
    extends DDataClusterShardingRememberEntitiesNewExtractorSpec
class DDataClusterShardingRememberEntitiesNewExtractorMultiJvmNode2
    extends DDataClusterShardingRememberEntitiesNewExtractorSpec
class DDataClusterShardingRememberEntitiesNewExtractorMultiJvmNode3
    extends DDataClusterShardingRememberEntitiesNewExtractorSpec

abstract class ClusterShardingRememberEntitiesNewExtractorSpec(
    multiNodeConfig: ClusterShardingRememberEntitiesNewExtractorSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {
  import ClusterShardingRememberEntitiesNewExtractorSpec._
  import multiNodeConfig._

  val typeName = "Entity"

  def startShardingWithExtractor1(): Unit = {
    startSharding(
      system,
      typeName = typeName,
      entityProps = ClusterShardingRememberEntitiesNewExtractorSpec.props(None),
      settings = settings.withRole("sharding"),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId1)
  }

  def startShardingWithExtractor2(sys: ActorSystem, probe: ActorRef): Unit = {
    startSharding(
      sys,
      typeName = typeName,
      entityProps = ClusterShardingRememberEntitiesNewExtractorSpec.props(Some(probe)),
      settings = ClusterShardingSettings(sys).withRememberEntities(config.rememberEntities).withRole("sharding"),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId2)
  }

  def region(sys: ActorSystem = system) = ClusterSharding(sys).shardRegion(typeName)

  s"Cluster with min-nr-of-members using sharding (${multiNodeConfig.mode})" must {

    "start up first cluster and sharding" in within(15.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(second, third))

      join(first, first)
      join(second, first)
      join(third, first)

      runOn(first, second, third) {
        within(remaining) {
          awaitAssert {
            cluster.state.members.count(_.status == MemberStatus.Up) should ===(3)
          }
        }
      }
      runOn(second, third) {
        startShardingWithExtractor1()
      }
      enterBarrier("first-cluster-up")

      runOn(second, third) {
        // one entity for each shard id
        (1 to 10).foreach { n =>
          region() ! n
          expectMsg(n)
        }
      }
      enterBarrier("first-cluster-entities-up")
    }

    "shutdown sharding nodes" in within(30.seconds) {
      runOn(first) {
        testConductor.exit(second, 0).await
        testConductor.exit(third, 0).await
      }
      runOn(first) {
        within(remaining) {
          awaitAssert {
            cluster.state.members.count(_.status == MemberStatus.Up) should ===(1)
          }
        }
      }
      enterBarrier("first-sharding-cluster-stopped")
    }

    "start new nodes with different extractor, and have the entities running on the right shards" in within(30.seconds) {

      // start it with a new shard id extractor, which will put the entities
      // on different shards

      runOn(second, third) {
        watch(region())
        Cluster(system).leave(Cluster(system).selfAddress)
        expectTerminated(region())
        awaitAssert {
          Cluster(system).isTerminated should ===(true)
        }
      }
      enterBarrier("first-cluster-terminated")

      // no sharding nodes left of the original cluster, start a new nodes
      runOn(second, third) {
        val sys2 = ActorSystem(system.name, system.settings.config)
        val probe2 = TestProbe()(sys2)

        if (persistenceIsNeeded) {
          sys2.actorSelection(node(first) / "user" / "store").tell(Identify(None), probe2.ref)
          val sharedStore = probe2.expectMsgType[ActorIdentity](10.seconds).ref.get
          SharedLeveldbJournal.setStore(sharedStore, sys2)
        }

        Cluster(sys2).join(node(first).address)
        startShardingWithExtractor2(sys2, probe2.ref)
        probe2.expectMsgType[Started](20.seconds)

        var stats: ShardRegion.CurrentShardRegionState = null
        within(10.seconds) {
          awaitAssert {
            region(sys2) ! ShardRegion.GetShardRegionState
            val reply = expectMsgType[ShardRegion.CurrentShardRegionState]
            reply.shards should not be empty
            stats = reply
          }
        }

        for {
          shardState <- stats.shards
          entityId <- shardState.entityIds
        } {
          val calculatedShardId = extractShardId2(entityId.toInt)
          calculatedShardId should ===(shardState.shardId)
        }

        enterBarrier("verified")
        shutdown(sys2)
      }

      runOn(first) {
        enterBarrier("verified")
      }

      enterBarrier("done")
    }

  }
}
