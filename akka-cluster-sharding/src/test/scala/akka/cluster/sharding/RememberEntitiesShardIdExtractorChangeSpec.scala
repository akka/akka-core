/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.sharding.ShardRegion.CurrentRegions
import akka.persistence.PersistentActor
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

/**
 * Covers that remembered entities is correctly migrated when used and the shard id extractor
 * is changed so that entities should live on other shards after a full restart of the cluster.
 */
object RememberEntitiesShardIdExtractorChangeSpec {
  val config = ConfigFactory.parseString(s"""
       akka.loglevel = INFO
       akka.actor.provider = "cluster"
       akka.remote.artery.canonical.port = 0 
       akka.cluster.sharding {
        remember-entities = on
        remember-entities-store = "eventsourced"
        state-store-mode = "ddata"
       }
       akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
       akka.persistence.journal.plugin = "akka.persistence.journal.leveldb"
       akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
       akka.persistence.snapshot-store.local.dir = "target/RememberEntitiesShardIdExtractorChangeSpec-${UUID
    .randomUUID()
    .toString}"
       akka.persistence.journal.leveldb {
         native = off
          dir = "target/journal-PersistentShardingMigrationSpec-${UUID.randomUUID()}"
      }
      """)

  case class Message(id: Long)

  class PA extends PersistentActor {
    override def persistenceId: String = "pa-" + self.path.name
    override def receiveRecover: Receive = {
      case _ =>
    }
    override def receiveCommand: Receive = {
      case _ =>
        sender() ! "ack"
    }
  }

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ Message(id) => (id.toString, msg)
    case _                 => throw new IllegalArgumentException()
  }

  val firstExtractShardId: ShardRegion.ExtractShardId = {
    case Message(id)                 => (id % 10).toString
    case ShardRegion.StartEntity(id) => (id.toInt % 10).toString
    case _                           => throw new IllegalArgumentException()
  }

  val secondExtractShardId: ShardRegion.ExtractShardId = {
    case Message(id)                 => (id % 10 + 1L).toString
    case ShardRegion.StartEntity(id) => (id.toInt % 10 + 1L).toString
    case _                           => throw new IllegalArgumentException()
  }

  val TypeName = "ShardIdExtractorChange"
}
class RememberEntitiesShardIdExtractorChangeSpec
    extends AkkaSpec(PersistentShardingMigrationSpec.config)
    with ImplicitSender {

  import RememberEntitiesShardIdExtractorChangeSpec._

  "Sharding with remember entities enabled" should {
    "allow a change to the shard id extractor" in {

      withSystem("FirstShardIdExtractor", firstExtractShardId) { (_, region) =>
        assertRegionRegistrationComplete(region)
        region ! Message(1)
        expectMsg("ack")
        region ! Message(11)
        expectMsg("ack")
        region ! Message(21)
        expectMsg("ack")
      }

      withSystem("SecondShardIdExtractor", secondExtractShardId) { (system, region) =>
        val probe = TestProbe()(system)

        awaitAssert {
          region.tell(ShardRegion.GetShardRegionState, probe.ref)
          val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState]
          // shards should have been remembered but migrated over to shard 2
          state.shards.collect { case ShardRegion.ShardState("1", entities) => entities } shouldEqual Set(Set.empty)
          state.shards.collect { case ShardRegion.ShardState("2", entities) => entities } shouldEqual Set(
            Set("1", "11", "21"))
        }
      }

      withSystem("ThirdIncarnation", secondExtractShardId) { (system, region) =>
        val probe = TestProbe()(system)
        // Only way to verify that they were "normal"-remember-started here is to look at debug logs, will show
        // [akka://ThirdIncarnation@127.0.0.1:51533/system/sharding/ShardIdExtractorChange/1/RememberEntitiesStore] Recovery completed for shard [1] with [0] entities
        // [akka://ThirdIncarnation@127.0.0.1:51533/system/sharding/ShardIdExtractorChange/2/RememberEntitiesStore] Recovery completed for shard [2] with [3] entities
        awaitAssert {
          region.tell(ShardRegion.GetShardRegionState, probe.ref)
          val state = probe.expectMsgType[ShardRegion.CurrentShardRegionState]
          state.shards.collect { case ShardRegion.ShardState("1", entities) => entities } shouldEqual Set(Set.empty)
          state.shards.collect { case ShardRegion.ShardState("2", entities) => entities } shouldEqual Set(
            Set("1", "11", "21"))
        }
      }
    }

    def withSystem(systemName: String, extractShardId: ShardRegion.ExtractShardId)(
        f: (ActorSystem, ActorRef) => Unit): Unit = {
      val system = ActorSystem(systemName, config)
      Cluster(system).join(Cluster(system).selfAddress)
      try {
        val region = ClusterSharding(system).start(TypeName, Props(new PA()), extractEntityId, extractShardId)
        f(system, region)
      } finally {
        Await.ready(system.terminate(), 20.seconds)
      }
    }

    def assertRegionRegistrationComplete(region: ActorRef): Unit = {
      awaitAssert {
        region ! ShardRegion.GetCurrentRegions
        expectMsgType[CurrentRegions].regions should have size (1)
      }
    }
  }

}
