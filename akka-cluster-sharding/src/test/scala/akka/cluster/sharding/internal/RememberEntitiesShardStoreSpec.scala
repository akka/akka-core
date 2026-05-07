/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import java.util.UUID
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.Props
import akka.cluster.{ Cluster, MemberStatus }
import akka.cluster.ddata.{ Replicator, ReplicatorSettings }
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit.{ AkkaSpec, ImplicitSender, WithLogCapturing }

/**
 * Covers the interaction between the shard and the remember entities store
 */
object RememberEntitiesShardStoreSpec {
  def config =
    ConfigFactory.parseString(s"""
      akka.loglevel=DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.state-store-mode = ddata
      akka.cluster.sharding.snapshot-after = 2
      akka.cluster.sharding.remember-entities = on
      # no leaks between test runs thank you
      akka.cluster.sharding.distributed-data.durable.keys = []
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/${classOf[RememberEntitiesShardStoreSpec].getName}-${UUID
                                   .randomUUID()
                                   .toString}"
    """.stripMargin)
}

// shared base class for both persistence and ddata specs
abstract class RememberEntitiesShardStoreSpec
    extends AkkaSpec(RememberEntitiesShardStoreSpec.config)
    with AnyWordSpecLike
    with ImplicitSender
    with WithLogCapturing {

  def storeName: String
  def storeProps(shardId: ShardId, typeName: String, settings: ClusterShardingSettings): Props

  override def atStartup(): Unit = {
    // Form a one node cluster
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert(cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1))
  }

  s"The $storeName" must {

    val shardingSettings = ClusterShardingSettings(system)

    "store starts and stops and list remembered entity ids" in {

      val store = system.actorOf(storeProps("FakeShardId", "FakeTypeName", shardingSettings))

      store ! RememberEntitiesShardStore.GetEntities
      expectMsgType[RememberEntitiesShardStore.RememberedEntities].entities should be(empty)

      store ! RememberEntitiesShardStore.Update(Set("1", "2", "3"), Set.empty)
      expectMsg(RememberEntitiesShardStore.UpdateDone(Set("1", "2", "3"), Set.empty))

      store ! RememberEntitiesShardStore.Update(Set("4", "5", "6"), Set("2", "3"))
      expectMsg(RememberEntitiesShardStore.UpdateDone(Set("4", "5", "6"), Set("2", "3")))

      store ! RememberEntitiesShardStore.Update(Set.empty, Set("6"))
      expectMsg(RememberEntitiesShardStore.UpdateDone(Set.empty, Set("6")))

      store ! RememberEntitiesShardStore.Update(Set("2"), Set.empty)
      expectMsg(RememberEntitiesShardStore.UpdateDone(Set("2"), Set.empty))

      // the store does not support get after update
      val storeIncarnation2 = system.actorOf(storeProps("FakeShardId", "FakeTypeName", shardingSettings))

      storeIncarnation2 ! RememberEntitiesShardStore.GetEntities
      expectMsgType[RememberEntitiesShardStore.RememberedEntities].entities should ===(Set("1", "2", "4", "5"))
    }

    "handle a late request" in {
      // the store does not support get after update
      val storeIncarnation3 = system.actorOf(storeProps("FakeShardId", "FakeTypeName", shardingSettings))

      Thread.sleep(500)
      storeIncarnation3 ! RememberEntitiesShardStore.GetEntities
      expectMsgType[RememberEntitiesShardStore.RememberedEntities].entities should ===(Set("1", "2", "4", "5")) // from previous test
    }

    "handle a large batch" in {
      var store = system.actorOf(storeProps("FakeShardIdLarge", "FakeTypeNameLarge", shardingSettings))
      store ! RememberEntitiesShardStore.GetEntities
      expectMsgType[RememberEntitiesShardStore.RememberedEntities].entities should be(empty)

      store ! RememberEntitiesShardStore.Update((1 to 1000).map(_.toString).toSet, (1001 to 2000).map(_.toString).toSet)
      val response = expectMsgType[RememberEntitiesShardStore.UpdateDone]
      response.started should have size (1000)
      response.stopped should have size (1000)

      watch(store)
      system.stop(store)
      expectTerminated(store)

      store = system.actorOf(storeProps("FakeShardIdLarge", "FakeTypeNameLarge", shardingSettings))
      store ! RememberEntitiesShardStore.GetEntities
      expectMsgType[RememberEntitiesShardStore.RememberedEntities].entities should have size (1000)
    }

  }

}

class DDataRememberEntitiesShardStoreSpec extends RememberEntitiesShardStoreSpec {

  val replicatorSettings = ReplicatorSettings(system)
  val replicator = system.actorOf(Replicator.props(replicatorSettings))

  override def storeName: String = "DDataRememberEntitiesShardStore"
  override def storeProps(shardId: ShardId, typeName: String, settings: ClusterShardingSettings): Props =
    DDataRememberEntitiesShardStore.props(shardId, typeName, settings, replicator, majorityMinCap = 1)
}

class EventSourcedRememberEntitiesShardStoreSpec extends RememberEntitiesShardStoreSpec {

  override def storeName: String = "EventSourcedRememberEntitiesShardStore"
  override def storeProps(shardId: ShardId, typeName: String, settings: ClusterShardingSettings): Props =
    EventSourcedRememberEntitiesShardStore.props(typeName, shardId, settings)

  import akka.cluster.sharding.internal.EventSourcedRememberEntitiesShardStore.{ createEntityEvents, EntitiesStarted }

  it must {
    "create entity events in correct batches when entity counts exceed limits" in {
      val entityIds = (1 to 20).map(_.toString).toSet
      val entityEvents = createEntityEvents(entityIds, EntitiesStarted.apply _, batchSize = 7)
      entityEvents.size shouldBe 3
      val entities = entityEvents.map(_.asInstanceOf[EntitiesStarted].entities.toSeq)
      entities.map(_.size) should contain theSameElementsAs Seq(7, 7, 6)
      entities.flatten should contain theSameElementsAs entityIds
    }

    "create single entity event when entity counts under limits" in {
      val entityIds = (1 to 7).map(_.toString).toSet
      val entityEvents = createEntityEvents(entityIds, EntitiesStarted.apply _, batchSize = 7)
      entityEvents.size shouldBe 1
      val entities = entityEvents.map(_.asInstanceOf[EntitiesStarted].entities.toSeq)
      entities.map(_.size) should contain theSameElementsAs Seq(7)
      entities.flatten should contain theSameElementsAs entityIds
    }
  }

}
