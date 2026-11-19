/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.cluster.sharding.ClusterShardingSettings
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object EventSourcedRememberEntitiesCoordinatorStoreSpec {
  def config =
    ConfigFactory.parseString(s"""
      akka.loglevel=DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.snapshot-after = 2
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/${classOf[EventSourcedRememberEntitiesCoordinatorStoreSpec].getName}-${UUID
      .randomUUID()
      .toString}"
    """)
}

class EventSourcedRememberEntitiesCoordinatorStoreSpec
    extends AkkaSpec(EventSourcedRememberEntitiesCoordinatorStoreSpec.config)
    with AnyWordSpecLike
    with ImplicitSender
    with WithLogCapturing {

  private val settings = ClusterShardingSettings(system)

  "The EventSourcedRememberEntitiesCoordinatorStore" must {

    "remember added shards across incarnations" in {
      val store = system.actorOf(EventSourcedRememberEntitiesCoordinatorStore.props("TypeA", settings))
      store ! RememberEntitiesCoordinatorStore.GetShards
      expectMsgType[RememberEntitiesCoordinatorStore.RememberedShards].entities should be(empty)

      (1 to 5).foreach { n =>
        store ! RememberEntitiesCoordinatorStore.AddShard(n.toString)
        expectMsg(RememberEntitiesCoordinatorStore.UpdateDone(n.toString))
      }

      watch(store)
      system.stop(store)
      expectTerminated(store)

      val store2 = system.actorOf(EventSourcedRememberEntitiesCoordinatorStore.props("TypeA", settings))
      store2 ! RememberEntitiesCoordinatorStore.GetShards
      expectMsgType[RememberEntitiesCoordinatorStore.RememberedShards].entities should ===(
        (1 to 5).map(_.toString).toSet)
    }

    "not persist a shard id that is already remembered" in {
      val writes = TestProbe()
      system.eventStream.subscribe(writes.ref, classOf[InmemJournal.Write])
      val persistenceId = "/sharding/TypeBCoordinator"

      val store = system.actorOf(EventSourcedRememberEntitiesCoordinatorStore.props("TypeB", settings))
      store ! RememberEntitiesCoordinatorStore.AddShard("1")
      expectMsg(RememberEntitiesCoordinatorStore.UpdateDone("1"))
      writes.fishForMessage() {
        case InmemJournal.Write("1", `persistenceId`, _) => true
        case _                                           => false
      }

      store ! RememberEntitiesCoordinatorStore.AddShard("1")
      expectMsg(RememberEntitiesCoordinatorStore.UpdateDone("1"))

      watch(store)
      system.stop(store)
      expectTerminated(store)

      // also after recovery
      val store2 = system.actorOf(EventSourcedRememberEntitiesCoordinatorStore.props("TypeB", settings))
      store2 ! RememberEntitiesCoordinatorStore.AddShard("1")
      expectMsg(RememberEntitiesCoordinatorStore.UpdateDone("1"))
      store2 ! RememberEntitiesCoordinatorStore.GetShards
      expectMsgType[RememberEntitiesCoordinatorStore.RememberedShards].entities should ===(Set("1"))

      writes.expectNoMessage()
    }
  }
}
