/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import java.util.concurrent.CyclicBarrier

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.scaladsl.adapter._
import akka.persistence
import akka.persistence.SelectedSnapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.typed.StashingWhenSnapshottingSpec.ControllableSnapshotStoreExt
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

object StashingWhenSnapshottingSpec {
  object ControllableSnapshotStoreExt extends ExtensionId[ControllableSnapshotStoreExt] {

    override def createExtension(system: ActorSystem[_]): ControllableSnapshotStoreExt =
      new ControllableSnapshotStoreExt()
  }

  class ControllableSnapshotStoreExt extends Extension {
    val completeSnapshotWrite = Promise[Unit]()
    val snapshotWriteStarted = new CyclicBarrier(2)
  }

  class ControllableSnapshotStore extends SnapshotStore {
    override def loadAsync(
        persistenceId: String,
        criteria: persistence.SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future.successful(None)

    override def saveAsync(metadata: persistence.SnapshotMetadata, snapshot: Any): Future[Unit] = {
      ControllableSnapshotStoreExt(context.system.toTyped).snapshotWriteStarted.await()
      ControllableSnapshotStoreExt(context.system.toTyped).completeSnapshotWrite.future
    }
    override def deleteAsync(metadata: persistence.SnapshotMetadata): Future[Unit] = Future.successful(())
    override def deleteAsync(persistenceId: String, criteria: persistence.SnapshotSelectionCriteria): Future[Unit] =
      Future.successful(())
  }
  val config = ConfigFactory.parseString(s"""
  slow-snapshot {
    class = "akka.persistence.typed.StashingWhenSnapshottingSpec$$ControllableSnapshotStore"
  }
  akka.actor.allow-java-serialization = on
  akka {
    loglevel = "INFO"

    persistence {
      journal {
        plugin = "akka.persistence.journal.inmem"
        auto-start-journals = []
      }

      snapshot-store {
        plugin = "slow-snapshot"
        auto-start-journals = []
      }
    }
  }
    """)

  def persistentTestBehavior(pid: PersistenceId, eventProbe: TestProbe[String]) =
    EventSourcedBehavior[String, String, List[String]](
      pid,
      Nil,
      (_, command) => Effect.persist(command),
      (state, event) => {
        eventProbe.ref.tell(event)
        event :: state
      }).snapshotWhen((_, event, _) => event.contains("snap"))
}

class StashingWhenSnapshottingSpec
    extends ScalaTestWithActorTestKit(StashingWhenSnapshottingSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  "A persistent actor" should {
    "stash messages and automatically replay when snapshot is in progress" in {
      val eventProbe = TestProbe[String]()
      val persistentActor =
        spawn(StashingWhenSnapshottingSpec.persistentTestBehavior(PersistenceId.ofUniqueId("1"), eventProbe))
      persistentActor ! "one"
      eventProbe.expectMessage("one")
      persistentActor ! "snap"
      eventProbe.expectMessage("snap")
      ControllableSnapshotStoreExt(system).snapshotWriteStarted.await()
      persistentActor ! "two"
      eventProbe.expectNoMessage() // snapshot in progress
      ControllableSnapshotStoreExt(system).completeSnapshotWrite.complete(Success(()))
      eventProbe.expectMessage("two")
    }
  }
}
