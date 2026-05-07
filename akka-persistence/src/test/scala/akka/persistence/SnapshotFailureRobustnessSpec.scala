/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.IOException
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.Config

import akka.actor.{ ActorRef, Props }
import akka.event.Logging
import akka.persistence.snapshot.local.LocalSnapshotStore
import akka.testkit.{ EventFilter, ImplicitSender, TestEvent }

object SnapshotFailureRobustnessSpec {

  case class Cmd(payload: String)
  case class DeleteSnapshot(seqNr: Int)
  case class DeleteSnapshots(criteria: SnapshotSelectionCriteria)

  class SaveSnapshotTestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {
    override def receiveRecover: Receive = {
      case SnapshotOffer(md, s) => probe ! ((md, s))
      case other                => probe ! other
    }

    override def receiveCommand = {
      case Cmd(payload)            => persist(payload)(_ => saveSnapshot(payload))
      case SaveSnapshotSuccess(md) => probe ! md.sequenceNr
      case other                   => probe ! other
    }
  }

  class DeleteSnapshotTestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {

    override def snapshotPluginId: String =
      "akka.persistence.snapshot-store.local-delete-fail"

    override def receiveRecover: Receive = {
      case SnapshotOffer(md, s) => probe ! ((md, s))
      case other                => probe ! other
    }

    override def receiveCommand = {
      case Cmd(payload)            => persist(payload)(_ => saveSnapshot(payload))
      case DeleteSnapshot(seqNr)   => deleteSnapshot(seqNr)
      case DeleteSnapshots(crit)   => deleteSnapshots(crit)
      case SaveSnapshotSuccess(md) => probe ! md.sequenceNr
      case other                   => probe ! other
    }
  }

  class LoadSnapshotTestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {
    override def receiveRecover: Receive = {
      case SnapshotOffer(md, s) => probe ! ((md, s))
      case payload: String      => probe ! s"${payload}-${lastSequenceNr}"
      case other                => probe ! other
    }

    override def receiveCommand = {
      case Cmd(payload) =>
        persist(payload) { _ =>
          probe ! s"${payload}-${lastSequenceNr}"
        }
      case SnapshotOffer(md, s) => probe ! ((md, s))
      case other                => probe ! other
    }
  }

  class FailingLocalSnapshotStore(config: Config) extends LocalSnapshotStore(config) {
    override def save(metadata: SnapshotMetadata, snapshot: Any): Unit = {
      if (metadata.sequenceNr == 2 || snapshot.toString.startsWith("boom")) {
        val bytes = "b0rkb0rk".getBytes("UTF-8") // length >= 8 to prevent EOF exception
        val tmpFile = withOutputStream(metadata)(_.write(bytes))
        tmpFile.renameTo(snapshotFileForWrite(metadata))
      } else super.save(metadata, snapshot)
    }
  }

  class DeleteFailingLocalSnapshotStore(config: Config) extends LocalSnapshotStore(config) {
    override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
      super.deleteAsync(metadata) // we actually delete it properly, but act as if it failed
      Future.failed(new IOException("Failed to delete snapshot for some reason!"))
    }

    override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
      super.deleteAsync(persistenceId, criteria) // we actually delete it properly, but act as if it failed
      Future.failed(new IOException("Failed to delete snapshot for some reason!"))
    }
  }
}

class SnapshotFailureRobustnessSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "SnapshotFailureRobustnessSpec",
        serialization = "off",
        extraConfig = Some(s"""
  akka.persistence.snapshot-store.local.class = "akka.persistence.SnapshotFailureRobustnessSpec$$FailingLocalSnapshotStore"
  akka.persistence.snapshot-store.local-delete-fail = $${akka.persistence.snapshot-store.local}
  akka.persistence.snapshot-store.local-delete-fail.class = "akka.persistence.SnapshotFailureRobustnessSpec$$DeleteFailingLocalSnapshotStore"
  """)))
    with ImplicitSender {

  import SnapshotFailureRobustnessSpec._

  "A persistentActor with a failing snapshot" must {
    "recover state starting from the most recent complete snapshot" in {
      val sPersistentActor = system.actorOf(Props(classOf[SaveSnapshotTestPersistentActor], name, testActor))
      val persistenceId = name

      expectMsg(RecoveryCompleted)
      sPersistentActor ! Cmd("blahonga")
      expectMsg(1)
      sPersistentActor ! Cmd("kablama")
      expectMsg(2)
      system.eventStream.publish(
        TestEvent.Mute(EventFilter[java.io.NotSerializableException](start = "Error loading snapshot")))
      system.eventStream.subscribe(testActor, classOf[Logging.Error])
      try {
        system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, testActor))
        expectMsgType[Logging.Error].message.toString should startWith("Error loading snapshot")
        expectMsgPF() {
          case (SnapshotMetadata(`persistenceId`, 1, timestamp), state) =>
            state should ===("blahonga")
            timestamp should be > (0L)
        }
        expectMsg("kablama-2")
        expectMsg(RecoveryCompleted)
        expectNoMessage(1 second)
      } finally {
        system.eventStream.unsubscribe(testActor, classOf[Logging.Error])
        system.eventStream.publish(TestEvent.UnMute(EventFilter.error(start = "Error loading snapshot [")))
      }
    }

    "fail recovery and stop actor when no snapshot could be loaded" in {
      val sPersistentActor = system.actorOf(Props(classOf[SaveSnapshotTestPersistentActor], name, testActor))

      expectMsg(RecoveryCompleted)
      sPersistentActor ! Cmd("ok")
      expectMsg(1)
      // max-attempts = 3
      sPersistentActor ! Cmd("boom")
      expectMsg(2)
      sPersistentActor ! Cmd("boom")
      expectMsg(3)
      sPersistentActor ! Cmd("boom")
      expectMsg(4)
      system.eventStream.publish(
        TestEvent.Mute(EventFilter[java.io.NotSerializableException](start = "Error loading snapshot")))
      system.eventStream.publish(
        TestEvent.Mute(EventFilter[java.io.NotSerializableException](start = "Persistence failure")))
      system.eventStream.subscribe(testActor, classOf[Logging.Error])
      try {
        val lPersistentActor = system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, testActor))
        (1 to 3).foreach { _ =>
          expectMsgType[Logging.Error].message.toString should startWith("Error loading snapshot")
        }
        expectMsgType[Logging.Error].message.toString should startWith("Persistence failure")
        watch(lPersistentActor)
        expectTerminated(lPersistentActor)
      } finally {
        system.eventStream.unsubscribe(testActor, classOf[Logging.Error])
        system.eventStream.publish(TestEvent.UnMute(EventFilter.error(start = "Error loading snapshot [")))
      }
    }

    "receive failure message when deleting a single snapshot fails" in {
      val p = system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, testActor))
      val persistenceId = name

      expectMsg(RecoveryCompleted)
      p ! Cmd("hello")
      expectMsg(1)
      p ! DeleteSnapshot(1)
      expectMsgPF() {
        case DeleteSnapshotFailure(SnapshotMetadata(`persistenceId`, 1, _), cause) =>
          // ok, expected failure
          cause.getMessage should include("Failed to delete")
      }
    }
    "receive failure message when bulk deleting snapshot fails" in {
      val p = system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, testActor))

      expectMsg(RecoveryCompleted)
      p ! Cmd("hello")
      expectMsg(1)
      p ! Cmd("hola")
      expectMsg(2)
      val criteria = SnapshotSelectionCriteria(maxSequenceNr = 10)
      p ! DeleteSnapshots(criteria)
      expectMsgPF() {
        case DeleteSnapshotsFailure(_, cause) =>
          // ok, expected failure
          cause.getMessage should include("Failed to delete")
      }
    }
  }
}

class SnapshotIsOptionalSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "SnapshotFailureReplayEventsSpec",
        serialization = "off",
        extraConfig = Some(s"""
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.class = "akka.persistence.SnapshotFailureRobustnessSpec$$FailingLocalSnapshotStore"
    akka.persistence.snapshot-store.local.dir = "target/persistence-${UUID.randomUUID().toString}"
    akka.persistence.snapshot-store.local.snapshot-is-optional = true
  """)))
    with ImplicitSender {

  import SnapshotFailureRobustnessSpec._

  "A persistentActor with a failing snapshot with snapshot-is-optional=true" must {
    "fall back to events" in {
      val sPersistentActor = system.actorOf(Props(classOf[SaveSnapshotTestPersistentActor], name, testActor))

      expectMsg(RecoveryCompleted)
      sPersistentActor ! Cmd("boom1")
      expectMsg(1)
      sPersistentActor ! Cmd("boom2")
      expectMsg(2)

      system.eventStream.publish(
        TestEvent.Mute(EventFilter[java.io.NotSerializableException](start = "Error loading snapshot")))
      try {
        system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, testActor))

        expectMsg("boom1-1") // from event replay
        expectMsg("boom2-2") // from event replay
        expectMsg(RecoveryCompleted)
        expectNoMessage()
      } finally {
        system.eventStream.publish(TestEvent.UnMute(EventFilter.error(start = "Error loading snapshot [")))
      }
    }

  }
}
