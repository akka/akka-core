/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.File

import org.apache.commons.io.FileUtils

import akka.actor._
import akka.persistence.serialization.Snapshot
import akka.serialization.{ Serialization, SerializationExtension }
import akka.testkit._

object SnapshotRecoveryWithEmptyJournalSpec {
  val survivingSnapshotPath = "target/survivingSnapshotPath"

  case object TakeSnapshot

  class SaveSnapshotTestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {
    var state = List.empty[String]

    override def receiveRecover: Receive = {
      case payload: String                     => state = s"${payload}-${lastSequenceNr}" :: state
      case SnapshotOffer(_, snapshot: List[_]) => state = snapshot.asInstanceOf[List[String]]
    }

    override def receiveCommand: PartialFunction[Any, Unit] = {
      case payload: String =>
        persist(payload) { _ =>
          state = s"${payload}-${lastSequenceNr}" :: state
        }
      case TakeSnapshot             => saveSnapshot(state)
      case SaveSnapshotSuccess(md)  => probe ! md.sequenceNr
      case GetState                 => probe ! state.reverse
      case o: DeleteMessagesSuccess => probe ! o
    }
  }

  class LoadSnapshotTestPersistentActor(name: String, _recovery: Recovery, probe: ActorRef)
      extends NamedPersistentActor(name) {
    override def recovery: Recovery = _recovery

    override def receiveRecover: Receive = {
      case payload: String      => probe ! s"${payload}-${lastSequenceNr}"
      case offer: SnapshotOffer => probe ! offer
      case other                => probe ! other
    }

    override def receiveCommand: PartialFunction[Any, Unit] = {
      case "done" => probe ! "done"
      case payload: String =>
        persist(payload) { _ =>
          probe ! s"${payload}-${lastSequenceNr}"
        }
      case offer: SnapshotOffer => probe ! offer
      case other                => probe ! other
    }
  }
}

class SnapshotRecoveryWithEmptyJournalSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "SnapshotRecoveryWithEmptyJournalSpec",
        extraConfig = Some(s"""
  akka.persistence.snapshot-store.local.dir = "${SnapshotRecoveryWithEmptyJournalSpec.survivingSnapshotPath}"
  """)))
    with ImplicitSender {

  import SnapshotRecoveryWithEmptyJournalSpec._

  val persistenceId: String = namePrefix
  val snapshotsDir: File = new File(survivingSnapshotPath)

  val serializationExtension: Serialization = SerializationExtension(system)

  // Prepare a hand made snapshot file as basis for the recovery start point
  private def createSnapshotFile(sequenceNr: Long, ts: Long, data: Any): Unit = {
    val snapshotFile = new File(snapshotsDir, s"snapshot-$persistenceId-$sequenceNr-$ts")
    FileUtils.writeByteArrayToFile(snapshotFile, serializationExtension.serialize(Snapshot(data)).get)
  }

  val givenSnapshotSequenceNr: Long = 4711L
  val givenTimestamp: Long = 1000L

  override protected def atStartup(): Unit = {
    super.atStartup()
    createSnapshotFile(givenSnapshotSequenceNr - 1, givenTimestamp - 1, List("a-1"))
    createSnapshotFile(givenSnapshotSequenceNr, givenTimestamp, List("a-1", "b-2"))
  }

  "A persistent actor in a system that only has snapshots and no previous journal activity" must {
    "recover its state and sequence number starting from the most recent snapshot and use subsequent sequence numbers to persist events to the journal" in {
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], persistenceId, Recovery(), testActor))
      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, `givenSnapshotSequenceNr`, timestamp), state) =>
          state should ===(List("a-1", "b-2"))
          timestamp shouldEqual givenTimestamp
      }
      expectMsg(RecoveryCompleted)

      val persistentActor1 = system.actorOf(Props(classOf[SaveSnapshotTestPersistentActor], persistenceId, testActor))
      persistentActor1 ! "c"
      persistentActor1 ! TakeSnapshot
      expectMsgAllOf(givenSnapshotSequenceNr + 1)
    }
  }
}
