/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor._
import akka.testkit._

object SnapshotSpec {
  case object TakeSnapshot

  class SaveSnapshotTestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {
    var state = List.empty[String]

    override def receiveRecover: Receive = {
      case payload: String                     => state = s"${payload}-${lastSequenceNr}" :: state
      case SnapshotOffer(_, snapshot: List[_]) => state = snapshot.asInstanceOf[List[String]]
    }

    override def receiveCommand = {
      case payload: String =>
        persist(payload) { _ =>
          state = s"${payload}-${lastSequenceNr}" :: state
        }
      case TakeSnapshot            => saveSnapshot(state)
      case SaveSnapshotSuccess(md) => probe ! md.sequenceNr
      case GetState                => probe ! state.reverse
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

    override def receiveCommand = {
      case "done" => probe ! "done"
      case payload: String =>
        persist(payload) { _ =>
          probe ! s"${payload}-${lastSequenceNr}"
        }
      case offer: SnapshotOffer => probe ! offer
      case other                => probe ! other
    }
  }

  class IgnoringSnapshotTestPersistentActor(name: String, _recovery: Recovery, probe: ActorRef)
      extends NamedPersistentActor(name) {
    override def recovery: Recovery = _recovery

    override def receiveRecover: Receive = {
      case payload: String                             => probe ! s"${payload}-${lastSequenceNr}"
      case other if !other.isInstanceOf[SnapshotOffer] => probe ! other
    }

    override def receiveCommand = {
      case "done" => probe ! "done"
      case payload: String =>
        persist(payload) { _ =>
          probe ! s"${payload}-${lastSequenceNr}"
        }
      case other => probe ! other
    }
  }

  final case class Delete1(metadata: SnapshotMetadata)
  final case class DeleteN(criteria: SnapshotSelectionCriteria)

  class DeleteSnapshotTestPersistentActor(name: String, _recovery: Recovery, probe: ActorRef)
      extends LoadSnapshotTestPersistentActor(name, _recovery, probe) {

    override def receiveCommand = receiveDelete.orElse(super.receiveCommand)
    def receiveDelete: Receive = {
      case Delete1(metadata) => deleteSnapshot(metadata.sequenceNr)
      case DeleteN(criteria) => deleteSnapshots(criteria)
    }
  }
}

class SnapshotSpec extends PersistenceSpec(PersistenceSpec.config("inmem", "SnapshotSpec")) with ImplicitSender {
  import SnapshotProtocol._
  import SnapshotSpec._

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val persistentActor = system.actorOf(Props(classOf[SaveSnapshotTestPersistentActor], name, testActor))
    persistentActor ! "a"
    persistentActor ! TakeSnapshot
    persistentActor ! "b"
    persistentActor ! TakeSnapshot
    persistentActor ! "c"
    persistentActor ! "d"
    persistentActor ! TakeSnapshot
    persistentActor ! "e"
    persistentActor ! "f"
    expectMsgAllOf(1L, 2L, 4L)
  }

  "A persistentActor" must {
    "recover state starting from the most recent snapshot" in {
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, Recovery(), testActor))
      val persistenceId = name

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 4, timestamp), state) =>
          state should ===(List("a-1", "b-2", "c-3", "d-4").reverse)
          timestamp should be > (0L)
      }
      expectMsg("e-5")
      expectMsg("f-6")
      expectMsg(RecoveryCompleted)
    }
    "recover completely if snapshot is not handled" in {
      system.actorOf(Props(classOf[IgnoringSnapshotTestPersistentActor], name, Recovery(), testActor))

      expectMsg("a-1")
      expectMsg("b-2")
      expectMsg("c-3")
      expectMsg("d-4")
      expectMsg("e-5")
      expectMsg("f-6")
      expectMsg(RecoveryCompleted)
    }
    "recover state starting from the most recent snapshot matching an upper sequence number bound" in {
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, Recovery(toSequenceNr = 3), testActor))
      val persistenceId = name

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 2, timestamp), state) =>
          state should ===(List("a-1", "b-2").reverse)
          timestamp should be > (0L)
      }
      expectMsg("c-3")
      expectMsg(RecoveryCompleted)
    }
    "recover state starting from the most recent snapshot matching an upper sequence number bound (without further replay)" in {
      val persistentActor =
        system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, Recovery(toSequenceNr = 4), testActor))
      val persistenceId = name

      persistentActor ! "done"

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 4, timestamp), state) =>
          state should ===(List("a-1", "b-2", "c-3", "d-4").reverse)
          timestamp should be > (0L)
      }
      expectMsg(RecoveryCompleted)
      expectMsg("done")
    }
    "recover state starting from the most recent snapshot matching criteria" in {
      val recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria(maxSequenceNr = 2))
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, recovery, testActor))
      val persistenceId = name

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 2, timestamp), state) =>
          state should ===(List("a-1", "b-2").reverse)
          timestamp should be > (0L)
      }
      expectMsg("c-3")
      expectMsg("d-4")
      expectMsg("e-5")
      expectMsg("f-6")
      expectMsg(RecoveryCompleted)
    }
    "recover state starting from the most recent snapshot matching criteria and an upper sequence number bound" in {
      val recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria(maxSequenceNr = 2), toSequenceNr = 3)
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, recovery, testActor))
      val persistenceId = name

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 2, timestamp), state) =>
          state should ===(List("a-1", "b-2").reverse)
          timestamp should be > (0L)
      }
      expectMsg("c-3")
      expectMsg(RecoveryCompleted)
    }
    "recover state from scratch if snapshot based recovery is disabled" in {
      val recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.None, toSequenceNr = 3)
      system.actorOf(Props(classOf[LoadSnapshotTestPersistentActor], name, recovery, testActor))

      expectMsg("a-1")
      expectMsg("b-2")
      expectMsg("c-3")
      expectMsg(RecoveryCompleted)
    }
    "support single snapshot deletions" in {
      val deleteProbe = TestProbe()

      // recover persistentActor from 3rd snapshot and then delete snapshot
      val recovery = Recovery(toSequenceNr = 4)
      val persistentActor1 =
        system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, recovery, testActor))
      val persistenceId = name

      system.eventStream.subscribe(deleteProbe.ref, classOf[DeleteSnapshot])

      persistentActor1 ! "done"

      val metadata = expectMsgPF() {
        case SnapshotOffer(md @ SnapshotMetadata(`persistenceId`, 4, _), state) =>
          state should ===(List("a-1", "b-2", "c-3", "d-4").reverse)
          md
      }
      expectMsg(RecoveryCompleted)
      expectMsg("done")

      persistentActor1 ! Delete1(metadata)
      deleteProbe.expectMsgType[DeleteSnapshot]
      expectMsgPF() { case DeleteSnapshotSuccess(SnapshotMetadata(`persistenceId`, 4, _)) => }

      // recover persistentActor from 2nd snapshot (3rd was deleted) plus replayed messages
      system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, recovery, testActor))

      expectMsgPF(hint = "" + SnapshotOffer(SnapshotMetadata(`persistenceId`, 2, 0), null)) {
        case SnapshotOffer(md @ SnapshotMetadata(`persistenceId`, 2, _), state) =>
          state should ===(List("a-1", "b-2").reverse)
          md
      }
      expectMsg("c-3")
      expectMsg("d-4")
      expectMsg(RecoveryCompleted)
    }
    "support bulk snapshot deletions" in {
      val deleteProbe = TestProbe()

      val recovery = Recovery(toSequenceNr = 4)
      val persistentActor1 =
        system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, recovery, testActor))
      val persistenceId = name

      system.eventStream.subscribe(deleteProbe.ref, classOf[DeleteSnapshots])

      // recover persistentActor and the delete first three (= all) snapshots
      val criteria = SnapshotSelectionCriteria(maxSequenceNr = 4)
      persistentActor1 ! DeleteN(criteria)
      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(`persistenceId`, 4, _), state) =>
          state should ===(List("a-1", "b-2", "c-3", "d-4").reverse)
      }
      expectMsg(RecoveryCompleted)
      deleteProbe.expectMsgType[DeleteSnapshots]
      expectMsgPF() { case DeleteSnapshotsSuccess(`criteria`) => }

      // recover persistentActor from replayed messages (all snapshots deleted)
      system.actorOf(Props(classOf[DeleteSnapshotTestPersistentActor], name, recovery, testActor))

      expectMsg("a-1")
      expectMsg("b-2")
      expectMsg("c-3")
      expectMsg("d-4")
      expectMsg(RecoveryCompleted)
    }
  }
}
