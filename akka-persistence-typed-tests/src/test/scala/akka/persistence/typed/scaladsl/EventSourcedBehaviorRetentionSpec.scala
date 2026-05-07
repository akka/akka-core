/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Try

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.typed.DeleteEventsCompleted
import akka.persistence.typed.DeleteSnapshotsCompleted
import akka.persistence.typed.DeleteSnapshotsFailed
import akka.persistence.typed.DeletionTarget
import akka.persistence.typed.EventSourcedSignal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.SnapshotCompleted
import akka.persistence.typed.SnapshotFailed
import akka.persistence.typed.SnapshotSelectionCriteria
import akka.serialization.jackson.CborSerializable

object EventSourcedBehaviorRetentionSpec extends Matchers {

  sealed trait Command extends CborSerializable
  case object Increment extends Command
  final case class IncrementWithPersistAll(nr: Int) extends Command
  final case class GetValue(replyTo: ActorRef[State]) extends Command
  case object StopIt extends Command

  final case class WrappedSignal(signal: EventSourcedSignal)

  sealed trait Event extends CborSerializable
  final case class Incremented(delta: Int) extends Event

  final case class State(value: Int, history: Vector[Int]) extends CborSerializable

  def counter(
      @nowarn("msg=never used") ctx: ActorContext[Command],
      persistenceId: PersistenceId,
      probe: Option[ActorRef[(State, Event)]] = None,
      snapshotSignalProbe: Option[ActorRef[WrappedSignal]] = None,
      deleteSnapshotSignalProbe: Option[ActorRef[WrappedSignal]] = None,
      eventSignalProbe: Option[ActorRef[Try[EventSourcedSignal]]] = None)
      : EventSourcedBehavior[Command, Event, State] = {
    EventSourcedBehavior[Command, Event, State](
      persistenceId,
      emptyState = State(0, Vector.empty),
      commandHandler = (state, cmd) =>
        cmd match {
          case Increment =>
            Effect.persist(Incremented(1))

          case IncrementWithPersistAll(n) =>
            Effect.persist((0 until n).map(_ => Incremented(1)))

          case GetValue(replyTo) =>
            replyTo ! state
            Effect.none

          case StopIt =>
            Effect.none.thenStop()

        },
      eventHandler = (state, evt) =>
        evt match {
          case Incremented(delta) =>
            probe.foreach(_ ! ((state, evt)))
            State(state.value + delta, state.history :+ state.value)
        }).receiveSignal {
      case (_, RecoveryCompleted) => ()
      case (_, sc: SnapshotCompleted) =>
        snapshotSignalProbe.foreach(_ ! WrappedSignal(sc))
      case (_, sf: SnapshotFailed) =>
        snapshotSignalProbe.foreach(_ ! WrappedSignal(sf))
      case (_, dc: DeleteSnapshotsCompleted) =>
        deleteSnapshotSignalProbe.foreach(_ ! WrappedSignal(dc))
      case (_, dsf: DeleteSnapshotsFailed) =>
        deleteSnapshotSignalProbe.foreach(_ ! WrappedSignal(dsf))
      case (_, e: EventSourcedSignal) =>
        eventSignalProbe.foreach(_ ! Success(e))
    }
  }

  implicit class WrappedSignalProbeAssert(probe: TestProbe[WrappedSignal]) {
    def expectSnapshotCompleted(sequenceNumber: Int): SnapshotCompleted = {
      val wrapped = probe.expectMessageType[WrappedSignal]
      wrapped.signal shouldBe a[SnapshotCompleted]
      val completed = wrapped.signal.asInstanceOf[SnapshotCompleted]
      completed.metadata.sequenceNr should ===(sequenceNumber)
      completed
    }

    def expectDeleteSnapshotCompleted(maxSequenceNr: Long): DeleteSnapshotsCompleted = {
      val wrapped = probe.expectMessageType[WrappedSignal]
      wrapped.signal shouldBe a[DeleteSnapshotsCompleted]
      val signal = wrapped.signal.asInstanceOf[DeleteSnapshotsCompleted]
      signal.target should ===(
        DeletionTarget.Criteria(SnapshotSelectionCriteria.latest.withMaxSequenceNr(maxSequenceNr)))
      signal
    }
  }

}

class EventSourcedBehaviorRetentionSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(PersistenceTestKitSnapshotPlugin.config))
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorRetentionSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  "EventSourcedBehavior with snapshot predicate" must {
    "delete events after snapshot" in {
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val alwaysSnapshot: Behavior[Command] =
        Behaviors.setup { ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref)).snapshotWhen((_, _, _) => true, deleteEventsOnSnapshot = true)
        }
      val c = spawn(alwaysSnapshot)
      val replyProbe = TestProbe[State]()

      c ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(1)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 1
      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(1, Vector(0)))
      c ! StopIt
      val watchProbe = TestProbe()
      watchProbe.expectTerminated(c)

      val probe = TestProbe[(State, Event)]()
      val c2 = spawn(Behaviors.setup[Command](ctx => counter(ctx, pid, Some(probe.ref))))

      probe.expectNoMessage()
      c2 ! Increment
      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(2, Vector(0, 1)))
    }

    "snapshot via predicate" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val alwaysSnapshot: Behavior[Command] =
        Behaviors.setup { ctx =>
          counter(ctx, pid, snapshotSignalProbe = Some(snapshotSignalProbe.ref)).snapshotWhen { (_, _, _) =>
            true
          }
        }
      val c = spawn(alwaysSnapshot)
      val replyProbe = TestProbe[State]()

      c ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(1)
      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(1, Vector(0)))
      c ! StopIt
      val watchProbe = TestProbe()
      watchProbe.expectTerminated(c)

      val probe = TestProbe[(State, Event)]()
      val c2 = spawn(Behaviors.setup[Command](ctx => counter(ctx, pid, Some(probe.ref))))
      // state should be rebuilt from snapshot, no events replayed
      // Fails as snapshot is async (i think)
      probe.expectNoMessage()
      c2 ! Increment
      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(2, Vector(0, 1)))
    }
  }

  "EventSourcedBehavior with retention" must {

    "snapshot every N sequence nrs" in {
      val pid = nextPid()
      val c = spawn(Behaviors.setup[Command](ctx =>
        counter(ctx, pid).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 2))))

      val replyProbe = TestProbe[State]()

      c ! Increment
      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(1, Vector(0)))
      c ! StopIt
      val watchProbe = TestProbe()
      watchProbe.expectTerminated(c)

      // no snapshot should have happened
      val probeC2 = TestProbe[(State, Event)]()
      val snapshotProbe = createTestProbe[WrappedSignal]()

      val c2 = spawn(
        Behaviors.setup[Command](ctx =>
          counter(ctx, pid, probe = Some(probeC2.ref), snapshotSignalProbe = Some(snapshotProbe.ref))
            .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 2))))
      probeC2.expectMessage[(State, Event)]((State(0, Vector()), Incremented(1)))

      c2 ! Increment
      snapshotProbe.expectSnapshotCompleted(2)
      c2 ! StopIt
      watchProbe.expectTerminated(c2)

      val probeC3 = TestProbe[(State, Event)]()
      val c3 = spawn(
        Behaviors.setup[Command](ctx =>
          counter(ctx, pid, Some(probeC3.ref))
            .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 2))))
      // this time it should have been snapshotted so no events to replay
      probeC3.expectNoMessage()
      c3 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(2, Vector(0, 1)))
    }

    "snapshot every N sequence nrs when persisting multiple events" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val c =
        spawn(
          Behaviors.setup[Command](ctx =>
            counter(ctx, pid, None, Some(snapshotSignalProbe.ref))
              .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 2))))
      val replyProbe = TestProbe[State]()

      c ! IncrementWithPersistAll(3)
      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, Vector(0, 1, 2)))
      // snapshot at seqNr 3 because of persistAll
      snapshotSignalProbe.expectSnapshotCompleted(3)
      c ! StopIt
      val watchProbe = TestProbe()
      watchProbe.expectTerminated(c)

      val probeC2 = TestProbe[(State, Event)]()
      val c2 = spawn(
        Behaviors.setup[Command](ctx =>
          counter(ctx, pid, Some(probeC2.ref))
            .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 2))))
      probeC2.expectNoMessage()
      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, Vector(0, 1, 2)))
    }

    "check all events for snapshot in PersistAll" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val snapshotAtTwo = Behaviors.setup[Command](ctx =>
        counter(ctx, pid, snapshotSignalProbe = Some(snapshotSignalProbe.ref)).snapshotWhen { (s, _, _) =>
          s.value == 2
        })
      val c: ActorRef[Command] = spawn(snapshotAtTwo)

      val replyProbe = TestProbe[State]()

      c ! IncrementWithPersistAll(3)

      c ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, Vector(0, 1, 2)))
      // snapshot at seqNr 3 because of persistAll
      snapshotSignalProbe.expectSnapshotCompleted(3)
      c ! StopIt
      val watchProbe = TestProbe()
      watchProbe.expectTerminated(c)

      val probeC2 = TestProbe[(State, Event)]()
      val c2 = spawn(Behaviors.setup[Command](ctx => counter(ctx, pid, Some(probeC2.ref))))
      // middle event triggered all to be snapshot
      probeC2.expectNoMessage()
      c2 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, Vector(0, 1, 2)))
    }

    "delete snapshots automatically, based on criteria" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](
          ctx =>
            counter(
              ctx,
              pid,
              snapshotSignalProbe = Some(snapshotSignalProbe.ref),
              deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref))
              .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 3, keepNSnapshots = 2))))

      (1 to 10).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(10, (0 until 10).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(3)
      snapshotSignalProbe.expectSnapshotCompleted(6)
      snapshotSignalProbe.expectSnapshotCompleted(9)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(3)

      (1 to 3).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(13, (0 until 13).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(12)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(6)

      (1 to 3).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(16, (0 until 16).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(15)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(9)

      (1 to 4).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(20, (0 until 20).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(18)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(12)

      snapshotSignalProbe.expectNoMessage()
    }

    "optionally delete both old events and snapshots" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref)).withRetention(
            // tests the Java API as well
            RetentionCriteria.snapshotEvery(numberOfEvents = 3, keepNSnapshots = 2).withDeleteEventsOnSnapshot)))

      (1 to 10).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(10, (0 until 10).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(3)
      snapshotSignalProbe.expectSnapshotCompleted(6)
      snapshotSignalProbe.expectSnapshotCompleted(9)

      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 3
      // Note that when triggering deletion of snapshots from deletion of events it is intentionally "off by one".
      // The reason for -1 is that a snapshot at the exact toSequenceNr is still useful and the events
      // after that can be replayed after that snapshot, but replaying the events after toSequenceNr without
      // starting at the snapshot at toSequenceNr would be invalid.
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(2)

      // one at a time since snapshotting+event-deletion switches to running state before deleting snapshot so ordering
      // if sending many commands in one go is not deterministic
      persistentActor ! Increment // 11
      persistentActor ! Increment // 12
      snapshotSignalProbe.expectSnapshotCompleted(12)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 6
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)

      persistentActor ! Increment // 13
      persistentActor ! Increment // 14
      persistentActor ! Increment // 11
      persistentActor ! Increment // 15
      snapshotSignalProbe.expectSnapshotCompleted(15)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 9
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(8)

      persistentActor ! Increment // 16
      persistentActor ! Increment // 17
      persistentActor ! Increment // 18
      snapshotSignalProbe.expectSnapshotCompleted(18)

      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 12
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(11)

      eventProbe.expectNoMessage()
      snapshotSignalProbe.expectNoMessage()
    }

    "be possible to combine snapshotWhen and retention criteria" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref))
            .snapshotWhen((_, _, seqNr) => seqNr == 3 || seqNr == 13)
            .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 5, keepNSnapshots = 1))))

      (1 to 3).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, (0 until 3).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(3)
      eventProbe.expectNoMessage()

      (4 to 10).foreach(_ => persistentActor ! Increment)
      snapshotSignalProbe.expectSnapshotCompleted(5)
      snapshotSignalProbe.expectSnapshotCompleted(10)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)

      (11 to 13).foreach(_ => persistentActor ! Increment)
      snapshotSignalProbe.expectSnapshotCompleted(13)
      // no deletes triggered by snapshotWhen
      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }

      (14 to 16).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(16, (0 until 16).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(15)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(10)
      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }
    }

    "be possible to combine snapshotWhen and retention criteria withDeleteEventsOnSnapshot" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref))
            .snapshotWhen((_, _, seqNr) => seqNr == 3 || seqNr == 13)
            .withRetention(
              RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 3).withDeleteEventsOnSnapshot)))

      (1 to 3).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, (0 until 3).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(2) // every-2 through criteria
      snapshotSignalProbe.expectSnapshotCompleted(3) // snapshotWhen
      // no event deletes or snapshot deletes after snapshotWhen
      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }

      // one at a time since snapshotting+event-deletion switches to running state before deleting snapshot so ordering
      // if sending many commands in one go is not deterministic
      persistentActor ! Increment // 4
      snapshotSignalProbe.expectSnapshotCompleted(4) // every-2 through criteria
      persistentActor ! Increment // 5
      persistentActor ! Increment // 6
      snapshotSignalProbe.expectSnapshotCompleted(6) // every-2 through criteria

      persistentActor ! Increment // 7
      persistentActor ! Increment // 8
      snapshotSignalProbe.expectSnapshotCompleted(8) // every-2 through criteria
      // triggers delete up to snapshot no 2
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 2
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(1) // then delete oldest snapshot

      persistentActor ! Increment // 9
      persistentActor ! Increment // 10
      snapshotSignalProbe.expectSnapshotCompleted(10) // every-2 through criteria
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(3)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 4

      persistentActor ! Increment // 11
      persistentActor ! Increment // 12
      snapshotSignalProbe.expectSnapshotCompleted(12) // every-2 through criteria
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 6

      persistentActor ! Increment // 13
      snapshotSignalProbe.expectSnapshotCompleted(13) // snapshotWhen
      // no deletes triggered by snapshotWhen
      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }

      persistentActor ! Increment // 14
      snapshotSignalProbe.expectSnapshotCompleted(14) // every-2 through criteria
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 8
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(7)

      persistentActor ! Increment // 15
      persistentActor ! Increment // 16
      snapshotSignalProbe.expectSnapshotCompleted(16) // every-2 through criteria
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 10
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(9)

      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }
    }

    "be possible to combine snapshotWhen withDeleteEventsOnSnapshot and retention criteria withDeleteEventsOnSnapshot" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref))
            .snapshotWhen((_, _, seqNr) => seqNr == 3 || seqNr == 13, deleteEventsOnSnapshot = true)
            .withRetention(
              RetentionCriteria.snapshotEvery(numberOfEvents = 2, keepNSnapshots = 3).withDeleteEventsOnSnapshot)))

      (1 to 3).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(3, (0 until 3).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(2) // every-2 through criteria
      snapshotSignalProbe.expectSnapshotCompleted(3) // snapshotWhen
      // no event deletes or snapshot deletes after snapshotWhen
      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }

      // one at a time since snapshotting+event-deletion switches to running state before deleting snapshot so ordering
      // if sending many commands in one go is not deterministic
      persistentActor ! Increment // 4
      snapshotSignalProbe.expectSnapshotCompleted(4) // every-2 through criteria
      persistentActor ! Increment // 5
      persistentActor ! Increment // 6
      snapshotSignalProbe.expectSnapshotCompleted(6) // every-2 through criteria

      persistentActor ! Increment // 7
      persistentActor ! Increment // 8
      snapshotSignalProbe.expectSnapshotCompleted(8) // every-2 through criteria
      // triggers delete up to snapshot no 2
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 2
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(1) // then delete oldest snapshot

      persistentActor ! Increment // 9
      persistentActor ! Increment // 10
      snapshotSignalProbe.expectSnapshotCompleted(10) // every-2 through criteria
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(3)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 4

      persistentActor ! Increment // 11
      persistentActor ! Increment // 12
      snapshotSignalProbe.expectSnapshotCompleted(12) // every-2 through criteria
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 6

      persistentActor ! Increment // 13
      snapshotSignalProbe.expectSnapshotCompleted(13) // snapshotWhen
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 7

      persistentActor ! Increment // 14
      snapshotSignalProbe.expectSnapshotCompleted(14) // every-2 through criteria
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 8
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(6)

      // Work around race condition: Wait here for delete to complete (how is it not complete when we saw the signal?) or else the new snapshot
      // is deferred.
      // Observed in logs:
      // "Skipping retention at seqNr [16] because previous retention has not completed yet. Next retention will cover skipped retention."
      Thread.sleep(50)

      persistentActor ! Increment // 15
      persistentActor ! Increment // 16
      snapshotSignalProbe.expectSnapshotCompleted(16) // every-2 through criteria
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 10
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(7)

      eventProbe.within(3.seconds) {
        eventProbe.expectNoMessage()
        snapshotSignalProbe.expectNoMessage()
      }
    }

    "be possible to snapshot every event" in {
      // very bad idea to snapshot every event, but technically possible
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val replyProbe = TestProbe[State]()

      val persistentActor = spawn(
        Behaviors.setup[Command](
          ctx =>
            counter(
              ctx,
              pid,
              snapshotSignalProbe = Some(snapshotSignalProbe.ref),
              deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref))
              .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 1, keepNSnapshots = 3))))

      (1 to 4).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(4, (0 until 4).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(1)
      snapshotSignalProbe.expectSnapshotCompleted(2)
      snapshotSignalProbe.expectSnapshotCompleted(3)
      snapshotSignalProbe.expectSnapshotCompleted(4)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(1)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(5)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(2)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(6)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(3)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(7)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(4)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(8)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(9)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(6)

      persistentActor ! Increment
      snapshotSignalProbe.expectSnapshotCompleted(10)
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(7)

      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(10, (0 until 10).toVector))
    }

    "be possible to snapshot every event withDeleteEventsOnSnapshot" in {
      // very bad idea to snapshot every event, but technically possible
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()
      val deleteSnapshotSignalProbe = TestProbe[WrappedSignal]()
      val eventProbe = TestProbe[Try[EventSourcedSignal]]()

      val persistentActor = spawn(
        Behaviors.setup[Command](ctx =>
          counter(
            ctx,
            pid,
            snapshotSignalProbe = Some(snapshotSignalProbe.ref),
            deleteSnapshotSignalProbe = Some(deleteSnapshotSignalProbe.ref),
            eventSignalProbe = Some(eventProbe.ref)).withRetention(
            RetentionCriteria.snapshotEvery(numberOfEvents = 1, keepNSnapshots = 3).withDeleteEventsOnSnapshot)))

      // one at a time since snapshotting+event-deletion switches to running state before deleting snapshot so ordering
      // if sending many commands in one go is not deterministic
      (1 to 4).foreach(_ => persistentActor ! Increment)
      snapshotSignalProbe.expectSnapshotCompleted(1)
      snapshotSignalProbe.expectSnapshotCompleted(2)
      snapshotSignalProbe.expectSnapshotCompleted(3)
      snapshotSignalProbe.expectSnapshotCompleted(4)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 1

      persistentActor ! Increment // 5
      snapshotSignalProbe.expectSnapshotCompleted(5)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 2
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(1)

      persistentActor ! Increment // 6
      snapshotSignalProbe.expectSnapshotCompleted(6)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 3
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(2)

      persistentActor ! Increment // 7
      snapshotSignalProbe.expectSnapshotCompleted(7)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 4
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(3)

      persistentActor ! Increment // 8
      snapshotSignalProbe.expectSnapshotCompleted(8)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 5
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(4)

      persistentActor ! Increment // 9
      snapshotSignalProbe.expectSnapshotCompleted(9)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 6
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(5)

      persistentActor ! Increment // 10
      snapshotSignalProbe.expectSnapshotCompleted(10)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 7
      deleteSnapshotSignalProbe.expectDeleteSnapshotCompleted(6)
    }

    "snapshot on recovery if expected snapshot is missing" in {
      val pid = nextPid()
      val snapshotSignalProbe = TestProbe[WrappedSignal]()

      {
        val persistentActor =
          spawn(Behaviors.setup[Command](ctx => counter(ctx, pid, snapshotSignalProbe = Some(snapshotSignalProbe.ref))))
        (1 to 5).foreach(_ => persistentActor ! Increment)
        snapshotSignalProbe.expectNoMessage()

        persistentActor ! StopIt
        val watchProbe = TestProbe()
        watchProbe.expectTerminated(persistentActor)
      }

      {
        val persistentActor = spawn(
          Behaviors.setup[Command](ctx =>
            counter(ctx, pid, snapshotSignalProbe = Some(snapshotSignalProbe.ref))
              .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 5, keepNSnapshots = 1))))

        val replyProbe = TestProbe[State]()
        persistentActor ! GetValue(replyProbe.ref)
        snapshotSignalProbe.expectSnapshotCompleted(5)
        replyProbe.expectMessage(State(5, Vector(0, 1, 2, 3, 4)))

        persistentActor ! StopIt
        val watchProbe = TestProbe()
        watchProbe.expectTerminated(persistentActor)
      }

      {
        val persistentActor = spawn(
          Behaviors.setup[Command](ctx =>
            counter(ctx, pid, snapshotSignalProbe = Some(snapshotSignalProbe.ref))
              .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 5, keepNSnapshots = 1))))

        val replyProbe = TestProbe[State]()
        persistentActor ! GetValue(replyProbe.ref)
        snapshotSignalProbe.expectNoMessage()
        replyProbe.expectMessage(State(5, Vector(0, 1, 2, 3, 4)))
      }
    }
  }
}
