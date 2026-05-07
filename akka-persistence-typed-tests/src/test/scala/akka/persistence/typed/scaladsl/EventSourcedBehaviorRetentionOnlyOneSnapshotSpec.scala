/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Success
import scala.util.Try

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.typed.DeleteEventsCompleted
import akka.persistence.typed.EventSourcedSignal
import akka.persistence.typed.PersistenceId

object EventSourcedBehaviorRetentionOnlyOneSnapshotSpec {
  private val config = ConfigFactory.parseString(s"""
    ${PersistenceTestKitSnapshotPlugin.PluginId} {
      only-one-snapshot = on
    }
    """).withFallback(PersistenceTestKitPlugin.config.withFallback(PersistenceTestKitSnapshotPlugin.config))
}

class EventSourcedBehaviorRetentionOnlyOneSnapshotSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorRetentionOnlyOneSnapshotSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorRetentionSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  "EventSourcedBehavior with retention and only-one-snapshot" must {

    "snapshot every N sequence nrs" in {
      val pid = nextPid()
      val c = spawn(Behaviors.setup[Command](ctx =>
        counter(ctx, pid).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2))))

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
            .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2))))
      probeC2.expectMessage[(State, Event)]((State(0, Vector()), Incremented(1)))

      c2 ! Increment
      snapshotProbe.expectSnapshotCompleted(2)
      c2 ! StopIt
      watchProbe.expectTerminated(c2)

      val probeC3 = TestProbe[(State, Event)]()
      val c3 = spawn(Behaviors.setup[Command](ctx =>
        counter(ctx, pid, Some(probeC3.ref)).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 2))))
      // this time it should have been snapshotted so no events to replay
      probeC3.expectNoMessage()
      c3 ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(2, Vector(0, 1)))
    }

    "not delete snapshots" in {
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
      // this is the difference compared to EventSourcedBehaviorRetentionSpec
      deleteSnapshotSignalProbe.expectNoMessage()

      (1 to 10).foreach(_ => persistentActor ! Increment)
      persistentActor ! GetValue(replyProbe.ref)
      replyProbe.expectMessage(State(20, (0 until 20).toVector))
      snapshotSignalProbe.expectSnapshotCompleted(12)
      snapshotSignalProbe.expectSnapshotCompleted(15)
      snapshotSignalProbe.expectSnapshotCompleted(18)
      // this is the difference compared to EventSourcedBehaviorRetentionSpec
      deleteSnapshotSignalProbe.expectNoMessage()

      snapshotSignalProbe.expectNoMessage()
    }

    "optionally delete old events" in {
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
            // tests the Java API as well
            RetentionCriteria.snapshotEvery(numberOfEvents = 3).withDeleteEventsOnSnapshot)))

      // one at a time since snapshotting+event-deletion switches to running state before deleting events so ordering
      // if sending many commands in one go is not deterministic

      persistentActor ! Increment // 1
      persistentActor ! Increment // 2
      persistentActor ! Increment // 3
      snapshotSignalProbe.expectSnapshotCompleted(3)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 3
      deleteSnapshotSignalProbe.expectNoMessage()

      persistentActor ! Increment // 4
      persistentActor ! Increment // 5
      persistentActor ! Increment // 6
      snapshotSignalProbe.expectSnapshotCompleted(6)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 6
      deleteSnapshotSignalProbe.expectNoMessage()

      persistentActor ! Increment // 7
      persistentActor ! Increment // 8
      persistentActor ! Increment // 9
      snapshotSignalProbe.expectSnapshotCompleted(9)
      eventProbe.expectMessageType[Success[DeleteEventsCompleted]].value.toSequenceNr shouldEqual 9
      deleteSnapshotSignalProbe.expectNoMessage()
    }

  }
}
