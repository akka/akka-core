/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import java.io.NotSerializableException

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKitSpec.TestCounter.{ NotSerializableState, NullState }
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.JournalFailureException
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.serialization.DisabledJavaSerializer
import akka.serialization.jackson.CborSerializable

object EventSourcedBehaviorTestKitSpec {

  object TestCounter {
    sealed trait Command
    case object Increment extends Command with CborSerializable
    final case class IncrementWithConfirmation(replyTo: ActorRef[Done]) extends Command with CborSerializable
    final case class IncrementWithNoReply(replyTo: ActorRef[Done]) extends Command with CborSerializable
    final case class IncrementWithAsyncReply(replyTo: ActorRef[Done]) extends Command with CborSerializable
    case class IncrementSeveral(n: Int) extends Command with CborSerializable
    final case class GetValue(replyTo: ActorRef[State]) extends Command with CborSerializable

    private case class AsyncReply(replyTo: ActorRef[Done]) extends Command with CborSerializable

    sealed trait Event
    final case class Incremented(delta: Int) extends Event with CborSerializable

    sealed trait State
    final case class RealState(value: Int, history: Vector[Int]) extends State with CborSerializable
    final case class NullState() extends State with CborSerializable

    case object IncrementWithNotSerializableEvent extends Command with CborSerializable
    final case class NotSerializableEvent(delta: Int) extends Event

    case object IncrementWithNotSerializableState extends Command with CborSerializable
    final case class IncrementedWithNotSerializableState(delta: Int) extends Event with CborSerializable
    final case class NotSerializableState(value: Int, history: Vector[Int]) extends State

    case object NotSerializableCommand extends Command

    final case class IncrementWithNotSerializableReply(replyTo: ActorRef[NotSerializableReply])
        extends Command
        with CborSerializable
    class NotSerializableReply

    def apply(persistenceId: PersistenceId): Behavior[Command] =
      apply(persistenceId, RealState(0, Vector.empty))

    def apply(persistenceId: PersistenceId, emptyState: State): Behavior[Command] =
      Behaviors.setup(ctx => counter(ctx, persistenceId, emptyState))

    private def counter(
        ctx: ActorContext[Command],
        persistenceId: PersistenceId,
        emptyState: State): EventSourcedBehavior[Command, Event, State] = {
      EventSourcedBehavior.withEnforcedReplies[Command, Event, State](
        persistenceId,
        emptyState,
        commandHandler = (state, command) =>
          command match {
            case Increment =>
              Effect.persist(Incremented(1)).thenNoReply()

            case IncrementWithConfirmation(replyTo) =>
              Effect.persist(Incremented(1)).thenReply(replyTo)(_ => Done)

            case IncrementWithAsyncReply(replyTo) =>
              ctx.self ! AsyncReply(replyTo)
              Effect.noReply

            case AsyncReply(replyTo) =>
              Effect.persist(Incremented(1)).thenReply(replyTo)(_ => Done)

            case IncrementWithNoReply(_) =>
              Effect.persist(Incremented(1)).thenNoReply()

            case IncrementSeveral(n: Int) =>
              val events = (1 to n).map(_ => Incremented(1))
              Effect.persist(events).thenNoReply()

            case IncrementWithNotSerializableEvent =>
              Effect.persist(NotSerializableEvent(1)).thenNoReply()

            case IncrementWithNotSerializableState =>
              Effect.persist(IncrementedWithNotSerializableState(1)).thenNoReply()

            case IncrementWithNotSerializableReply(replyTo) =>
              Effect.persist(Incremented(1)).thenReply(replyTo)(_ => new NotSerializableReply)

            case NotSerializableCommand =>
              Effect.noReply

            case GetValue(replyTo) =>
              Effect.reply(replyTo)(state)

          },
        eventHandler = {
          case (RealState(value, history), Incremented(delta)) =>
            if (delta <= 0)
              throw new IllegalStateException("Delta must be positive")
            RealState(value + delta, history :+ value)
          case (RealState(value, history), NotSerializableEvent(delta)) =>
            RealState(value + delta, history :+ value)
          case (RealState(value, history), IncrementedWithNotSerializableState(delta)) =>
            NotSerializableState(value + delta, history :+ value)
          case (state: NotSerializableState, _) =>
            throw new IllegalStateException(state.toString)
          case (null, _)        => NullState()
          case (NullState(), _) => NullState()
        })
    }
  }
}

class EventSourcedBehaviorTestKitSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorTestKitSpec._

  private val persistenceId = PersistenceId.ofUniqueId("test")
  private val behavior = TestCounter(persistenceId)

  private def createTestKitNull() = {
    EventSourcedBehaviorTestKit[TestCounter.Command, TestCounter.Event, TestCounter.State](
      system,
      TestCounter(persistenceId, null))
  }

  private def createTestKit() = {
    EventSourcedBehaviorTestKit[TestCounter.Command, TestCounter.Event, TestCounter.State](system, behavior)
  }

  "EventSourcedBehaviorTestKit" must {

    "handle null state" in {
      val eventSourcedTestKit = createTestKitNull()
      val result = eventSourcedTestKit.runCommand(TestCounter.Increment)
      result.state shouldBe NullState()
    }

    "run commands" in {
      val eventSourcedTestKit = createTestKit()

      val result1 = eventSourcedTestKit.runCommand(TestCounter.Increment)
      result1.event should ===(TestCounter.Incremented(1))
      result1.state should ===(TestCounter.RealState(1, Vector(0)))

      val result2 = eventSourcedTestKit.runCommand(TestCounter.Increment)
      result2.event should ===(TestCounter.Incremented(1))
      result2.state should ===(TestCounter.RealState(2, Vector(0, 1)))

      result2.eventOfType[TestCounter.Incremented].delta should ===(1)
      intercept[AssertionError] {
        // wrong event type
        result2.eventOfType[TestCounter.NotSerializableEvent].delta should ===(1)
      }
    }

    "run command emitting several events" in {
      val eventSourcedTestKit = createTestKit()

      val result = eventSourcedTestKit.runCommand(TestCounter.IncrementSeveral(3))
      result.events should ===(List(TestCounter.Incremented(1), TestCounter.Incremented(1), TestCounter.Incremented(1)))
      result.state should ===(TestCounter.RealState(3, Vector(0, 1, 2)))
    }

    "run commands with reply" in {
      val eventSourcedTestKit = createTestKit()

      val result1 = eventSourcedTestKit.runCommand[Done](TestCounter.IncrementWithConfirmation(_))
      result1.event should ===(TestCounter.Incremented(1))
      result1.state should ===(TestCounter.RealState(1, Vector(0)))
      result1.reply should ===(Done)

      val result2 = eventSourcedTestKit.runCommand[Done](TestCounter.IncrementWithConfirmation(_))
      result2.event should ===(TestCounter.Incremented(1))
      result2.state should ===(TestCounter.RealState(2, Vector(0, 1)))
      result2.reply should ===(Done)
    }

    "detect missing reply" in {
      val eventSourcedTestKit = createTestKit()

      intercept[AssertionError] {
        eventSourcedTestKit.runCommand[Done](_ => TestCounter.Increment)
      }.getMessage should include("Missing expected reply")
    }

    "run command with reply that is not emitting events" in {
      val eventSourcedTestKit = createTestKit()

      eventSourcedTestKit.runCommand(TestCounter.Increment)
      val result = eventSourcedTestKit.runCommand[TestCounter.State](TestCounter.GetValue(_))
      result.hasNoEvents should ===(true)
      intercept[AssertionError] {
        result.event
      }
      result.state should ===(TestCounter.RealState(1, Vector(0)))
    }

    "run command with async reply" in {
      val eventSourcedTestKit = createTestKit()
      val result = eventSourcedTestKit.runCommand[Done](replyTo => TestCounter.IncrementWithAsyncReply(replyTo))
      result.event should ===(TestCounter.Incremented(1))
      result.reply should ===(Done)
      result.state should ===(TestCounter.RealState(1, Vector(0)))
    }

    "run command with no reply" in {
      // This is a fictive usage scenario. If the command has a replyTo the Behavior should (eventually) reply,
      // but here we verify that it doesn't reply.
      val eventSourcedTestKit = createTestKit()
      val replyToProbe = createTestProbe[Done]()
      val result = eventSourcedTestKit.runCommand(TestCounter.IncrementWithNoReply(replyToProbe.ref))
      result.event should ===(TestCounter.Incremented(1))
      result.state should ===(TestCounter.RealState(1, Vector(0)))
      replyToProbe.expectNoMessage()
    }

    "give access to current state" in {
      val eventSourcedTestKit = createTestKit()

      // initial state
      eventSourcedTestKit.getState() should ===(TestCounter.RealState(0, Vector.empty))

      // state after command
      eventSourcedTestKit.runCommand(TestCounter.Increment)
      eventSourcedTestKit.getState() should ===(TestCounter.RealState(1, Vector(0)))
    }

    "detect non-serializable events" in {
      val eventSourcedTestKit = createTestKit()

      val exc = intercept[IllegalArgumentException] {
        eventSourcedTestKit.runCommand(TestCounter.IncrementWithNotSerializableEvent)
      }
      (exc.getMessage should startWith).regex("Event.*isn't serializable")
      exc.getCause.getClass should ===(classOf[DisabledJavaSerializer.JavaSerializationException])
    }

    "detect non-serializable state" in {
      val eventSourcedTestKit = createTestKit()

      val exc = intercept[IllegalArgumentException] {
        eventSourcedTestKit.runCommand(TestCounter.IncrementWithNotSerializableState)
      }
      (exc.getMessage should include).regex("State.*isn't serializable")
      exc.getCause.getClass should ===(classOf[DisabledJavaSerializer.JavaSerializationException])
    }

    "detect non-serializable empty state" in {
      val eventSourcedTestKit =
        EventSourcedBehaviorTestKit[TestCounter.Command, TestCounter.Event, TestCounter.State](
          system,
          TestCounter(persistenceId, NotSerializableState(0, Vector.empty)))

      val exc = intercept[IllegalArgumentException] {
        eventSourcedTestKit.runCommand(TestCounter.Increment)
      }
      (exc.getMessage should include).regex("Empty State.*isn't serializable")
      exc.getCause.getClass should ===(classOf[DisabledJavaSerializer.JavaSerializationException])
    }

    "detect non-serializable command" in {
      val eventSourcedTestKit = createTestKit()

      val exc = intercept[IllegalArgumentException] {
        eventSourcedTestKit.runCommand(TestCounter.NotSerializableCommand)
      }
      (exc.getMessage should include).regex("Command.*isn't serializable")
      exc.getCause.getClass should ===(classOf[DisabledJavaSerializer.JavaSerializationException])
    }

    "detect non-serializable reply" in {
      val eventSourcedTestKit = createTestKit()

      val exc = intercept[IllegalArgumentException] {
        eventSourcedTestKit.runCommand(replyTo => TestCounter.IncrementWithNotSerializableReply(replyTo))
      }
      (exc.getMessage should include).regex("Reply.*isn't serializable")
      exc.getCause.getClass should ===(classOf[NotSerializableException])
    }

    "support test of replay" in {
      val eventSourcedTestKit = createTestKit()

      eventSourcedTestKit.runCommand(TestCounter.Increment)
      eventSourcedTestKit.runCommand(TestCounter.Increment)
      val expectedState = TestCounter.RealState(3, Vector(0, 1, 2))
      eventSourcedTestKit.runCommand(TestCounter.Increment).state should ===(expectedState)
    }

    "support test of replay from stored events" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.persistenceTestKit
        .persistForRecovery(persistenceId.id, List(TestCounter.Incremented(1), TestCounter.Incremented(1)))
      eventSourcedTestKit.restart().state should ===(TestCounter.RealState(2, Vector(0, 1)))
    }

    "support test of invalid events" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.persistenceTestKit
        .persistForRecovery(persistenceId.id, List(TestCounter.Incremented(1), TestCounter.Incremented(-1)))
      intercept[IllegalStateException] {
        eventSourcedTestKit.restart()
      }

    }

    "only allow EventSourcedBehavior" in {
      intercept[IllegalArgumentException] {
        EventSourcedBehaviorTestKit[TestCounter.Command, TestCounter.Event, TestCounter.State](
          system,
          Behaviors.empty[TestCounter.Command])
      }
    }

    "support test of failures" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.runCommand(TestCounter.Increment)
      eventSourcedTestKit.runCommand(TestCounter.Increment)
      eventSourcedTestKit.persistenceTestKit.failNextPersisted(persistenceId.id, TestException("DB err"))
      LoggingTestKit.error[JournalFailureException].expect {
        intercept[AssertionError] {
          eventSourcedTestKit.runCommand(TestCounter.Increment)
        }
      }

      eventSourcedTestKit.restart().state should ===(TestCounter.RealState(2, Vector(0, 1)))
      eventSourcedTestKit.runCommand(TestCounter.Increment).state should ===(TestCounter.RealState(3, Vector(0, 1, 2)))
    }

    "have possibility to clear" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.runCommand(TestCounter.Increment)
      eventSourcedTestKit.runCommand(TestCounter.Increment).state should ===(TestCounter.RealState(2, Vector(0, 1)))

      eventSourcedTestKit.clear()
      eventSourcedTestKit.runCommand(TestCounter.Increment).state should ===(TestCounter.RealState(1, Vector(0)))
    }

    "initialize from snapshot" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.initialize(TestCounter.RealState(1, Vector(0)))

      val result = eventSourcedTestKit.runCommand[TestCounter.State](TestCounter.GetValue(_))
      result.reply shouldEqual TestCounter.RealState(1, Vector(0))
    }

    "initialize from event" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.initialize(TestCounter.Incremented(1))

      val result = eventSourcedTestKit.runCommand[TestCounter.State](TestCounter.GetValue(_))
      result.reply shouldEqual TestCounter.RealState(1, Vector(0))
    }

    "initialize from snapshot and event" in {
      val eventSourcedTestKit = createTestKit()
      eventSourcedTestKit.initialize(TestCounter.RealState(1, Vector(0)), TestCounter.Incremented(1))

      val result = eventSourcedTestKit.runCommand[TestCounter.State](TestCounter.GetValue(_))
      result.reply shouldEqual TestCounter.RealState(2, Vector(0, 1))
    }
  }

}
