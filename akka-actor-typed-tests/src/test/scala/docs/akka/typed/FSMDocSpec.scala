/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import org.scalatest.wordspec.AnyWordSpecLike

object FSMDocSpec {

  //#simple-state
  //#simple-events
  object Buncher {
    //#simple-state

    // FSM event becomes the type of the message Actor supports
    sealed trait Event
    final case class SetTarget(ref: ActorRef[Batch]) extends Event
    final case class Queue(obj: Any) extends Event
    case object Flush extends Event
    private case object Timeout extends Event
    //#simple-events

    //#storing-state
    sealed trait Data
    case object Uninitialized extends Data
    final case class Todo(target: ActorRef[Batch], queue: immutable.Seq[Any]) extends Data

    final case class Batch(obj: immutable.Seq[Any])
    //#storing-state

    //#simple-state
    // states of the FSM represented as behaviors

    // initial state
    def apply(): Behavior[Event] = idle(Uninitialized)

    private def idle(data: Data): Behavior[Event] = Behaviors.receiveMessage[Event] { message =>
      (message, data) match {
        case (SetTarget(ref), Uninitialized) =>
          idle(Todo(ref, Vector.empty))
        case (Queue(obj), t @ Todo(_, v)) =>
          active(t.copy(queue = v :+ obj))
        case _ =>
          Behaviors.unhandled
      }
    }

    private def active(data: Todo): Behavior[Event] =
      Behaviors.withTimers[Event] { timers =>
        // instead of FSM state timeout
        timers.startSingleTimer(Timeout, 1.second)
        Behaviors.receiveMessagePartial {
          case Flush | Timeout =>
            data.target ! Batch(data.queue)
            idle(data.copy(queue = Vector.empty))
          case Queue(obj) =>
            active(data.copy(queue = data.queue :+ obj))
        }
      }

    //#simple-events
  }
  //#simple-events
  //#simple-state
}

class FSMDocSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import FSMDocSpec._

  "FSMDocSpec" must {
    "work" in {
      val buncher = spawn(Buncher())
      val probe = TestProbe[Buncher.Batch]()
      buncher ! Buncher.SetTarget(probe.ref)
      buncher ! Buncher.Queue(42)
      buncher ! Buncher.Queue(43)
      probe.expectMessage(Buncher.Batch(immutable.Seq(42, 43)))
      buncher ! Buncher.Queue(44)
      buncher ! Buncher.Flush
      buncher ! Buncher.Queue(45)
      probe.expectMessage(Buncher.Batch(immutable.Seq(44)))
      probe.expectMessage(Buncher.Batch(immutable.Seq(45)))

    }
  }
}
