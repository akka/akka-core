/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import akka.testkit.{ AkkaSpec => MyFavoriteTestFrameWorkPlusAkkaTestKit }
import akka.util.ByteString

import scala.annotation.nowarn

//#test-code
import akka.actor.Props
import scala.collection.immutable

object FSMDocSpec {
  // messages and data types
  //#test-code
  import akka.actor.ActorRef
  //#simple-events
  // received events
  final case class SetTarget(ref: ActorRef)
  final case class Queue(obj: Any)
  case object Flush

  // sent events
  final case class Batch(obj: immutable.Seq[Any])
  //#simple-events
  //#simple-state
  // states
  sealed trait State
  case object Idle extends State
  case object Active extends State

  sealed trait Data
  case object Uninitialized extends Data
  final case class Todo(target: ActorRef, queue: immutable.Seq[Any]) extends Data
  //#simple-state
  //#test-code
}

@nowarn("msg=never used") // sample snippets
class FSMDocSpec extends MyFavoriteTestFrameWorkPlusAkkaTestKit {
  import FSMDocSpec._

  //#fsm-code-elided
  //#simple-imports
  import akka.actor.FSM
  import scala.concurrent.duration._
  //#simple-imports
  //#simple-fsm
  class Buncher extends FSM[State, Data] {

    //#fsm-body
    startWith(Idle, Uninitialized)

    //#when-syntax
    when(Idle) {
      case Event(SetTarget(ref), Uninitialized) =>
        stay().using(Todo(ref, Vector.empty))
    }
    //#when-syntax

    //#transition-elided
    onTransition {
      case Active -> Idle =>
        stateData match {
          case Todo(ref, queue) => ref ! Batch(queue)
          case _                => // nothing to do
        }
    }
    //#transition-elided
    //#when-syntax

    when(Active, stateTimeout = 1.second) {
      case Event(Flush | StateTimeout, t: Todo) =>
        goto(Idle).using(t.copy(queue = Vector.empty))
    }
    //#when-syntax

    //#unhandled-elided
    whenUnhandled {
      // common code for both states
      case Event(Queue(obj), t @ Todo(_, v)) =>
        goto(Active).using(t.copy(queue = v :+ obj))

      case Event(e, s) =>
        log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
        stay()
    }
    //#unhandled-elided
    //#fsm-body

    initialize()
  }
  //#simple-fsm
  object DemoCode {
    trait StateType
    case object SomeState extends StateType
    case object Processing extends StateType
    case object Error extends StateType
    case object Idle extends StateType
    case object Active extends StateType

    class Dummy extends FSM[StateType, Int] {
      class X
      val newData = 42
      object WillDo
      object Tick

      //#modifier-syntax
      when(SomeState) {
        case Event(msg, _) =>
          goto(Processing).using(newData).forMax(5.seconds).replying(WillDo)
      }
      //#modifier-syntax

      //#transition-syntax
      onTransition {
        case Idle -> Active => startTimerWithFixedDelay("timeout", Tick, 1.second)
        case Active -> _    => cancelTimer("timeout")
        case x -> Idle      => log.info("entering Idle from " + x)
      }
      //#transition-syntax

      //#alt-transition-syntax
      onTransition(handler _)

      def handler(from: StateType, to: StateType): Unit = {
        // handle it here ...
      }
      //#alt-transition-syntax

      //#stop-syntax
      when(Error) {
        case Event("stop", _) =>
          // do cleanup ...
          stop()
      }
      //#stop-syntax

      //#transform-syntax
      when(SomeState)(transform {
        case Event(bytes: ByteString, read) => stay().using(read + bytes.length)
      }.using {
        case s @ FSM.State(state, read, timeout, stopReason, replies) if read > 1000 =>
          goto(Processing)
      })
      //#transform-syntax

      //#alt-transform-syntax
      val processingTrigger: PartialFunction[State, State] = {
        case s @ FSM.State(state, read, timeout, stopReason, replies) if read > 1000 =>
          goto(Processing)
      }

      when(SomeState)(transform {
        case Event(bytes: ByteString, read) => stay().using(read + bytes.length)
      }.using(processingTrigger))
      //#alt-transform-syntax

      //#termination-syntax
      onTermination {
        case StopEvent(FSM.Normal, state, data)         => // ...
        case StopEvent(FSM.Shutdown, state, data)       => // ...
        case StopEvent(FSM.Failure(cause), state, data) => // ...
      }
      //#termination-syntax

      //#unhandled-syntax
      whenUnhandled {
        case Event(x: X, data) =>
          log.info("Received unhandled event: " + x)
          stay()
        case Event(msg, _) =>
          log.warning("Received unknown event: " + msg)
          goto(Error)
      }
      //#unhandled-syntax

    }

    //#logging-fsm
    import akka.actor.LoggingFSM
    class MyFSM extends LoggingFSM[StateType, Data] {
      //#body-elided
      override def logDepth = 12
      onTermination {
        case StopEvent(FSM.Failure(_), state, data) =>
          val lastEvents = getLog.mkString("\n\t")
          log.warning(
            "Failure in state " + state + " with data " + data + "\n" +
            "Events leading up to this point:\n\t" + lastEvents)
      }
      // ...
      //#body-elided
    }
    //#logging-fsm

  }
  //#fsm-code-elided

  "simple finite state machine" must {

    "demonstrate NullFunction" in {
      class A extends FSM[Int, Null] {
        val SomeState = 0
        //#NullFunction
        when(SomeState)(FSM.NullFunction)
        //#NullFunction
      }
    }

    "batch correctly" in {
      val buncher = system.actorOf(Props(classOf[Buncher], this))
      buncher ! SetTarget(testActor)
      buncher ! Queue(42)
      buncher ! Queue(43)
      expectMsg(Batch(immutable.Seq(42, 43)))
      buncher ! Queue(44)
      buncher ! Flush
      buncher ! Queue(45)
      expectMsg(Batch(immutable.Seq(44)))
      expectMsg(Batch(immutable.Seq(45)))
    }

    "not batch if uninitialized" in {
      val buncher = system.actorOf(Props(classOf[Buncher], this))
      buncher ! Queue(42)
      expectNoMessage()
    }
  }
}
//#test-code
