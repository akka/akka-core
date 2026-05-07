/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import language.postfixOps

import akka.testkit._
import scala.concurrent.duration._

class FSMTimingSpec extends AkkaSpec with ImplicitSender {
  import FSMTimingSpec._
  import FSM._

  val fsm = system.actorOf(Props(new StateMachine(testActor)))
  fsm ! SubscribeTransitionCallBack(testActor)
  expectMsg(1 second, CurrentState(fsm, Initial))

  ignoreMsg {
    case Transition(_, bs: FSMTimingSpec.State, _) if bs eq Initial => true // SI-5900 workaround
  }

  "A Finite State Machine" must {

    "receive StateTimeout" taggedAs TimingTest in {
      within(2 seconds) {
        within(500 millis, 2 seconds) {
          fsm ! TestStateTimeout
          expectMsg(Transition(fsm, TestStateTimeout, Initial))
        }
        expectNoMessage(remainingOrDefault)
      }
    }

    "cancel a StateTimeout" taggedAs TimingTest in {
      within(1 second) {
        fsm ! TestStateTimeout
        fsm ! Cancel
        expectMsg(Cancel)
        expectMsg(Transition(fsm, TestStateTimeout, Initial))
        expectNoMessage(remainingOrDefault)
      }
    }

    "cancel a StateTimeout when actor is stopped" taggedAs TimingTest in {
      val stoppingActor = system.actorOf(Props[StoppingActor]())
      system.eventStream.subscribe(testActor, classOf[DeadLetter])
      stoppingActor ! TestStoppingActorStateTimeout
      within(400 millis) {
        expectNoMessage(remainingOrDefault)
      }
    }

    "allow StateTimeout override" taggedAs TimingTest in {
      // the timeout in state TestStateTimeout is 800 ms, then it will change to Initial
      within(400 millis) {
        fsm ! TestStateTimeoutOverride
        expectNoMessage()
      }
      within(1 second) {
        fsm ! Cancel
        expectMsg(Cancel)
        expectMsg(Transition(fsm, TestStateTimeout, Initial))
      }
    }

    "receive single-shot timer" taggedAs TimingTest in {
      within(2 seconds) {
        within(500 millis, 1 second) {
          fsm ! TestSingleTimer
          expectMsg(Tick)
          expectMsg(Transition(fsm, TestSingleTimer, Initial))
        }
        expectNoMessage()
      }
    }

    "resubmit single-shot timer" taggedAs TimingTest in {
      within(2.5 seconds) {
        within(500 millis, 1 second) {
          fsm ! TestSingleTimerResubmit
          expectMsg(Tick)
        }
        within(1 second) {
          expectMsg(Tock)
          expectMsg(Transition(fsm, TestSingleTimerResubmit, Initial))
        }
        expectNoMessage()
      }
    }

    "correctly cancel a named timer" taggedAs TimingTest in {
      fsm ! TestCancelTimer
      within(500 millis) {
        fsm ! Tick
        expectMsg(Tick)
      }
      within(300 millis, 1 second) {
        expectMsg(Tock)
      }
      fsm ! Cancel
      expectMsg(1 second, Transition(fsm, TestCancelTimer, Initial))
    }

    "not get confused between named and state timers" taggedAs TimingTest in {
      fsm ! TestCancelStateTimerInNamedTimerMessage
      fsm ! Tick
      expectMsg(500 millis, Tick)
      Thread.sleep(200) // this is ugly: need to wait for StateTimeout to be queued
      resume(fsm)
      expectMsg(
        500 millis,
        Transition(fsm, TestCancelStateTimerInNamedTimerMessage, TestCancelStateTimerInNamedTimerMessage2))
      fsm ! Cancel
      within(500 millis) {
        expectMsg(Cancel) // if this is not received, that means StateTimeout was not properly discarded
        expectMsg(Transition(fsm, TestCancelStateTimerInNamedTimerMessage2, Initial))
      }
    }

    "receive and cancel a repeated timer" taggedAs TimingTest in {
      fsm ! TestRepeatedTimer
      val seq = receiveWhile(2 seconds) {
        case Tick => Tick
      }
      (seq should have).length(5)
      within(500 millis) {
        expectMsg(Transition(fsm, TestRepeatedTimer, Initial))
      }
    }

    "notify unhandled messages" taggedAs TimingTest in {
      filterEvents(
        EventFilter.warning("unhandled event Tick in state TestUnhandled", source = fsm.path.toString, occurrences = 1),
        EventFilter.warning(
          "unhandled event Unhandled(test) in state TestUnhandled",
          source = fsm.path.toString,
          occurrences = 1)) {
        fsm ! TestUnhandled
        within(3 second) {
          fsm ! Tick
          fsm ! SetHandler
          fsm ! Tick
          expectMsg(Unhandled(Tick))
          fsm ! Unhandled("test")
          fsm ! Cancel
          expectMsg(Transition(fsm, TestUnhandled, Initial))
        }
      }
    }

  }

}

object FSMTimingSpec {

  def suspend(actorRef: ActorRef): Unit = actorRef match {
    case l: ActorRefWithCell => l.suspend()
    case _                   =>
  }

  def resume(actorRef: ActorRef): Unit = actorRef match {
    case l: ActorRefWithCell => l.resume(causedByFailure = null)
    case _                   =>
  }

  trait State
  case object Initial extends State
  case object TestStateTimeout extends State
  case object TestStateTimeoutOverride extends State
  case object TestSingleTimer extends State
  case object TestSingleTimerResubmit extends State
  case object TestRepeatedTimer extends State
  case object TestUnhandled extends State
  case object TestCancelTimer extends State
  case object TestCancelStateTimerInNamedTimerMessage extends State
  case object TestCancelStateTimerInNamedTimerMessage2 extends State
  case object TestStoppingActorStateTimeout extends State

  case object Tick
  case object Tock
  case object Cancel
  case object SetHandler

  final case class Unhandled(msg: AnyRef)

  class StateMachine(tester: ActorRef) extends Actor with FSM[State, Int] {
    import FSM._

    // need implicit system for dilated
    import context.system

    startWith(Initial, 0)
    when(Initial) {
      case Event(TestSingleTimer, _) =>
        startSingleTimer("tester", Tick, 500.millis.dilated)
        goto(TestSingleTimer)
      case Event(TestRepeatedTimer, _) =>
        startTimerWithFixedDelay("tester", Tick, 100.millis.dilated)
        goto(TestRepeatedTimer).using(4)
      case Event(TestStateTimeoutOverride, _) =>
        goto(TestStateTimeout).forMax(Duration.Inf)
      case Event(x: FSMTimingSpec.State, _) => goto(x)
    }
    when(TestStateTimeout, stateTimeout = 800.millis.dilated) {
      case Event(StateTimeout, _) => goto(Initial)
      case Event(Cancel, _)       => goto(Initial).replying(Cancel)
    }
    when(TestSingleTimer) {
      case Event(Tick, _) =>
        tester ! Tick
        goto(Initial)
    }
    onTransition {
      case Initial -> TestSingleTimerResubmit => startSingleTimer("blah", Tick, 500.millis.dilated)
    }
    when(TestSingleTimerResubmit) {
      case Event(Tick, _) =>
        tester ! Tick
        startSingleTimer("blah", Tock, 500.millis.dilated)
        stay()
      case Event(Tock, _) =>
        tester ! Tock
        goto(Initial)
    }
    when(TestCancelTimer) {
      case Event(Tick, _) =>
        startSingleTimer("hallo", Tock, 1.milli.dilated)
        TestKit.awaitCond(context.asInstanceOf[ActorCell].mailbox.hasMessages, 1.second.dilated)
        cancelTimer("hallo")
        sender() ! Tick
        startSingleTimer("hallo", Tock, 500.millis.dilated)
        stay()
      case Event(Tock, _) =>
        tester ! Tock
        stay()
      case Event(Cancel, _) =>
        cancelTimer("hallo")
        goto(Initial)
    }
    when(TestRepeatedTimer) {
      case Event(Tick, remaining) =>
        tester ! Tick
        if (remaining == 0) {
          cancelTimer("tester")
          goto(Initial)
        } else {
          stay().using(remaining - 1)
        }
    }
    when(TestCancelStateTimerInNamedTimerMessage) {
      // FSM is suspended after processing this message and resumed 500ms later
      case Event(Tick, _) =>
        suspend(self)
        startSingleTimer("named", Tock, 1.millis.dilated)
        TestKit.awaitCond(context.asInstanceOf[ActorCell].mailbox.hasMessages, 1.second.dilated)
        stay().forMax(1.millis.dilated).replying(Tick)
      case Event(Tock, _) =>
        goto(TestCancelStateTimerInNamedTimerMessage2)
    }
    when(TestCancelStateTimerInNamedTimerMessage2) {
      case Event(StateTimeout, _) =>
        goto(Initial)
      case Event(Cancel, _) =>
        goto(Initial).replying(Cancel)
    }
    when(TestUnhandled) {
      case Event(SetHandler, _) =>
        whenUnhandled {
          case Event(Tick, _) =>
            tester ! Unhandled(Tick)
            stay()
        }
        stay()
      case Event(Cancel, _) =>
        whenUnhandled(NullFunction)
        goto(Initial)
    }
  }

  class StoppingActor extends Actor with FSM[State, Int] {
    startWith(Initial, 0)

    when(Initial, 200 millis) {
      case Event(TestStoppingActorStateTimeout, _) =>
        context.stop(self)
        stay()
    }
  }

}
