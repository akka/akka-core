/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.testkit._

object FSMTransitionSpec {

  class Supervisor extends Actor {
    def receive = { case _ => }
  }

  class SendAnyTransitionFSM(target: ActorRef) extends Actor with FSM[Int, Int] {
    startWith(0, 0)
    when(0) {
      case Event("stay", _) => stay()
      case Event(_, _)      => goto(0)
    }
    onTransition { case from -> to => target ! (from -> to) }

    initialize()
  }

  class MyFSM(target: ActorRef) extends Actor with FSM[Int, Unit] {
    startWith(0, ())
    when(0) {
      case Event("tick", _) => goto(1)
    }
    when(1) {
      case Event("tick", _) => goto(0)
    }
    whenUnhandled {
      case Event("reply", _) => stay().replying("reply")
    }
    initialize()
    override def preRestart(reason: Throwable, msg: Option[Any]): Unit = { target ! "restarted" }
  }

  class OtherFSM(target: ActorRef) extends Actor with FSM[Int, Int] {
    startWith(0, 0)
    when(0) {
      case Event("tick", _) => goto(1).using(1)
      case Event("stay", _) => stay()
    }
    when(1) {
      case _ => goto(1)
    }
    onTransition {
      case 0 -> 1 => target ! ((stateData, nextStateData))
      case 1 -> 1 => target ! ((stateData, nextStateData))
    }
  }

  class Forwarder(target: ActorRef) extends Actor {
    def receive = { case x => target ! x }
  }

}

class FSMTransitionSpec extends AkkaSpec with ImplicitSender {

  import FSMTransitionSpec._

  "A FSM transition notifier" must {

    "not trigger onTransition for stay" in {
      val fsm = system.actorOf(Props(new SendAnyTransitionFSM(testActor)))
      expectMsg(0 -> 0) // caused by initialize(), OK.
      fsm ! "stay" // no transition event
      expectNoMessage(500.millis)
      fsm ! "goto" // goto(current state)
      expectMsg(0 -> 0)
    }

    "notify listeners" in {
      import FSM.{ CurrentState, SubscribeTransitionCallBack, Transition }

      val fsm = system.actorOf(Props(new MyFSM(testActor)))
      within(1 second) {
        fsm ! SubscribeTransitionCallBack(testActor)
        expectMsg(CurrentState(fsm, 0))
        fsm ! "tick"
        expectMsg(Transition(fsm, 0, 1))
        fsm ! "tick"
        expectMsg(Transition(fsm, 1, 0))
      }
    }

    "not fail when listener goes away" in {
      val forward = system.actorOf(Props(new Forwarder(testActor)))
      val fsm = system.actorOf(Props(new MyFSM(testActor)))

      within(1 second) {
        fsm ! FSM.SubscribeTransitionCallBack(forward)
        expectMsg(FSM.CurrentState(fsm, 0))
        akka.pattern.gracefulStop(forward, 5 seconds)
        fsm ! "tick"
        expectNoMessage()
      }
    }
  }

  "A FSM" must {

    "make previous and next state data available in onTransition" in {
      val fsm = system.actorOf(Props(new OtherFSM(testActor)))
      within(1 second) {
        fsm ! "tick"
        expectMsg((0, 1))
      }
    }

    "trigger transition event when goto() the same state" in {
      import FSM.Transition
      val forward = system.actorOf(Props(new Forwarder(testActor)))
      val fsm = system.actorOf(Props(new OtherFSM(testActor)))

      within(1 second) {
        fsm ! FSM.SubscribeTransitionCallBack(forward)
        expectMsg(FSM.CurrentState(fsm, 0))
        fsm ! "tick"
        expectMsg((0, 1))
        expectMsg(Transition(fsm, 0, 1))
        fsm ! "tick"
        expectMsg((1, 1))
        expectMsg(Transition(fsm, 1, 1))
      }
    }

    "not trigger transition event on stay()" in {
      val forward = system.actorOf(Props(new Forwarder(testActor)))
      val fsm = system.actorOf(Props(new OtherFSM(testActor)))

      within(1 second) {
        fsm ! FSM.SubscribeTransitionCallBack(forward)
        expectMsg(FSM.CurrentState(fsm, 0))
        fsm ! "stay"
        expectNoMessage()
      }
    }

    "not leak memory in nextState" in {
      val fsmref = system.actorOf(Props(new Actor with FSM[Int, ActorRef] {
        startWith(0, null)
        when(0) {
          case Event("switch", _) => goto(1).using(sender())
        }
        onTransition {
          case x -> y => nextStateData ! (x -> y)
        }
        when(1) {
          case Event("test", _) =>
            try {
              sender() ! s"failed: $nextStateData"
            } catch {
              case _: IllegalStateException => sender() ! "ok"
            }
            stay()
        }
      }))
      fsmref ! "switch"
      expectMsg((0, 1))
      fsmref ! "test"
      expectMsg("ok")
    }
  }

}
