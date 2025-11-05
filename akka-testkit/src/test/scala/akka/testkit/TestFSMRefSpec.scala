/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import scala.concurrent.duration._

import language.postfixOps

import akka.actor._

class TestFSMRefSpec extends AkkaSpec {

  "A TestFSMRef" must {

    "allow access to state data" in {
      val fsm = TestFSMRef(new Actor with FSM[Int, String] {
        startWith(1, "")
        when(1) {
          case Event("go", _)         => goto(2).using("go")
          case Event(StateTimeout, _) => goto(2).using("timeout")
        }
        when(2) {
          case Event("back", _) => goto(1).using("back")
        }
      }, "test-fsm-ref-1")
      fsm.stateName should ===(1)
      fsm.stateData should ===("")
      fsm ! "go"
      fsm.stateName should ===(2)
      fsm.stateData should ===("go")
      fsm.setState(stateName = 1)
      fsm.stateName should ===(1)
      fsm.stateData should ===("go")
      fsm.setState(stateData = "buh")
      fsm.stateName should ===(1)
      fsm.stateData should ===("buh")
      fsm.setState(timeout = 100 millis)
      within(80 millis, 500 millis) {
        awaitCond(fsm.stateName == 2 && fsm.stateData == "timeout")
      }
    }

    "allow access to timers" in {
      val fsm = TestFSMRef(new Actor with FSM[Int, Null] {
        startWith(1, null)
        when(1) {
          case _ => stay()
        }
      }, "test-fsm-ref-2")
      fsm.isTimerActive("test") should ===(false)
      fsm.startTimerWithFixedDelay("test", 12, 10 millis)
      fsm.isTimerActive("test") should ===(true)
      fsm.cancelTimer("test")
      fsm.isTimerActive("test") should ===(false)
    }
  }

  "A TestFSMRef Companion Object" must {

    val guardian = system.asInstanceOf[ActorSystemImpl].guardian

    val parent = system.actorOf(Props(new Actor { def receive = { case _ => } }))

    class TestFSMActor extends Actor with FSM[Int, Null] {
      startWith(1, null)
      when(1) {
        case _ => stay()
      }
      val supervisor = context.parent
      val name = context.self.path.name
    }

    def fsmActorFactory = new TestFSMActor

    "allow creation of a TestFSMRef with a default supervisor" in {
      val fsm = TestFSMRef(fsmActorFactory)
      fsm.underlyingActor.supervisor should be(guardian)
    }

    "allow creation of a TestFSMRef with a specified name" in {
      val fsm = TestFSMRef(fsmActorFactory, "fsmActor")
      fsm.underlyingActor.name should be("fsmActor")
    }

    "allow creation of a TestFSMRef with a specified supervisor" in {
      val fsm = TestFSMRef(fsmActorFactory, parent)
      fsm.underlyingActor.supervisor should be(parent)
    }

    "allow creation of a TestFSMRef with a specified supervisor and name" in {
      val fsm = TestFSMRef(fsmActorFactory, parent, "supervisedFsmActor")
      fsm.underlyingActor.supervisor should be(parent)
      fsm.underlyingActor.name should be("supervisedFsmActor")
    }
  }
}
