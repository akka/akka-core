/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import akka.actor.{ Actor, Props }
import akka.testkit.{ AkkaSpec, DefaultTimeout }

object ControlAwareDispatcherSpec {
  val config = """
    unbounded-control-dispatcher {
      mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
    }
    bounded-control-dispatcher {
      mailbox-type = "akka.dispatch.BoundedControlAwareMailbox"
    }
    """

  case object ImportantMessage extends ControlMessage
}

class ControlAwareDispatcherSpec extends AkkaSpec(ControlAwareDispatcherSpec.config) with DefaultTimeout {
  import ControlAwareDispatcherSpec.ImportantMessage

  "A ControlAwareDispatcher" must {
    "deliver control messages first using an unbounded mailbox" in {
      val dispatcherKey = "unbounded-control-dispatcher"
      testControl(dispatcherKey)
    }

    "deliver control messages first using a bounded mailbox" in {
      val dispatcherKey = "bounded-control-dispatcher"
      testControl(dispatcherKey)
    }
  }

  def testControl(dispatcherKey: String) = {
    // It's important that the actor under test is not a top level actor
    // with RepointableActorRef, since messages might be queued in
    // UnstartedCell and the sent to the PriorityQueue and consumed immediately
    // without the ordering taking place.
    system.actorOf(Props(new Actor {
      context.actorOf(Props(new Actor {

        self ! "test"
        self ! "test2"
        self ! ImportantMessage

        def receive = {
          case x => testActor ! x
        }
      }).withDispatcher(dispatcherKey))

      def receive = Actor.emptyBehavior

    }))

    expectMsg(ImportantMessage)
    expectMsg("test")
    expectMsg("test2")
  }
}
