/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.actor.Props
import akka.testkit.AkkaSpec

class RouteeCreationSpec extends AkkaSpec {

  "Creating Routees" must {

    "result in visible routees" in {
      val N = 100
      system.actorOf(RoundRobinPool(N).props(Props(new Actor {
        system.actorSelection(self.path).tell(Identify(self.path), testActor)
        def receive = Actor.emptyBehavior
      })))
      for (i <- 1 to N) {
        expectMsgType[ActorIdentity] match {
          case ActorIdentity(_, Some(_)) => // fine
          case x                         => fail(s"routee $i was not found $x")
        }
      }
    }

    "allow sending to context.parent" in {
      val N = 100
      system.actorOf(RoundRobinPool(N).props(Props(new Actor {
        context.parent ! "one"
        def receive = {
          case "one" => testActor.forward("two")
        }
      })))
      val gotit = receiveWhile(messages = N) {
        case "two" => lastSender.toString
      }
      expectNoMessage(100.millis)
      if (gotit.size != N) {
        fail(s"got only ${gotit.size} from [${gotit.mkString(", ")}]")
      }
    }

  }

}
