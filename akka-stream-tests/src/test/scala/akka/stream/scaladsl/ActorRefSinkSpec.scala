/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NoStackTrace

import akka.actor.{ Actor, ActorRef, Props }
import akka.stream.testkit._
import akka.stream.testkit.scaladsl._
import akka.testkit.TestProbe

object ActorRefSinkSpec {
  case class Fw(ref: ActorRef) extends Actor {
    def receive = {
      case msg => ref.forward(msg)
    }
  }

  val te = new RuntimeException("oh dear") with NoStackTrace
}

class ActorRefSinkSpec extends StreamSpec {
  import ActorRefSinkSpec._

  "A ActorRefSink" must {

    "send the elements to the ActorRef" in {
      Source(List(1, 2, 3)).runWith(Sink.actorRef(testActor, onCompleteMessage = "done", _ => "failure"))
      expectMsg(1)
      expectMsg(2)
      expectMsg(3)
      expectMsg("done")
    }

    "cancel stream when actor terminates" in {
      val fw = system.actorOf(Props(classOf[Fw], testActor).withDispatcher("akka.test.stream-dispatcher"))
      val publisher =
        TestSource[Int]()
          .to(Sink.actorRef(fw, onCompleteMessage = "done", _ => "failure"))
          .run()
          .sendNext(1)
          .sendNext(2)
      expectMsg(1)
      expectMsg(2)
      system.stop(fw)
      publisher.expectCancellation()
    }

    "sends error message if upstream fails" in {
      val actorProbe = TestProbe()
      val probe = TestSource[String]().to(Sink.actorRef(actorProbe.ref, "complete", _ => "failure")).run()
      probe.sendError(te)
      actorProbe.expectMsg("failure")
    }
  }

}
