/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.actor.ActorRef
import akka.stream.ActorAttributes._
import akka.stream.Attributes._
import akka.stream.testkit.StreamSpec
import akka.testkit.TestProbe

object FlowSectionSpec {
  val config =
    s"""
      my-dispatcher1 = $${akka.test.stream-dispatcher}
      my-dispatcher2 = $${akka.test.stream-dispatcher}
    """
}

class FlowSectionSpec extends StreamSpec(FlowSectionSpec.config) {

  "A flow".can {

    "have an op with a different dispatcher" in {
      val flow = Flow[Int].map(sendThreadNameTo(testActor)).withAttributes(dispatcher("my-dispatcher1"))

      Source.single(1).via(flow).to(Sink.ignore).run()

      expectMsgType[String] should include("my-dispatcher1")
    }

    "have a nested flow with a different dispatcher" in {
      Source
        .single(1)
        .via(Flow[Int].map(sendThreadNameTo(testActor)).withAttributes(dispatcher("my-dispatcher1")))
        .to(Sink.ignore)
        .run()

      expectMsgType[String] should include("my-dispatcher1")
    }

    "have multiple levels of nesting" in {

      val probe1 = TestProbe()
      val probe2 = TestProbe()

      val flow1 = Flow[Int].map(sendThreadNameTo(probe1.ref)).withAttributes(dispatcher("my-dispatcher1"))

      val flow2 = flow1.via(Flow[Int].map(sendThreadNameTo(probe2.ref))).withAttributes(dispatcher("my-dispatcher2"))

      Source.single(1).via(flow2).to(Sink.ignore).run()

      probe1.expectMsgType[String] should include("my-dispatcher1")
      probe2.expectMsgType[String] should include("my-dispatcher2")

    }

    "include name in toString" in {
      pending //FIXME: Flow has no simple toString anymore
      val n = "Uppercase reverser"
      val f1 = Flow[String].map(_.toLowerCase)
      val f2 = Flow[String].map(_.toUpperCase).map(_.reverse).named(n).map(_.toLowerCase)

      f1.via(f2).toString should include(n)
    }

    "have an op section with a different dispatcher and name" in {
      val defaultDispatcher = TestProbe()
      val customDispatcher = TestProbe()

      val f1 = Flow[Int].map(sendThreadNameTo(defaultDispatcher.ref))
      val f2 = Flow[Int]
        .map(sendThreadNameTo(customDispatcher.ref))
        .map(x => x)
        .withAttributes(dispatcher("my-dispatcher1") and name("separate-disptacher"))

      Source(0 to 2).via(f1).via(f2).runWith(Sink.ignore)

      defaultDispatcher.receiveN(3).foreach {
        case s: String  => s should include("akka.test.stream-dispatcher")
        case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
      }

      customDispatcher.receiveN(3).foreach {
        case s: String  => s should include("my-dispatcher1")
        case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
      }
    }

    def sendThreadNameTo[T](probe: ActorRef)(element: T) = {
      probe ! Thread.currentThread.getName
      element
    }

  }

}
