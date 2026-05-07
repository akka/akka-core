/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NoStackTrace

import akka.Done
import akka.stream.testkit._
import akka.stream.testkit.Utils._

class FlowWireTapSpec extends StreamSpec("akka.stream.materializer.debug.fuzzing-mode = off") {

  import system.dispatcher

  "A wireTap" must {

    "call the procedure for each element" in {
      Source(1 to 100).wireTap(testActor ! _).runWith(Sink.ignore).futureValue
      (1 to 100).foreach { i =>
        expectMsg(i)
      }
    }

    "complete the future for an empty stream" in {
      Source.empty[String].wireTap(testActor ! _).runWith(Sink.ignore).foreach { _ =>
        testActor ! "done"
      }
      expectMsg("done")
    }

    "yield the first error" in {
      val p = TestPublisher.manualProbe[Int]()
      Source.fromPublisher(p).wireTap(testActor ! _).runWith(Sink.ignore).failed.foreach { ex =>
        testActor ! ex
      }
      val proc = p.expectSubscription()
      proc.expectRequest()
      val rte = new RuntimeException("ex") with NoStackTrace
      proc.sendError(rte)
      expectMsg(rte)
    }

    "not cause subsequent stages to be failed if throws (same as wireTap(Sink))" in {
      val error = TE("Boom!")
      val future = Source.single(1).wireTap(_ => throw error).runWith(Sink.ignore)
      future.futureValue shouldEqual Done
    }
  }

}
