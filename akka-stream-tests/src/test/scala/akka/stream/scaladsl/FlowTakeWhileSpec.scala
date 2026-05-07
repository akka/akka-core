/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NoStackTrace

import akka.stream.ActorAttributes._
import akka.stream.Supervision._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink

class FlowTakeWhileSpec extends StreamSpec {

  "A TakeWhile" must {

    "take while predicate is true" in {
      Source(1 to 4).takeWhile(_ < 3).runWith(TestSink[Int]()).request(3).expectNext(1, 2).expectComplete()
    }

    "complete the future for an empty stream" in {
      Source.empty[Int].takeWhile(_ < 2).runWith(TestSink[Int]()).request(1).expectComplete()
    }

    "continue if error" in {
      val testException = new Exception("test") with NoStackTrace

      Source(1 to 4)
        .takeWhile(a => if (a == 3) throw testException else true)
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(4)
        .expectNext(1, 2, 4)
        .expectComplete()
    }

    "emit the element that caused the predicate to return false and then no more with inclusive set" in {
      Source(1 to 10).takeWhile(_ < 3, true).runWith(TestSink[Int]()).request(4).expectNext(1, 2, 3).expectComplete()
    }

  }

}
