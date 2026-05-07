/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NoStackTrace

import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.EventFilter

class FlowRecoverSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """) {

  val ex = new RuntimeException("ex") with NoStackTrace

  "A Recover" must {
    "recover when there is a handler" in {
      Source(1 to 4)
        .map { a =>
          if (a == 3) throw ex else a
        }
        .recover { case _: Throwable => 0 }
        .runWith(TestSink[Int]())
        .requestNext(1)
        .requestNext(2)
        .requestNext(0)
        .request(1)
        .expectComplete()
    }

    "failed stream if handler is not for such exception type" in {
      Source(1 to 3)
        .map { a =>
          if (a == 2) throw ex else a
        }
        .recover { case _: IndexOutOfBoundsException => 0 }
        .runWith(TestSink[Int]())
        .requestNext(1)
        .request(1)
        .expectError(ex)
    }

    "not influence stream when there is no exceptions" in {
      Source(1 to 3)
        .map(identity)
        .recover { case _: Throwable => 0 }
        .runWith(TestSink[Int]())
        .request(3)
        .expectNextN(1 to 3)
        .expectComplete()
    }

    "finish stream if it's empty" in {
      Source.empty.recover { case _: Throwable => 0 }.runWith(TestSink[Int]()).request(1).expectComplete()
    }

    "not log error when exception is thrown from recover block" in {
      val ex = new IndexOutOfBoundsException("quite intuitive")
      EventFilter[IndexOutOfBoundsException](occurrences = 0).intercept {
        Source
          .failed(new IllegalStateException("expected illegal state"))
          .recover { case _: IllegalStateException => throw ex }
          .runWith(TestSink[Int]())
          .expectSubscriptionAndError(ex)
      }
    }
  }
}
