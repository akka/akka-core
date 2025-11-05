/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.TimeoutException

import scala.util.control.NoStackTrace

import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink

class OnErrorCompleteSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  val ex = new RuntimeException("ex") with NoStackTrace

  "A CompleteOn" must {
    "can complete with all exceptions" in {
      Source(List(1, 2))
        .map { a =>
          if (a == 2) throw ex else a
        }
        .onErrorComplete[Throwable]()
        .runWith(TestSink[Int]())
        .request(2)
        .expectNext(1)
        .expectComplete()
    }

    "can complete with dedicated exception type" in {
      Source(List(1, 2))
        .map { a =>
          if (a == 2) throw new IllegalArgumentException() else a
        }
        .onErrorComplete[IllegalArgumentException]()
        .runWith(TestSink[Int]())
        .request(2)
        .expectNext(1)
        .expectComplete()
    }

    "can fail if an unexpected exception occur" in {
      Source(List(1, 2))
        .map { a =>
          if (a == 2) throw new IllegalArgumentException() else a
        }
        .onErrorComplete[TimeoutException]()
        .runWith(TestSink[Int]())
        .request(1)
        .expectNext(1)
        .request(1)
        .expectError()
    }

    "can complete if the pf is applied" in {
      Source(List(1, 2))
        .map { a =>
          if (a == 2) throw new TimeoutException() else a
        }
        .onErrorComplete {
          case _: IllegalArgumentException => false
          case _: TimeoutException         => true
        }
        .runWith(TestSink[Int]())
        .request(2)
        .expectNext(1)
        .expectComplete()
    }

    "can fail if the pf is not applied" in {
      Source(List(1, 2))
        .map { a =>
          if (a == 2) throw ex else a
        }
        .onErrorComplete {
          case _: IllegalArgumentException => false
          case _: TimeoutException         => true
        }
        .runWith(TestSink[Int]())
        .request(2)
        .expectNext(1)
        .expectError()
    }

  }
}
