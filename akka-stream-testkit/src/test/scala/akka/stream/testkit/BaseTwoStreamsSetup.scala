/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit

import scala.collection.immutable
import scala.util.control.NoStackTrace

import org.reactivestreams.Publisher

import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit._
import akka.testkit.AkkaSpec

abstract class BaseTwoStreamsSetup extends AkkaSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  val TestException = new RuntimeException("test") with NoStackTrace

  type Outputs

  def setup(p1: Publisher[Int], p2: Publisher[Int]): TestSubscriber.Probe[Outputs]

  def failedPublisher[T]: Publisher[T] = TestPublisher.error[T](TestException)

  def completedPublisher[T]: Publisher[T] = TestPublisher.empty[T]()

  def nonemptyPublisher[T](elems: immutable.Iterable[T]): Publisher[T] = Source(elems).runWith(Sink.asPublisher(false))

  def soonToFailPublisher[T]: Publisher[T] = TestPublisher.lazyError[T](TestException)

  def soonToCompletePublisher[T]: Publisher[T] = TestPublisher.lazyEmpty[T]

  def commonTests() = {
    "work with two immediately completed publishers" in assertAllStagesStopped {
      val subscriber = setup(completedPublisher, completedPublisher)
      subscriber.expectSubscriptionAndComplete()
    }

    "work with two delayed completed publishers" in assertAllStagesStopped {
      val subscriber = setup(soonToCompletePublisher, soonToCompletePublisher)
      subscriber.expectSubscriptionAndComplete()
    }

    "work with one immediately completed and one delayed completed publisher" in assertAllStagesStopped {
      val subscriber = setup(completedPublisher, soonToCompletePublisher)
      subscriber.expectSubscriptionAndComplete()
    }

    "work with two immediately failed publishers" in assertAllStagesStopped {
      val subscriber = setup(failedPublisher, failedPublisher)
      subscriber.expectSubscriptionAndError(TestException)
    }

    "work with two delayed failed publishers" in assertAllStagesStopped {
      val subscriber = setup(soonToFailPublisher, soonToFailPublisher)
      subscriber.expectSubscriptionAndError(TestException)
    }

    // Warning: The two test cases below are somewhat implementation specific and might fail if the implementation
    // is changed. They are here to be an early warning though.
    "work with one immediately failed and one delayed failed publisher (case 1)" in assertAllStagesStopped {
      val subscriber = setup(soonToFailPublisher, failedPublisher)
      subscriber.expectSubscriptionAndError(TestException)
    }

    "work with one immediately failed and one delayed failed publisher (case 2)" in assertAllStagesStopped {
      val subscriber = setup(failedPublisher, soonToFailPublisher)
      subscriber.expectSubscriptionAndError(TestException)
    }
  }

}
