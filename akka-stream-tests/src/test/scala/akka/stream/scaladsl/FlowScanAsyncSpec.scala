/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure

import org.scalatest.matchers.should.Matchers

import akka.pattern
import akka.stream.ActorAttributes
import akka.stream.Supervision
import akka.stream.impl.ReactiveStreamsCompliance
import akka.stream.testkit._
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl._

class FlowScanAsyncSpec extends StreamSpec with Matchers {

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  "A ScanAsync" must {

    val sumScanFlow = Flow[Int].scanAsync(0) { (accumulator, next) =>
      Future(accumulator + next)
    }

    "work with a empty source" in {
      Source.empty[Int].via(sumScanFlow).runWith(TestSink[Int]()).request(1).expectNextOrComplete(0)
    }

    "complete after zero-element has been consumed" in {
      val (pub, sub) =
        TestSource[Int]()
          .via(Flow[Int].scanAsync(0)((acc, in) => Future.successful(acc + in)))
          .toMat(TestSink())(Keep.both)
          .run()

      sub.request(10)
      sub.expectNext(0)
      pub.sendComplete()
      sub.expectComplete()
    }

    "complete after stream has been consumed and pending futures resolved" in {
      val (pub, sub) =
        TestSource[Int]()
          .via(Flow[Int].scanAsync(0)((acc, in) => Future.successful(acc + in)))
          .toMat(TestSink())(Keep.both)
          .run()

      pub.sendNext(1)
      sub.request(10)
      sub.expectNext(0)
      sub.expectNext(1)
      pub.sendComplete()
      sub.expectComplete()
    }

    "fail after zero-element has been consumed" in {
      val (pub, sub) =
        TestSource[Int]()
          .via(Flow[Int].scanAsync(0)((acc, in) => Future.successful(acc + in)))
          .toMat(TestSink())(Keep.both)
          .run()

      sub.request(10)
      sub.expectNext(0)
      pub.sendError(TE("bang"))
      sub.expectError(TE("bang"))
    }

    "work with a single source" in {
      Source.single(1).via(sumScanFlow).runWith(TestSink[Int]()).request(2).expectNext(0, 1).expectComplete()
    }

    "work with a large source" in {
      val elements = 1 to 100000
      val expectedSum = elements.sum
      val result = Source(elements).via(sumScanFlow).runWith(Sink.last).futureValue(timeout(patience.timeout * 2))
      result should ===(expectedSum)
    }

    "work with slow futures" in {
      val delay = 500.milliseconds
      val delayedFutureScanFlow = Flow[Int].scanAsync(0) { (accumulator, next) =>
        pattern.after(delay, system.scheduler)(Future.successful(accumulator + next))
      }
      val elements = 1 :: 1 :: Nil
      Source(elements)
        .via(delayedFutureScanFlow)
        .runWith(TestSink[Int]())
        .request(3)
        .expectNext(100.milliseconds, 0)
        .expectNext(1.second, 1)
        .expectNext(1.second, 2)
        .expectComplete()
    }

    "throw error with a failed source" in {
      val expected = Utils.TE("failed source")
      Source.failed[Int](expected).via(sumScanFlow).runWith(TestSink[Int]()).request(2).expectNextOrError(0, expected)
    }

    "with the restarting decider" should {
      "skip error values with a failed scan" in {
        val elements = 1 :: -1 :: 1 :: Nil
        whenFailedScan(elements, 0, decider = Supervision.restartingDecider).expectNext(1, 1).expectComplete()
      }

      "emit zero with a failed future" in {
        val elements = 1 :: -1 :: 1 :: Nil
        whenFailedFuture(elements, 0, decider = Supervision.restartingDecider).expectNext(1, 1).expectComplete()
      }

      "skip error values and handle stage completion after future get resolved" in {
        val promises = Promise[Int]().success(1) :: Promise[Int]() :: Nil
        val (pub, sub) = whenEventualFuture(promises, 0, decider = Supervision.restartingDecider)
        pub.sendNext(0)
        sub.expectNext(0, 1)
        pub.sendNext(1)
        promises(1).complete(Failure(TE("bang")))
        pub.sendComplete()
        sub.expectComplete()
      }

      "skip error values and handle stage completion before future get resolved" in {
        val promises = Promise[Int]().success(1) :: Promise[Int]() :: Nil
        val (pub, sub) = whenEventualFuture(promises, 0, decider = Supervision.restartingDecider)
        pub.sendNext(0)
        sub.expectNext(0, 1)
        pub.sendNext(1)
        pub.sendComplete()
        promises(1).complete(Failure(TE("bang")))
        sub.expectComplete()
      }
    }

    "with the resuming decider" should {
      "skip values with a failed scan" in {
        val elements = 1 :: -1 :: 1 :: Nil
        whenFailedScan(elements, 0, decider = Supervision.resumingDecider).expectNext(1, 2).expectComplete()
      }

      "skip values with a failed future" in {
        val elements = 1 :: -1 :: 1 :: Nil
        whenFailedFuture(elements, 0, decider = Supervision.resumingDecider).expectNext(1, 2).expectComplete()
      }

      "skip error values and handle stage completion after future get resolved" in {
        val promises = Promise[Int]().success(1) :: Promise[Int]() :: Nil
        val (pub, sub) = whenEventualFuture(promises, 0, decider = Supervision.resumingDecider)
        pub.sendNext(0)
        sub.expectNext(0, 1)
        pub.sendNext(1)
        promises(1).complete(Failure(TE("bang")))
        pub.sendComplete()
        sub.expectComplete()
      }

      "skip error values and handle stage completion before future get resolved" in {
        val promises = Promise[Int]().success(1) :: Promise[Int]() :: Nil
        val (pub, sub) = whenEventualFuture(promises, 0, decider = Supervision.resumingDecider)
        pub.sendNext(0)
        sub.expectNext(0, 1)
        pub.sendNext(1)
        pub.sendComplete()
        promises(1).complete(Failure(TE("bang")))
        sub.expectComplete()
      }
    }

    "with the stopping decider" should {
      "throw error with a failed scan function" in {
        val expected = Utils.TE("failed scan function")
        val elements = -1 :: Nil
        whenFailedScan(elements, 0, expected).expectError(expected)
      }

      "throw error with a failed future" in {
        val expected = Utils.TE("failed future generated from scan function")
        val elements = -1 :: Nil
        whenFailedFuture(elements, 0, expected).expectError(expected)
      }

      "throw error with a null element" in {
        val expectedMessage = ReactiveStreamsCompliance.ElementMustNotBeNullMsg
        val elements = "null" :: Nil
        val actual = whenNullElement(elements, "").expectError()
        actual.getClass should ===(classOf[NullPointerException])
        assert(actual.getMessage === expectedMessage)
      }
    }

    def whenFailedScan(
        elements: immutable.Seq[Int],
        zero: Int,
        throwable: Throwable = new Exception("non fatal exception"),
        decider: Supervision.Decider = Supervision.stoppingDecider): Probe[Int] = {
      val failedScanFlow = Flow[Int].scanAsync(zero) { (accumulator: Int, next: Int) =>
        if (next >= 0) Future(accumulator + next)
        else throw throwable
      }
      Source(elements)
        .via(failedScanFlow)
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .runWith(TestSink[Int]())
        .request(elements.size + 1)
        .expectNext(zero)
    }

    def whenEventualFuture(
        promises: immutable.Seq[Promise[Int]],
        zero: Int,
        decider: Supervision.Decider): (TestPublisher.Probe[Int], TestSubscriber.Probe[Int]) = {
      require(promises.nonEmpty, "must be at least one promise")
      val promiseScanFlow = Flow[Int].scanAsync(zero) { (_: Int, next: Int) =>
        promises(next).future
      }

      val (pub, sub) = TestSource[Int]()
        .via(promiseScanFlow)
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .toMat(TestSink())(Keep.both)
        .run()

      sub.request(promises.size + 1)

      (pub, sub)
    }

    def whenFailedFuture(
        elements: immutable.Seq[Int],
        zero: Int,
        throwable: Throwable = new Exception("non fatal exception"),
        decider: Supervision.Decider = Supervision.stoppingDecider): Probe[Int] = {
      val failedFutureScanFlow = Flow[Int].scanAsync(zero) { (accumulator: Int, next: Int) =>
        if (next >= 0) Future(accumulator + next)
        else Future.failed(throwable)
      }
      Source(elements)
        .via(failedFutureScanFlow)
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .runWith(TestSink[Int]())
        .request(elements.size + 1)
        .expectNext(zero)
    }

    def whenNullElement(
        elements: immutable.Seq[String],
        zero: String,
        decider: Supervision.Decider = Supervision.stoppingDecider): Probe[String] = {
      val nullFutureScanFlow: Flow[String, String, _] = Flow[String].scanAsync(zero) { (_: String, next: String) =>
        if (next != "null") Future(next)
        else Future(null)
      }
      Source(elements)
        .via(nullFutureScanFlow)
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .runWith(TestSink[String]())
        .request(elements.size + 1)
        .expectNext(zero)
    }

  }

}
