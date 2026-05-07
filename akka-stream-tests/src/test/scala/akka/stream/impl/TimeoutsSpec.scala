/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils._

class TimeoutsSpec extends StreamSpec {

  "InitialTimeout" must {

    "pass through elements unmodified" in {
      Await.result(Source(1 to 100).initialTimeout(2.seconds).grouped(200).runWith(Sink.head), 3.seconds) should ===(
        1 to 100)
    }

    "pass through error unmodified" in {
      a[TE] shouldBe thrownBy {
        Await.result(
          Source(1 to 100).concat(Source.failed(TE("test"))).initialTimeout(2.seconds).grouped(200).runWith(Sink.head),
          3.seconds)
      }
    }

    "fail if no initial element passes until timeout" in {
      val downstreamProbe = TestSubscriber.probe[Int]()
      Source.maybe[Int].initialTimeout(1.second).runWith(Sink.fromSubscriber(downstreamProbe))

      downstreamProbe.expectSubscription()
      downstreamProbe.expectNoMessage(500.millis)

      val ex = downstreamProbe.expectError()
      ex.getMessage should ===("The first element has not yet passed through in 1 second.")
    }

  }

  "CompletionTimeout" must {

    "pass through elements unmodified" in {
      Await.result(Source(1 to 100).completionTimeout(2.seconds).grouped(200).runWith(Sink.head), 3.seconds) should ===(
        1 to 100)
    }

    "pass through error unmodified" in {
      a[TE] shouldBe thrownBy {
        Await.result(
          Source(1 to 100)
            .concat(Source.failed(TE("test")))
            .completionTimeout(2.seconds)
            .grouped(200)
            .runWith(Sink.head),
          3.seconds)
      }
    }

    "fail if not completed until timeout" in {
      val upstreamProbe = TestPublisher.probe[Int]()
      val downstreamProbe = TestSubscriber.probe[Int]()
      Source.fromPublisher(upstreamProbe).completionTimeout(2.seconds).runWith(Sink.fromSubscriber(downstreamProbe))

      upstreamProbe.sendNext(1)
      downstreamProbe.requestNext(1)
      downstreamProbe.expectNoMessage(500.millis) // No timeout yet

      upstreamProbe.sendNext(2)
      downstreamProbe.requestNext(2)
      downstreamProbe.expectNoMessage(500.millis) // No timeout yet

      val ex = downstreamProbe.expectError()
      ex.getMessage should ===("The stream has not been completed in 2 seconds.")
    }

  }

  "IdleTimeout" must {

    "pass through elements unmodified" in {
      Await.result(Source(1 to 100).idleTimeout(2.seconds).grouped(200).runWith(Sink.head), 3.seconds) should ===(
        1 to 100)
    }

    "pass through error unmodified" in {
      a[TE] shouldBe thrownBy {
        Await.result(
          Source(1 to 100).concat(Source.failed(TE("test"))).idleTimeout(2.seconds).grouped(200).runWith(Sink.head),
          3.seconds)
      }
    }

    "fail if time between elements is too large" in {
      val upstreamProbe = TestPublisher.probe[Int]()
      val downstreamProbe = TestSubscriber.probe[Int]()
      Source.fromPublisher(upstreamProbe).idleTimeout(1.seconds).runWith(Sink.fromSubscriber(downstreamProbe))

      // Two seconds in overall, but won't timeout until time between elements is large enough
      // (i.e. this works differently from completionTimeout)
      for (_ <- 1 to 4) {
        upstreamProbe.sendNext(1)
        downstreamProbe.requestNext(1)
        downstreamProbe.expectNoMessage(500.millis) // No timeout yet
      }

      val ex = downstreamProbe.expectError()
      ex.getMessage should ===("No elements passed in the last 1 second.")
    }

  }

  "BackpressureTimeout" must {

    "pass through elements unmodified" in {
      Await.result(Source(1 to 100).backpressureTimeout(1.second).grouped(200).runWith(Sink.head), 3.seconds) should ===(
        1 to 100)
    }

    "succeed if subscriber demand arrives" in {
      val subscriber = TestSubscriber.probe[Int]()

      Source(1 to 4).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      for (i <- 1 to 3) {
        subscriber.requestNext(i)
        subscriber.expectNoMessage(250.millis)
      }

      subscriber.requestNext(4)
      subscriber.expectComplete()
    }

    "not throw if publisher is less frequent than timeout" in {
      val publisher = TestPublisher.probe[String]()
      val subscriber = TestSubscriber.probe[String]()

      Source.fromPublisher(publisher).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      subscriber.request(2)

      subscriber.expectNoMessage(1.second)
      publisher.sendNext("Quick Msg")
      subscriber.expectNext("Quick Msg")

      subscriber.expectNoMessage(3.seconds)
      publisher.sendNext("Slow Msg")
      subscriber.expectNext("Slow Msg")

      publisher.sendComplete()
      subscriber.expectComplete()
    }

    "not throw if publisher won't perform emission ever" in {
      val publisher = TestPublisher.probe[String]()
      val subscriber = TestSubscriber.probe[String]()

      Source.fromPublisher(publisher).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      subscriber.request(16)
      subscriber.expectNoMessage(2.second)

      publisher.sendComplete()
      subscriber.expectComplete()
    }

    "throw if subscriber won't generate demand on time" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      Source.fromPublisher(publisher).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      subscriber.request(1)
      publisher.sendNext(1)
      subscriber.expectNext(1)

      Thread.sleep(3000)

      subscriber.expectError().getMessage should ===("No demand signalled in the last 1 second.")
    }

    "throw if subscriber never generate demand" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      Source.fromPublisher(publisher).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      subscriber.expectSubscription()

      Thread.sleep(3000)

      subscriber.expectError().getMessage should ===("No demand signalled in the last 1 second.")
    }

    "not throw if publisher completes without fulfilling subscriber's demand" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      Source.fromPublisher(publisher).backpressureTimeout(1.second).runWith(Sink.fromSubscriber(subscriber))

      subscriber.request(2)
      publisher.sendNext(1)
      subscriber.expectNext(1)

      subscriber.expectNoMessage(2.second)

      publisher.sendComplete()
      subscriber.expectComplete()
    }

  }

  "IdleTimeoutBidi" must {

    "not signal error in simple loopback case and pass through elements unmodified" in {
      val timeoutIdentity = BidiFlow.bidirectionalIdleTimeout[Int, Int](2.seconds).join(Flow[Int])

      Await.result(Source(1 to 100).via(timeoutIdentity).grouped(200).runWith(Sink.head), 3.seconds) should ===(
        1 to 100)
    }

    "not signal error if traffic is one-way" in {
      val upstreamWriter = TestPublisher.probe[Int]()
      val downstreamWriter = TestPublisher.probe[String]()

      val upstream = Flow.fromSinkAndSourceMat(Sink.ignore, Source.fromPublisher(upstreamWriter))(Keep.left)
      val downstream = Flow.fromSinkAndSourceMat(Sink.ignore, Source.fromPublisher(downstreamWriter))(Keep.left)

      val assembly: RunnableGraph[(Future[Done], Future[Done])] = upstream
        .joinMat(BidiFlow.bidirectionalIdleTimeout[Int, String](2.seconds))(Keep.left)
        .joinMat(downstream)(Keep.both)

      val (upFinished, downFinished) = assembly.run()

      upstreamWriter.sendNext(1)
      Thread.sleep(1000)
      upstreamWriter.sendNext(1)
      Thread.sleep(1000)
      upstreamWriter.sendNext(1)
      Thread.sleep(1000)

      upstreamWriter.sendComplete()
      downstreamWriter.sendComplete()

      Await.ready(upFinished, 3.seconds)
      Await.ready(downFinished, 3.seconds)
    }

    "be able to signal timeout once no traffic on either sides" in {
      val upWrite = TestPublisher.probe[String]()
      val upRead = TestSubscriber.probe[Int]()

      val downWrite = TestPublisher.probe[Int]()
      val downRead = TestSubscriber.probe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val timeoutStage = b.add(BidiFlow.bidirectionalIdleTimeout[String, Int](2.seconds))
          Source.fromPublisher(upWrite) ~> timeoutStage.in1
          timeoutStage.out1 ~> Sink.fromSubscriber(downRead)
          Sink.fromSubscriber(upRead) <~ timeoutStage.out2
          timeoutStage.in2 <~ Source.fromPublisher(downWrite)
          ClosedShape
        })
        .run()

      // Request enough for the whole test
      upRead.request(100)
      downRead.request(100)

      upWrite.sendNext("DATA1")
      downRead.expectNext("DATA1")
      Thread.sleep(1500)

      downWrite.sendNext(1)
      upRead.expectNext(1)
      Thread.sleep(1500)

      upWrite.sendNext("DATA2")
      downRead.expectNext("DATA2")
      Thread.sleep(1000)

      downWrite.sendNext(2)
      upRead.expectNext(2)

      upRead.expectNoMessage(500.millis)
      val error1 = upRead.expectError()
      val error2 = downRead.expectError()

      error1.isInstanceOf[TimeoutException] should be(true)
      error1.getMessage should ===("No elements passed in the last 2 seconds.")
      error2 should ===(error1)

      upWrite.expectCancellation()
      downWrite.expectCancellation()
    }

    "signal error to all outputs" in {
      val upWrite = TestPublisher.probe[String]()
      val upRead = TestSubscriber.probe[Int]()

      val downWrite = TestPublisher.probe[Int]()
      val downRead = TestSubscriber.probe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val timeoutStage = b.add(BidiFlow.bidirectionalIdleTimeout[String, Int](2.seconds))
          Source.fromPublisher(upWrite) ~> timeoutStage.in1
          timeoutStage.out1 ~> Sink.fromSubscriber(downRead)
          Sink.fromSubscriber(upRead) <~ timeoutStage.out2
          timeoutStage.in2 <~ Source.fromPublisher(downWrite)
          ClosedShape
        })
        .run()

      val te = TE("test")

      upWrite.sendError(te)

      upRead.expectSubscriptionAndError(te)
      downRead.expectSubscriptionAndError(te)
      downWrite.expectCancellation()
    }

  }

  "Subscription timeouts" must {

    val subscriptionTimeout =
      ActorAttributes.streamSubscriptionTimeout(100.millis, StreamSubscriptionTimeoutTerminationMode.cancel)

    "be effective for dangling downstream (no fanout)" in {
      val upstream = TestPublisher.probe()
      val (sub, _) =
        Source.asSubscriber
          .viaMat(Flow[Int].map(_.toString))(Keep.left)
          .toMat(Sink.asPublisher(fanout = false))(Keep.both)
          .withAttributes(subscriptionTimeout)
          .run()
      upstream.subscribe(sub)
      upstream.expectCancellation()
    }

    "be effective for dangling downstream (with fanout)" in {
      val upstream = TestPublisher.probe()
      val (sub, _) =
        Source.asSubscriber
          .viaMat(Flow[Int].map(_.toString))(Keep.left)
          .toMat(Sink.asPublisher(fanout = true))(Keep.both)
          .withAttributes(subscriptionTimeout)
          .run()
      upstream.subscribe(sub)
      upstream.expectCancellation()
    }

    // this one seems close to impossible to actually implement
    "be effective for dangling upstream" in pendingUntilFixed {
      val downstream = TestSubscriber.probe[String]()
      val (_, pub) =
        Source.asSubscriber
          .viaMat(Flow[Int].map(_.toString))(Keep.left)
          .toMat(Sink.asPublisher(fanout = false))(Keep.both)
          .withAttributes(subscriptionTimeout)
          .run()
      pub.subscribe(downstream)
      downstream.ensureSubscription()
      downstream.expectError() shouldBe a[SubscriptionTimeoutException]
    }

  }

}

class TimeoutChecksSpec extends AnyWordSpecLike with Matchers {

  "Timeout check interval" must {

    "run twice for timeouts under 800ms" in {
      Timers.timeoutCheckInterval(800.millis) should ===(100.millis)
    }

    "run every 8th of the value for timeouts for timeouts under 1s" in {
      Timers.timeoutCheckInterval(999.millis).toNanos should ===(999.millis.toNanos / 8)
    }

    "run every 1s for timeouts over 1s" in {
      Timers.timeoutCheckInterval(1001.millis) should ===(1.second)
    }

  }

}
