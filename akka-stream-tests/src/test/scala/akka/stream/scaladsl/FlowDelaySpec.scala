/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.Milliseconds
import org.scalatest.time.Span

import akka.Done
import akka.stream._
import akka.stream.Attributes._
import akka.stream.OverflowStrategies.EmitEarly
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestDuration
import akka.testkit.TimingTest

class FlowDelaySpec extends StreamSpec {

  "A Delay" must {

    "deliver elements with some time shift" taggedAs TimingTest in {
      Await.result(Source(1 to 10).delay(1.seconds).grouped(100).runWith(Sink.head), 1200.millis) should ===(1 to 10)
    }

    "add delay to initialDelay if exists upstream" taggedAs TimingTest in {
      Source(1 to 10)
        .initialDelay(1.second)
        .delay(1.second)
        .runWith(TestSink[Int]())
        .request(10)
        .expectNoMessage(1800.millis)
        .expectNext(300.millis, 1)
        .expectNextN(2 to 10)
        .expectComplete()
    }

    "deliver element after time passed from actual receiving element" in {
      Source(1 to 3)
        .delay(300.millis)
        .runWith(TestSink[Int]())
        .request(2)
        .expectNoMessage(200.millis) //delay
        .expectNext(200.millis, 1) //delayed element
        .expectNext(100.millis, 2) //buffered element
        .expectNoMessage(200.millis)
        .request(1)
        .expectNext(3) //buffered element
        .expectComplete()
    }

    "deliver elements with delay for slow stream" in {
      val c = TestSubscriber.manualProbe[Int]()
      val p = TestPublisher.manualProbe[Int]()

      Source.fromPublisher(p).delay(300.millis).to(Sink.fromSubscriber(c)).run()
      val cSub = c.expectSubscription()
      val pSub = p.expectSubscription()
      cSub.request(100)
      pSub.sendNext(1)
      c.expectNoMessage(200.millis)
      c.expectNext(1)
      pSub.sendNext(2)
      c.expectNoMessage(200.millis)
      c.expectNext(2)
      pSub.sendComplete()
      c.expectComplete()
    }

    "deliver delayed elements that arrive within the same timeout as preceding group of elements" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[Int]()
      val p = TestPublisher.manualProbe[Int]()

      Source.fromPublisher(p).delay(300.millis).to(Sink.fromSubscriber(c)).run()
      val cSub = c.expectSubscription()
      val pSub = p.expectSubscription()
      cSub.request(100)
      pSub.sendNext(1)
      pSub.sendNext(2)
      c.expectNoMessage(200.millis)
      pSub.sendNext(3)
      c.expectNext(1)
      c.expectNext(2)
      c.expectNoMessage(150.millis)
      c.expectNext(3)
      pSub.sendComplete()
      c.expectComplete()
    }

    "drop tail for internal buffer if it's full in DropTail mode" in {
      Await.result(
        Source(1 to 20)
          .delay(1.seconds, DelayOverflowStrategy.dropTail)
          .withAttributes(inputBuffer(16, 16))
          .grouped(100)
          .runWith(Sink.head),
        1200.millis) should ===((1 to 15).toList :+ 20)
    }

    "drop head for internal buffer if it's full in DropHead mode" in {
      Await.result(
        Source(1 to 20)
          .delay(1.seconds, DelayOverflowStrategy.dropHead)
          .withAttributes(inputBuffer(16, 16))
          .grouped(100)
          .runWith(Sink.head),
        1200.millis) should ===(5 to 20)
    }

    "clear all for internal buffer if it's full in DropBuffer mode" in {
      Await.result(
        Source(1 to 20)
          .delay(1.seconds, DelayOverflowStrategy.dropBuffer)
          .withAttributes(inputBuffer(16, 16))
          .grouped(100)
          .runWith(Sink.head),
        1200.millis) should ===(17 to 20)
    }

    "pass elements with delay through normally in backpressured mode" in {
      Source(1 to 3)
        .delay(300.millis, DelayOverflowStrategy.backpressure)
        .withAttributes(inputBuffer(1, 1))
        .runWith(TestSink[Int]())
        .request(5)
        .expectNoMessage(200.millis)
        .expectNext(200.millis, 1)
        .expectNoMessage(200.millis)
        .expectNext(200.millis, 2)
        .expectNoMessage(200.millis)
        .expectNext(200.millis, 3)
    }

    "fail on overflow in Fail mode" in {
      Source(1 to 20)
        .delay(300.millis, DelayOverflowStrategy.fail)
        .withAttributes(inputBuffer(16, 16))
        .runWith(TestSink[Int]())
        .request(100)
        .expectError(new BufferOverflowException("Buffer overflow for delay operator (max capacity was: 16)!"))

    }

    "emit early when buffer is full and in EmitEarly mode" in {
      val c = TestSubscriber.manualProbe[Int]()
      val p = TestPublisher.manualProbe[Int]()

      Source
        .fromPublisher(p)
        .delay(10.seconds, DelayOverflowStrategy.emitEarly)
        .withAttributes(inputBuffer(16, 16))
        .to(Sink.fromSubscriber(c))
        .run()
      val cSub = c.expectSubscription()
      val pSub = p.expectSubscription()
      cSub.request(20)

      for (i <- 1 to 16) pSub.sendNext(i)
      c.expectNoMessage(300.millis)
      pSub.sendNext(17)
      c.expectNext(100.millis, 1)
      //fail will terminate despite of non empty internal buffer
      pSub.sendError(new RuntimeException() with NoStackTrace)
    }

    "properly delay according to buffer size" taggedAs TimingTest in {
      import system.dispatcher

      import akka.pattern.pipe

      // With a buffer size of 1, delays add up
      Source(1 to 5)
        .delay(500.millis, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        .runWith(Sink.ignore)
        .pipeTo(testActor)

      expectNoMessage(2.seconds)
      expectMsg(Done)

      // With a buffer large enough to hold all arriving elements, delays don't add up
      Source(1 to 100)
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(initial = 100, max = 100))
        .runWith(Sink.ignore)
        .pipeTo(testActor)

      expectMsg(Done)

      // Delays that are already present are preserved when buffer is large enough
      Source
        .tick(100.millis, 100.millis, ())
        .take(10)
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(initial = 10, max = 10))
        .runWith(Sink.ignore)
        .pipeTo(testActor)

      expectNoMessage(900.millis)
      expectMsg(Done)
    }

    "not overflow buffer when DelayOverflowStrategy.backpressure" in {
      val probe = Source(1 to 6)
        .delay(100.millis, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(2, 2))
        .throttle(1, 200.millis, 1, ThrottleMode.Shaping)
        .runWith(TestSink())

      probe.request(10).expectNextN(1 to 6).expectComplete()
    }

    "not drop messages on overflow when EmitEarly" in {
      val probe =
        Source(1 to 2).delay(1.second, EmitEarly).withAttributes(Attributes.inputBuffer(1, 1)).runWith(TestSink())

      probe.request(10).expectNextN(1 to 2).expectComplete()
    }

    "not block overdue elements from being pushed to downstream stages" in {
      val N = 128
      val batchSize = 16
      val delayMillis = 500

      val elements = (1 to N).iterator

      val future = Source
        .tick(0.millis, 10.millis, 1)
        .mapConcat(_ => (1 to batchSize).map(_ => elements.next()))
        .take(N)
        .map { elem =>
          System.nanoTime() -> elem
        }
        .delay(delayMillis.millis, DelayOverflowStrategy.backpressure)
        .withAttributes(Attributes.inputBuffer(4, 4))
        .map {
          case (startTimestamp, elem) =>
            (System.nanoTime() - startTimestamp) / 1e6 -> elem
        }
        .runWith(Sink.seq)

      val results = future.futureValue(PatienceConfiguration.Timeout(Span(60000, Milliseconds)))
      results.length shouldBe N

      // check if every elements are delayed by roughly the same amount of time
      val delayHistogram = results.map(x => Math.floor(x._1 / delayMillis) * delayMillis).groupBy(identity).map {
        case (bucket, delays) => (bucket, delays.length)
      }

      delayHistogram shouldEqual Map(delayMillis.toDouble -> N)
    }

    // repeater for #27095
    "not throw NPE when using EmitEarly and buffer is full" taggedAs TimingTest in {
      val result =
        Source(1 to 9)
          .delay(1.second, DelayOverflowStrategy.emitEarly)
          .addAttributes(Attributes.inputBuffer(5, 5))
          .runWith(Sink.seq)
          .futureValue

      result should ===((1 to 9).toSeq)
    }

    "work with empty source" in {
      Source.empty[Int].delay(Duration.Zero).runWith(TestSink()).request(1).expectComplete()
    }

    "work with fixed delay" in {

      val fixedDelay = 1.second

      val elems = 1 to 10

      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .delay(fixedDelay)
        .map(start => System.nanoTime() - start)
        .runWith(TestSink())

      elems.foreach(_ => {
        val next = probe.request(1).expectNext(fixedDelay + fixedDelay.dilated)
        next should be >= fixedDelay.toNanos
      })

      probe.expectComplete()

    }

    "work without delay" in {

      val elems = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

      Source(elems).delay(Duration.Zero).runWith(TestSink()).request(elems.size).expectNextN(elems).expectComplete()
    }

    "work with linear increasing delay" taggedAs TimingTest in {

      val elems = 1 to 10
      val step = 1.second
      val initial = 1.second
      val max = 5.seconds

      def incWhile(i: (Int, Long)): Boolean = i._1 < 7

      val probe = Source(elems)
        .map(e => (e, System.nanoTime()))
        .delayWith(
          () => DelayStrategy.linearIncreasingDelay(step, incWhile, initial, max),
          OverflowStrategy.backpressure)
        .map(start => System.nanoTime() - start._2)
        .runWith(TestSink())

      elems.foreach(e =>
        if (incWhile((e, 1L))) {
          val afterIncrease = initial + e * step
          val delay = if (afterIncrease < max) {
            afterIncrease
          } else {
            max
          }
          val next = probe.request(1).expectNext(delay + delay.dilated)
          next should be >= delay.toNanos
        } else {
          val next = probe.request(1).expectNext(initial + initial.dilated)
          next should be >= initial.toNanos
        })

      probe.expectComplete()

    }
  }
}
