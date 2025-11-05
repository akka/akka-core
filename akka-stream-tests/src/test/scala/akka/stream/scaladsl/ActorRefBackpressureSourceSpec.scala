/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.actor.Status
import akka.stream.CompletionStrategy
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe

private object ActorRefBackpressureSourceSpec {
  case object AckMsg
}

class ActorRefBackpressureSourceSpec extends StreamSpec {
  import ActorRefBackpressureSourceSpec._

  "An Source.actorRefWithBackpressure" must {

    "emit received messages to the stream and ack" in {
      val probe = TestProbe()
      val (ref, s) = Source
        .actorRefWithBackpressure[Int](
          AckMsg, { case "ok" => CompletionStrategy.draining }: PartialFunction[Any, CompletionStrategy],
          PartialFunction.empty)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      val sub = s.expectSubscription()
      sub.request(10)
      probe.send(ref, 1)
      s.expectNext(1)
      probe.expectMsg(AckMsg)

      probe.send(ref, 2)
      s.expectNext(2)
      probe.expectMsg(AckMsg)

      s.expectNoMessage(50.millis)

      ref ! "ok"
      s.expectComplete()
    }

    "fail when consumer does not await ack" in {
      val probe = TestProbe()
      val (ref, s) = Source
        .actorRefWithBackpressure[Int](AckMsg, PartialFunction.empty, PartialFunction.empty)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      val sub = s.expectSubscription()
      for (n <- 1 to 20) probe.send(ref, n)
      sub.request(1)

      @scala.annotation.tailrec
      def verifyNext(n: Int): Unit = {
        if (n > 10)
          s.expectComplete()
        else
          s.expectNextOrError() match {
            case Right(`n`) => verifyNext(n + 1)
            case Right(x)   => fail(s"expected $n, got $x")
            case Left(e: IllegalStateException) =>
              e.getMessage shouldBe "Received new element before ack was signaled back"
            case Left(e) =>
              fail(s"Expected IllegalStateException, got ${e.getClass}", e)
          }
      }
      verifyNext(1)
    }

    "complete after receiving Status.Success" in {
      val probe = TestProbe()
      val (ref, s) = Source
        .actorRefWithBackpressure[Int](
          AckMsg, { case "ok" => CompletionStrategy.draining }: PartialFunction[Any, CompletionStrategy],
          PartialFunction.empty)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      val sub = s.expectSubscription()
      sub.request(10)
      probe.send(ref, 1)
      s.expectNext(1)
      probe.expectMsg(AckMsg)

      ref ! "ok"

      s.expectComplete()
    }

    "fail after receiving Status.Failure" in {
      val probe = TestProbe()
      val (ref, s) = Source
        .actorRefWithBackpressure[Int](
          AckMsg,
          PartialFunction.empty, { case Status.Failure(f) => f }: PartialFunction[Any, Throwable])
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      val sub = s.expectSubscription()
      sub.request(10)
      probe.send(ref, 1)
      s.expectNext(1)
      probe.expectMsg(AckMsg)

      ref ! Status.Failure(TE("test"))

      s.expectError(TE("test"))
    }

    "not buffer elements after receiving Status.Success" in {
      val probe = TestProbe()
      val (ref, s) = Source
        .actorRefWithBackpressure[Int](
          AckMsg, { case "ok" => CompletionStrategy.draining }: PartialFunction[Any, CompletionStrategy],
          PartialFunction.empty)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      val sub = s.expectSubscription()
      sub.request(10)
      probe.send(ref, 1)
      s.expectNext(1)
      probe.expectMsg(AckMsg)

      probe.send(ref, 2)
      s.expectNext(2)
      probe.expectMsg(AckMsg)

      ref ! "ok"

      probe.send(ref, 100)
      probe.send(ref, 100)
      probe.send(ref, 100)
      probe.send(ref, 100)
      probe.expectNoMessage(200.millis)

      s.expectComplete()
    }
  }
}
