/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.concurrent.duration._

import org.reactivestreams.Publisher

import akka.Done
import akka.actor.{ ActorRef, Status }
import akka.stream.{ OverflowStrategy, _ }
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl._

@nowarn("msg=deprecated")
class ActorRefSourceSpec extends StreamSpec {

  "A ActorRefSource" must {

    "emit received messages to the stream" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef(PartialFunction.empty, PartialFunction.empty, 10, OverflowStrategy.fail)
        .to(Sink.fromSubscriber(s))
        .run()
      val sub = s.expectSubscription()
      sub.request(2)
      ref ! 1
      s.expectNext(1)
      ref ! 2
      s.expectNext(2)
      ref ! 3
      s.expectNoMessage(500.millis)
    }

    "buffer when needed" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef(PartialFunction.empty, PartialFunction.empty, 100, OverflowStrategy.dropHead)
        .to(Sink.fromSubscriber(s))
        .run()
      val sub = s.expectSubscription()
      for (n <- 1 to 20) ref ! n
      sub.request(10)
      for (n <- 1 to 10) s.expectNext(n)
      sub.request(10)
      for (n <- 11 to 20) s.expectNext(n)

      for (n <- 200 to 399) ref ! n
      sub.request(100)
      for (n <- 300 to 399) s.expectNext(n)
    }

    "drop new when full and with dropNew strategy" in {
      val (ref, sub) = Source
        .actorRef(PartialFunction.empty, PartialFunction.empty, 100, OverflowStrategy.dropNew)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      for (n <- 1 to 20) ref ! n
      sub.request(10)
      for (n <- 1 to 10) sub.expectNext(n)
      sub.request(10)
      for (n <- 11 to 20) sub.expectNext(n)

      for (n <- 200 to 399) ref ! n
      sub.request(100)
      for (n <- 200 to 299) sub.expectNext(n)
    }

    "terminate when the stream is cancelled" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef(PartialFunction.empty, PartialFunction.empty, 0, OverflowStrategy.fail)
        .to(Sink.fromSubscriber(s))
        .run()
      watch(ref)
      val sub = s.expectSubscription()
      sub.cancel()
      expectTerminated(ref)
    }

    "not fail when 0 buffer space and demand is signalled" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef(PartialFunction.empty, PartialFunction.empty, 0, OverflowStrategy.dropHead)
        .to(Sink.fromSubscriber(s))
        .run()
      watch(ref)
      val sub = s.expectSubscription()
      sub.request(100)
      sub.cancel()
      expectTerminated(ref)
    }

    "signal buffered elements and complete the stream after receiving Status.Success" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 3, OverflowStrategy.fail)
        .to(Sink.fromSubscriber(s))
        .run()
      val sub = s.expectSubscription()
      ref ! 1
      ref ! 2
      ref ! 3
      ref ! "ok"
      sub.request(10)
      s.expectNext(1, 2, 3)
      s.expectComplete()
    }

    "signal buffered elements and complete the stream after receiving a Status.Success companion" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 3, OverflowStrategy.fail)
        .to(Sink.fromSubscriber(s))
        .run()
      val sub = s.expectSubscription()
      ref ! 1
      ref ! 2
      ref ! 3
      ref ! "ok"
      sub.request(10)
      s.expectNext(1, 2, 3)
      s.expectComplete()
    }

    "signal buffered elements and complete the stream after receiving a Status.Success with CompletionStrategy.Draining" in {
      val (ref, s) = Source
        .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 100, OverflowStrategy.fail)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      for (n <- 1 to 20) ref ! n
      ref ! "ok"

      s.request(10)
      for (n <- 1 to 10) s.expectNext(n)
      s.expectNoMessage(20.millis)
      s.request(10)
      for (n <- 11 to 20) s.expectNext(n)
      s.expectComplete()
    }

    "not signal buffered elements but complete immediately the stream after receiving a Status.Success with CompletionStrategy.Immediately" in {
      val (ref, s) = Source
        .actorRef({ case "ok" => CompletionStrategy.immediately }, PartialFunction.empty, 100, OverflowStrategy.fail)
        .toMat(TestSink[Int]())(Keep.both)
        .run()

      for (n <- 1 to 20) ref ! n
      ref ! "ok"

      s.request(10)

      def verifyNext(n: Int): Unit = {
        if (n > 10)
          s.expectComplete()
        else
          s.expectNextOrComplete() match {
            case Right(`n`) => verifyNext(n + 1)
            case Right(x)   => fail(s"expected $n, got $x")
            case Left(_)    => // ok, completed
          }
      }
      verifyNext(1)
    }

    "not buffer elements after receiving Status.Success" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 3, OverflowStrategy.dropBuffer)
        .to(Sink.fromSubscriber(s))
        .run()
      val sub = s.expectSubscription()
      ref ! 1
      ref ! 2
      ref ! 3
      ref ! "ok"
      ref ! 100
      ref ! 100
      ref ! 100
      sub.request(10)
      s.expectNext(1, 2, 3)
      s.expectComplete()
    }

    "complete and materialize the stream after receiving completion message" in {
      val (ref, done) = {
        Source
          .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 3, OverflowStrategy.dropBuffer)
          .toMat(Sink.ignore)(Keep.both)
          .run()
      }
      ref ! "ok"
      done.futureValue should be(Done)
    }

    "fail the stream when receiving failure message" in {
      val s = TestSubscriber.manualProbe[Int]()
      val ref = Source
        .actorRef(PartialFunction.empty, { case Status.Failure(e) => e }, 10, OverflowStrategy.fail)
        .to(Sink.fromSubscriber(s))
        .run()
      s.expectSubscription()
      val exc = TE("testfailure")
      ref ! Status.Failure(exc)
      s.expectError(exc)
    }

    "set actor name equal to stage name" in {
      val s = TestSubscriber.manualProbe[Int]()
      val name = "SomeCustomName"
      val ref = Source
        .actorRef({ case "ok" => CompletionStrategy.draining }, PartialFunction.empty, 10, OverflowStrategy.fail)
        .withAttributes(Attributes.name(name))
        .to(Sink.fromSubscriber(s))
        .run()
      ref.path.name.contains(name) should ===(true)
      ref ! "ok"
    }

    "be possible to run immediately, reproducer of #26714" in {
      (1 to 100).foreach { _ =>
        val mat = Materializer(system)
        val source: Source[String, ActorRef] =
          Source.actorRef[String](PartialFunction.empty, PartialFunction.empty, 10000, OverflowStrategy.fail)
        val (_: ActorRef, _: Publisher[String]) =
          source.toMat(Sink.asPublisher(false))(Keep.both).run()(mat)
        mat.shutdown()
      }
    }
  }
}
