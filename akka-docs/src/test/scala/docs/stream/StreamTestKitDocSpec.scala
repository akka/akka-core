/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import scala.concurrent.duration._
import scala.concurrent._

import akka.Done
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl._
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.pattern

class StreamTestKitDocSpec extends AkkaSpec {

  "strict collection" in {
    //#strict-collection
    val sinkUnderTest =
      Flow[Int].map(_ * 2).toMat(Sink.fold(0)(_ + _))(Keep.right)

    val future = Source(1 to 4).runWith(sinkUnderTest)
    val result = Await.result(future, 3.seconds)
    assert(result == 20)
    //#strict-collection
  }

  "grouped part of infinite stream" in {
    //#grouped-infinite
    val sourceUnderTest = Source.repeat(1).map(_ * 2)

    val future = sourceUnderTest.take(10).runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)
    assert(result == Seq.fill(10)(2))
    //#grouped-infinite
  }

  "folded stream" in {
    //#folded-stream
    val flowUnderTest = Flow[Int].takeWhile(_ < 5)

    val future = Source(1 to 10).via(flowUnderTest).runWith(Sink.fold(Seq.empty[Int])(_ :+ _))
    val result = Await.result(future, 3.seconds)
    assert(result == (1 to 4))
    //#folded-stream
  }

  "pipe to test probe" in {
    //#pipeto-testprobe
    import system.dispatcher
    import akka.pattern.pipe

    val sourceUnderTest = Source(1 to 4).grouped(2)

    val probe = TestProbe()
    sourceUnderTest.runWith(Sink.seq).pipeTo(probe.ref)
    probe.expectMsg(3.seconds, Seq(Seq(1, 2), Seq(3, 4)))
    //#pipeto-testprobe
  }

  "sink actor ref" in {
    //#sink-actorref
    case object Tick
    val sourceUnderTest = Source.tick(0.seconds, 200.millis, Tick)

    val probe = TestProbe()
    val cancellable = sourceUnderTest
      .to(Sink.actorRef(probe.ref, onCompleteMessage = "completed", onFailureMessage = _ => "failed"))
      .run()

    probe.expectMsg(1.second, Tick)
    probe.expectNoMessage(100.millis)
    probe.expectMsg(3.seconds, Tick)
    cancellable.cancel()
    probe.expectMsg(3.seconds, "completed")
    //#sink-actorref
  }

  "source actor ref" in {
    //#source-actorref
    val sinkUnderTest = Flow[Int].map(_.toString).toMat(Sink.fold("")(_ + _))(Keep.right)

    val (ref, future) = Source
      .actorRef(
        completionMatcher = {
          case Done =>
            CompletionStrategy.draining
        },
        // Never fail the stream because of a message:
        failureMatcher = PartialFunction.empty,
        bufferSize = 8,
        overflowStrategy = OverflowStrategy.fail)
      .toMat(sinkUnderTest)(Keep.both)
      .run()

    ref ! 1
    ref ! 2
    ref ! 3
    ref ! Done

    val result = Await.result(future, 3.seconds)
    assert(result == "123")
    //#source-actorref
  }

  "test sink probe" in {
    //#test-sink-probe
    val sourceUnderTest = Source(1 to 4).filter(_ % 2 == 0).map(_ * 2)

    sourceUnderTest.runWith(TestSink[Int]()).request(2).expectNext(4, 8).expectComplete()
    //#test-sink-probe
  }

  "test source probe" in {
    //#test-source-probe
    val sinkUnderTest = Sink.cancelled

    TestSource[Int]().toMat(sinkUnderTest)(Keep.left).run().expectCancellation()
    //#test-source-probe
  }

  "injecting failure" in {
    //#injecting-failure
    val sinkUnderTest = Sink.head[Int]

    val (probe, future) = TestSource[Int]().toMat(sinkUnderTest)(Keep.both).run()
    probe.sendError(new Exception("boom"))

    assert(future.failed.futureValue.getMessage == "boom")
    //#injecting-failure
  }

  "test source and a sink" in {
    import system.dispatcher
    //#test-source-and-sink
    val flowUnderTest = Flow[Int].mapAsyncUnordered(2) { sleep =>
      pattern.after(10.millis * sleep, using = system.scheduler)(Future.successful(sleep))
    }

    val (pub, sub) = TestSource[Int]().via(flowUnderTest).toMat(TestSink[Int]())(Keep.both).run()

    sub.request(n = 3)
    pub.sendNext(3)
    pub.sendNext(2)
    pub.sendNext(1)
    sub.expectNextUnordered(1, 2, 3)

    pub.sendError(new Exception("Power surge in the linear subroutine C-47!"))
    val ex = sub.expectError()
    assert(ex.getMessage.contains("C-47"))
    //#test-source-and-sink
  }

}
