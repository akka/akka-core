/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.FlowMonitorState
import akka.stream.FlowMonitorState._
import akka.stream.Materializer
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class FlowMonitorSpec extends StreamSpec {

  "A FlowMonitor" must {
    "return Finished when stream is completed" in {
      val ((source, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      source.sendComplete()
      awaitAssert(monitor.state == Finished, 3.seconds)
      sink.expectSubscriptionAndComplete()
    }

    "return Finished when stream is cancelled from downstream" in {
      val ((_, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      sink.cancel()
      awaitAssert(monitor.state == Finished, 3.seconds)
    }

    "return Failed when stream fails, and propagate the error" in {
      val ((source, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      val ex = new Exception("Source failed")
      source.sendError(ex)
      awaitAssert(monitor.state == Failed(ex), 3.seconds)
      sink.expectSubscriptionAndError(ex)
    }

    "return Initialized for an empty stream" in {
      val ((source, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      awaitAssert(monitor.state == Initialized, 3.seconds)
      source.expectRequest()
      sink.expectSubscription()
    }

    "return Received after receiving a message" in {
      val ((source, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      val msg = "message"
      source.sendNext(msg)
      sink.requestNext(msg)
      awaitAssert(monitor.state == Received(msg), 3.seconds)
    }

    // Check a stream that processes StreamState messages specifically, to make sure the optimization in FlowMonitorImpl
    // (to avoid allocating an object for each message) doesn't introduce a bug
    "return Received after receiving a StreamState message" in {
      val ((source, monitor), sink) =
        TestSource[Any]().monitorMat(Keep.both).toMat(TestSink[Any]())(Keep.both).run()
      val msg = Received("message")
      source.sendNext(msg)
      sink.requestNext(msg)
      awaitAssert(monitor.state == Received(msg), 3.seconds)
    }

    "return Failed when stream is abruptly terminated" in {
      val mat = Materializer(system)
      val (_, monitor) = // notice that `monitor` is like a Keep.both
        TestSource[Any]().monitor.to(Sink.ignore).run()(mat)
      mat.shutdown()

      awaitAssert(monitor.state shouldBe a[FlowMonitorState.Failed], remainingOrDefault)
    }

  }
}
