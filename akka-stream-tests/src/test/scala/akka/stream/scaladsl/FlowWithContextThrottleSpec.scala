/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.ThrottleMode.Shaping
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink

class FlowWithContextThrottleSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  private def toMessage(i: Int) = Message(s"data-$i", i.toLong)

  private def genMessage(length: Int, i: Int) = Message("a" * length, i.toLong)

  "throttle() on FlowWithContextOps" must {
    "on FlowWithContext" must {
      "work for the happy case" in {
        val throttle = FlowWithContext[Message, Long].throttle(19, 1000.millis, -1, Shaping)
        val input = (1 to 5).map(toMessage)
        val expected = input.map(message => (message, message.offset))

        Source(input)
          .asSourceWithContext(m => m.offset)
          .via(throttle)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNextN(expected)
          .expectComplete()
      }

      "accept very high rates" in {
        val throttle = FlowWithContext[Message, Long].throttle(1, 1.nanos, 0, Shaping)
        val input = (1 to 5).map(toMessage)
        val expected = input.map(message => (message, message.offset))

        Source(input)
          .asSourceWithContext(m => m.offset)
          .via(throttle)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNextN(expected)
          .expectComplete()
      }

      "accept very low rates" in {
        val throttle = FlowWithContext[Message, Long].throttle(1, 100.days, 1, Shaping)
        val input = (1 to 5).map(toMessage)
        val expected = (input.head, input.head.offset)

        Source(input)
          .asSourceWithContext(m => m.offset)
          .via(throttle)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNext(expected)
          .expectNoMessage(100.millis)
          .cancel() // We won't wait 100 days, sorry
      }

      "emit single element per tick" in {
        val upstream = TestPublisher.probe[Message]()
        val downstream = TestSubscriber.probe[(Message, Long)]()
        val throttle = FlowWithContext[Message, Long].throttle(1, 300.millis, 0, Shaping)

        Source
          .fromPublisher(upstream)
          .asSourceWithContext(m => m.offset)
          .via(throttle)
          .asSource
          .runWith(Sink.fromSubscriber(downstream))

        downstream.request(20)
        upstream.sendNext(Message("a", 1L))
        downstream.expectNoMessage(150.millis)
        downstream.expectNext((Message("a", 1L), 1L))

        upstream.sendNext(Message("b", 2L))
        downstream.expectNoMessage(150.millis)
        downstream.expectNext((Message("b", 2L), 2L))

        upstream.sendComplete()
        downstream.expectComplete()
      }

      "emit elements according to cost" in {
        val list = (1 to 4).map(i => genMessage(i * 2, i))
        val throttle = FlowWithContext[Message, Long].throttle(2, 200.millis, 0, _.data.length, Shaping)

        Source(list)
          .asSourceWithContext(m => m.offset)
          .via(throttle)
          .asSource
          .map(_._1)
          .runWith(TestSink[Message]())
          .request(4)
          .expectNext(list(0))
          .expectNoMessage(300.millis)
          .expectNext(list(1))
          .expectNoMessage(500.millis)
          .expectNext(list(2))
          .expectNoMessage(700.millis)
          .expectNext(list(3))
          .expectComplete()
      }
    }

    "on SourceWithContext" must {
      "work for the happy case" in {
        val input = (1 to 5).map(toMessage)
        val expected = input.map(message => (message, message.offset))

        Source(input)
          .asSourceWithContext(m => m.offset)
          .throttle(19, 1000.millis, -1, Shaping)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNextN(expected)
          .expectComplete()
      }

      "accept very high rates" in {
        val input = (1 to 5).map(toMessage)
        val expected = input.map(message => (message, message.offset))

        Source(input)
          .asSourceWithContext(m => m.offset)
          .throttle(1, 1.nanos, 0, Shaping)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNextN(expected)
          .expectComplete()
      }

      "accept very low rates" in {
        val input = (1 to 5).map(toMessage)
        val expected = (input.head, input.head.offset)

        Source(input)
          .asSourceWithContext(m => m.offset)
          .throttle(1, 100.days, 1, Shaping)
          .asSource
          .runWith(TestSink[(Message, Long)]())
          .request(5)
          .expectNext(expected)
          .expectNoMessage(100.millis)
          .cancel() // We won't wait 100 days, sorry
      }

      "emit single element per tick" in {
        val upstream = TestPublisher.probe[Message]()
        val downstream = TestSubscriber.probe[(Message, Long)]()

        Source
          .fromPublisher(upstream)
          .asSourceWithContext(m => m.offset)
          .throttle(1, 300.millis, 0, Shaping)
          .asSource
          .runWith(Sink.fromSubscriber(downstream))

        downstream.request(20)
        upstream.sendNext(Message("a", 1L))
        downstream.expectNoMessage(150.millis)
        downstream.expectNext((Message("a", 1L), 1L))

        upstream.sendNext(Message("b", 2L))
        downstream.expectNoMessage(150.millis)
        downstream.expectNext((Message("b", 2L), 2L))

        upstream.sendComplete()
        downstream.expectComplete()
      }

      "emit elements according to cost" in {
        val list = (1 to 4).map(i => genMessage(i * 2, i))

        Source(list)
          .asSourceWithContext(m => m.offset)
          .throttle(2, 200.millis, 0, _.data.length, Shaping)
          .asSource
          .map(_._1)
          .runWith(TestSink[Message]())
          .request(4)
          .expectNext(list(0))
          .expectNoMessage(300.millis)
          .expectNext(list(1))
          .expectNoMessage(500.millis)
          .expectNext(list(2))
          .expectNoMessage(700.millis)
          .expectNext(list(3))
          .expectComplete()
      }
    }
  }

}
