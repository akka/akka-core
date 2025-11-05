/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.ActorAttributes
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class FlowExpandSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "Expand" must {

    "pass-through elements unchanged when there is no rate difference" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      // Simply repeat the last element as an extrapolation step
      Source
        .fromPublisher(publisher)
        .expand(Iterator.single)
        .to(Sink.fromSubscriber(subscriber))
        // Shadow the fuzzed materializer (see the ordering guarantee needed by the for loop below).
        .withAttributes(ActorAttributes.fuzzingMode(false))
        .run()

      for (i <- 1 to 100) {
        // Order is important here: If the request comes first it will be extrapolated!
        publisher.sendNext(i)
        subscriber.requestNext(i)
      }

      subscriber.cancel()
    }

    "expand elements while upstream is silent" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      // Simply repeat the last element as an extrapolation step
      Source.fromPublisher(publisher).expand(Iterator.continually(_)).to(Sink.fromSubscriber(subscriber)).run()

      publisher.sendNext(42)

      for (_ <- 1 to 100) {
        subscriber.requestNext(42)
      }

      publisher.sendNext(-42)

      // The request below is otherwise in race with the above sendNext
      subscriber.expectNoMessage(500.millis)
      subscriber.requestNext(-42)

      subscriber.cancel()
    }

    "do not drop last element" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      // Simply repeat the last element as an extrapolation step
      Source.fromPublisher(publisher).expand(Iterator.continually(_)).to(Sink.fromSubscriber(subscriber)).run()

      publisher.sendNext(1)
      subscriber.requestNext(1)

      publisher.sendNext(2)
      publisher.sendComplete()

      // The request below is otherwise in race with the above sendNext(2) (and completion)
      subscriber.expectNoMessage(500.millis)

      subscriber.requestNext(2)
      subscriber.expectComplete()
    }

    "work on a variable rate chain" in {
      val future = Source(1 to 100)
        .map { i =>
          if (ThreadLocalRandom.current().nextBoolean()) Thread.sleep(10); i
        }
        .expand(Iterator.continually(_))
        .runFold(Set.empty[Int])(_ + _)

      Await.result(future, 10.seconds) should contain theSameElementsAs (1 to 100).toSet
    }

    "backpressure publisher when subscriber is slower" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.probe[Int]()

      Source.fromPublisher(publisher).expand(Iterator.continually(_)).to(Sink.fromSubscriber(subscriber)).run()

      publisher.sendNext(1)
      subscriber.requestNext(1)
      subscriber.requestNext(1)

      var pending = publisher.pending
      // Deplete pending requests coming from input buffer
      while (pending > 0) {
        publisher.unsafeSendNext(2)
        pending -= 1
      }

      // The above sends are absorbed in the input buffer, and will result in two one-sized batch requests
      pending += publisher.expectRequest()
      pending += publisher.expectRequest()
      while (pending > 0) {
        publisher.unsafeSendNext(2)
        pending -= 1
      }

      publisher.expectNoMessage(1.second)

      subscriber.request(2)
      subscriber.expectNext(2)
      subscriber.expectNext(2)

      // Now production is resumed
      publisher.expectRequest()

    }

    "work properly with finite extrapolations" in {
      val (source, sink) =
        TestSource[Int]().expand(i => Iterator.from(0).map(i -> _).take(3)).toMat(TestSink())(Keep.both).run()
      source.sendNext(1)
      sink.request(4).expectNext(1 -> 0, 1 -> 1, 1 -> 2).expectNoMessage(100.millis)
      source.sendNext(2).sendComplete()
      sink.expectNext(2 -> 0).expectComplete()
    }
  }

}
