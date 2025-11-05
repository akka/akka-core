/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Attributes.inputBuffer
import akka.stream.Supervision.restartingDecider
import akka.stream.Supervision.resumingDecider
import akka.stream.testkit._
import akka.stream.testkit.Utils.TE
import akka.testkit.TestLatch

class FlowConflateSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "Conflate" must {

    "pass-through elements unchanged when there is no rate difference" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source
        .fromPublisher(publisher)
        .conflateWithSeed(seed = i => i)(aggregate = (sum, i) => sum + i)
        .to(Sink.fromSubscriber(subscriber))
        .run()
      val sub = subscriber.expectSubscription()

      for (i <- 1 to 100) {
        sub.request(1)
        publisher.sendNext(i)
        subscriber.expectNext(i)
      }

      sub.cancel()
    }

    "pass-through elements unchanged when there is no rate difference (simple conflate)" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source.fromPublisher(publisher).conflate(_ + _).to(Sink.fromSubscriber(subscriber)).run()
      val sub = subscriber.expectSubscription()

      for (i <- 1 to 100) {
        sub.request(1)
        publisher.sendNext(i)
        subscriber.expectNext(i)
      }

      sub.cancel()
    }

    "conflate elements while downstream is silent" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source
        .fromPublisher(publisher)
        .conflateWithSeed(seed = i => i)(aggregate = (sum, i) => sum + i)
        .to(Sink.fromSubscriber(subscriber))
        .run()
      val sub = subscriber.expectSubscription()

      for (i <- 1 to 100) {
        publisher.sendNext(i)
      }
      subscriber.expectNoMessage(1.second)
      sub.request(1)
      subscriber.expectNext(5050)
      sub.cancel()
    }

    "conflate elements while downstream is silent (simple conflate)" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source.fromPublisher(publisher).conflate(_ + _).to(Sink.fromSubscriber(subscriber)).run()
      val sub = subscriber.expectSubscription()

      for (i <- 1 to 100) {
        publisher.sendNext(i)
      }
      subscriber.expectNoMessage(1.second)
      sub.request(1)
      subscriber.expectNext(5050)
      sub.cancel()
    }

    "work on a variable rate chain" in {
      val future = Source(1 to 1000)
        .conflateWithSeed(seed = i => i)(aggregate = (sum, i) => sum + i)
        .map { i =>
          if (ThreadLocalRandom.current().nextBoolean()) Thread.sleep(10); i
        }
        .runFold(0)(_ + _)
      Await.result(future, 10.seconds) should be(500500)
    }

    "work on a variable rate chain (simple conflate)" in {
      val future = Source(1 to 1000)
        .conflate(_ + _)
        .map { i =>
          if (ThreadLocalRandom.current().nextBoolean()) Thread.sleep(10); i
        }
        .runFold(0)(_ + _)
      Await.result(future, 10.seconds) should be(500500)
    }

    "backpressure subscriber when upstream is slower" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source
        .fromPublisher(publisher)
        .conflateWithSeed(seed = i => i)(aggregate = (sum, i) => sum + i)
        .to(Sink.fromSubscriber(subscriber))
        .run()
      val sub = subscriber.expectSubscription()

      sub.request(1)
      publisher.sendNext(1)
      subscriber.expectNext(1)

      sub.request(1)
      subscriber.expectNoMessage(500.millis)
      publisher.sendNext(2)
      subscriber.expectNext(2)

      publisher.sendNext(3)
      publisher.sendNext(4)
      // The request can be in race with the above onNext(4) so the result would be either 3 or 7.
      subscriber.expectNoMessage(500.millis)
      sub.request(1)
      subscriber.expectNext(7)

      sub.request(1)
      subscriber.expectNoMessage(500.millis)
      sub.cancel()

    }

    "work with a buffer and fold" in {
      val future = Source(1 to 50)
        .conflateWithSeed(seed = i => i)(aggregate = (sum, i) => sum + i)
        .buffer(50, OverflowStrategy.backpressure)
        .runFold(0)(_ + _)
      Await.result(future, 3.seconds) should be((1 to 50).sum)
    }

    "restart when `seed` throws and a restartingDecider is used" in {
      val sourceProbe = TestPublisher.probe[Int]()
      val sinkProbe = TestSubscriber.probe[Int]()
      val exceptionLatch = TestLatch()

      Source
        .fromPublisher(sourceProbe)
        .conflateWithSeed { i =>
          if (i % 2 == 0) {
            exceptionLatch.open()
            throw TE("I hate even seed numbers")
          } else i
        } { (sum, i) =>
          sum + i
        }
        .withAttributes(supervisionStrategy(restartingDecider))
        .to(Sink.fromSubscriber(sinkProbe))
        .withAttributes(inputBuffer(initial = 1, max = 1))
        .run()

      val sub = sourceProbe.expectSubscription()
      val sinkSub = sinkProbe.expectSubscription()

      // push the first value
      sub.expectRequest(1)
      sub.sendNext(1)

      // and consume it, so that the next element
      // will trigger seed
      sinkSub.request(1)
      sinkProbe.expectNext(1)

      sub.expectRequest(1)
      sub.sendNext(2)

      // make sure the seed exception happened
      // before going any further
      Await.result(exceptionLatch, 3.seconds)

      sub.expectRequest(1)
      sub.sendNext(3)

      // now we should have lost the 2 and the accumulated state
      sinkSub.request(1)
      sinkProbe.expectNext(3)
    }

    "restart when `aggregate` throws and a restartingDecider is used" in {
      val latch = TestLatch()
      val conflate = Flow[String]
        .conflateWithSeed(seed = i => i)((state, elem) =>
          if (elem == "two") {
            latch.open()
            throw TE("two is a three letter word")
          } else state + elem)
        .withAttributes(supervisionStrategy(restartingDecider))

      val sourceProbe = TestPublisher.probe[String]()
      val sinkProbe = TestSubscriber.probe[String]()

      Source
        .fromPublisher(sourceProbe)
        .via(conflate)
        .to(Sink.fromSubscriber(sinkProbe))
        .withAttributes(inputBuffer(initial = 4, max = 4))
        .run()

      val sub = sourceProbe.expectSubscription()

      sub.expectRequest(4)
      sub.sendNext("one")
      sub.sendNext("two")
      sub.sendNext("three")
      sub.sendComplete()

      // "one" should be lost
      Await.ready(latch, 3.seconds)
      sinkProbe.requestNext() should ===("three")

    }

    "resume when `aggregate` throws and a resumingDecider is used" in {

      val sourceProbe = TestPublisher.probe[Int]()
      val sinkProbe = TestSubscriber.probe[Vector[Int]]()
      val saw4Latch = TestLatch()

      Source
        .fromPublisher(sourceProbe)
        .conflateWithSeed(seed = i => Vector(i))((state, elem) =>
          if (elem == 2) {
            throw TE("three is a four letter word")
          } else {
            if (elem == 4) saw4Latch.open()
            state :+ elem
          })
        .withAttributes(supervisionStrategy(resumingDecider))
        .to(Sink.fromSubscriber(sinkProbe))
        .withAttributes(inputBuffer(initial = 1, max = 1))
        .run()

      val sub = sourceProbe.expectSubscription()
      val sinkSub = sinkProbe.expectSubscription()
      // push the first three values, the third will trigger
      // the exception
      sub.expectRequest(1)
      sub.sendNext(1)

      // causing the 1 to get thrown away
      sub.expectRequest(1)
      sub.sendNext(2)

      sub.expectRequest(1)
      sub.sendNext(3)

      sub.expectRequest(1)
      sub.sendNext(4)

      // and consume it, so that the next element
      // will trigger seed
      Await.ready(saw4Latch, 3.seconds)
      sinkSub.request(1)

      sinkProbe.expectNext(Vector(1, 3, 4))
    }

  }

}
