/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import org.reactivestreams.Publisher

import akka.stream.testkit.{ BaseTwoStreamsSetup, TestSubscriber }
import akka.stream.testkit.scaladsl.StreamTestKit._

class FlowZipAllSpec extends BaseTwoStreamsSetup {
  override type Outputs = (Int, Int)

  override def setup(p1: Publisher[Int], p2: Publisher[Int]) = {
    val subscriber = TestSubscriber.probe[Outputs]()
    Source.fromPublisher(p1).zip(Source.fromPublisher(p2)).runWith(Sink.fromSubscriber(subscriber))
    subscriber
  }

  "A zipAll for Flow" must {
    "work for shorter left side" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[(Int, String)]()
      Source(1 to 4)
        .zipAll(Source(List("A", "B", "C", "D", "E", "F")), -1, "MISSING")
        .runWith(Sink.fromSubscriber(probe))
      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext((1, "A"))
      probe.expectNext((2, "B"))

      subscription.request(1)
      probe.expectNext((3, "C"))
      subscription.request(1)
      probe.expectNext((4, "D"))

      subscription.request(2)
      probe.expectNext((-1, "E"))
      probe.expectNext((-1, "F"))

      subscription.request(1)
      probe.expectComplete()
    }

    "work for shorter right side" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[(Int, String)]()
      Source(1 to 8)
        .zipAll(Source(List("A", "B", "C", "D", "E", "F")), -1, "MISSING")
        .runWith(Sink.fromSubscriber(probe))
      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext((1, "A"))
      probe.expectNext((2, "B"))

      subscription.request(1)
      probe.expectNext((3, "C"))
      subscription.request(1)
      probe.expectNext((4, "D"))

      subscription.request(2)
      probe.expectNext((5, "E"))
      probe.expectNext((6, "F"))

      subscription.request(2)
      probe.expectNext((7, "MISSING"))
      probe.expectNext((8, "MISSING"))

      subscription.request(1)
      probe.expectComplete()
    }

    "work for equal lengths" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[(Int, String)]()
      Source(1 to 6)
        .zipAll(Source(List("A", "B", "C", "D", "E", "F")), -1, "MISSING")
        .runWith(Sink.fromSubscriber(probe))
      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext((1, "A"))
      probe.expectNext((2, "B"))

      subscription.request(1)
      probe.expectNext((3, "C"))
      subscription.request(1)
      probe.expectNext((4, "D"))

      subscription.request(2)
      probe.expectNext((5, "E"))
      probe.expectNext((6, "F"))

      subscription.request(1)
      probe.expectComplete()
    }
  }

}
