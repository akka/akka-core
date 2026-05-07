/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.testkit._

class FlowDropWithinSpec extends StreamSpec {

  "A DropWithin" must {

    "deliver elements after the duration, but not before" in {
      val input = Iterator.from(1)
      val p = TestPublisher.manualProbe[Int]()
      val c = TestSubscriber.manualProbe[Int]()
      Source.fromPublisher(p).dropWithin(1.second).to(Sink.fromSubscriber(c)).run()
      val pSub = p.expectSubscription()
      val cSub = c.expectSubscription()
      cSub.request(100)
      val demand1 = pSub.expectRequest()
      (1 to demand1.toInt).foreach { _ =>
        pSub.sendNext(input.next())
      }
      val demand2 = pSub.expectRequest()
      (1 to demand2.toInt).foreach { _ =>
        pSub.sendNext(input.next())
      }
      val demand3 = pSub.expectRequest()
      c.expectNoMessage(1500.millis)
      (1 to demand3.toInt).foreach { _ =>
        pSub.sendNext(input.next())
      }
      ((demand1 + demand2 + 1).toInt to (demand1 + demand2 + demand3).toInt).foreach { n =>
        c.expectNext(n)
      }
      pSub.sendComplete()
      c.expectComplete()
      c.expectNoMessage(200.millis)
    }

    "deliver completion even before the duration" in {
      val upstream = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[Int]()

      Source.fromPublisher(upstream).dropWithin(1.day).runWith(Sink.fromSubscriber(downstream))

      upstream.sendComplete()
      downstream.expectSubscriptionAndComplete()
    }

  }

}
