/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.ClosedShape
import akka.stream.testkit._
import akka.testkit.TimingTest

class TickSourceSpec extends StreamSpec {

  "A Flow based on tick publisher" must {
    "produce ticks" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[String]()
      Source.tick(1.second, 1.second, "tick").to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext("tick")
      c.expectNoMessage(200.millis)
      c.expectNext("tick")
      sub.cancel()
      c.expectNoMessage(200.millis)
    }

    "drop ticks when not requested" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[String]()
      Source.tick(1.second, 1.second, "tick").to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext("tick")
      c.expectNoMessage(200.millis)
      c.expectNext("tick")
      c.expectNoMessage(1400.millis)
      sub.request(2)
      c.expectNext("tick")
      c.expectNoMessage(200.millis)
      c.expectNext("tick")
      sub.cancel()
      c.expectNoMessage(200.millis)
    }

    "reject multiple subscribers, but keep the first" taggedAs TimingTest in {
      val p = Source.tick(1.second, 1.second, "tick").runWith(Sink.asPublisher(false))
      val c1 = TestSubscriber.manualProbe[String]()
      val c2 = TestSubscriber.manualProbe[String]()
      p.subscribe(c1)
      p.subscribe(c2)
      val sub1 = c1.expectSubscription()
      c2.expectSubscriptionAndError()
      sub1.request(1)
      c1.expectNext("tick")
      c1.expectNoMessage(200.millis)
      sub1.request(2)
      c1.expectNext("tick")
      sub1.cancel()
    }

    "be usable with zip for a simple form of rate limiting" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val zip = b.add(Zip[Int, String]())
          Source(1 to 100) ~> zip.in0
          Source.tick(1.second, 1.second, "tick") ~> zip.in1
          zip.out ~> Flow[(Int, String)].map { case (n, _) => n } ~> Sink.fromSubscriber(c)
          ClosedShape
        })
        .run()

      val sub = c.expectSubscription()
      sub.request(1000)
      c.expectNext(1)
      c.expectNoMessage(200.millis)
      c.expectNext(2)
      c.expectNoMessage(200.millis)
      sub.cancel()
    }

    "be possible to cancel" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[String]()
      val tickSource = Source.tick(1.second, 1.second, "tick")
      val cancellable = tickSource.to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNoMessage(600.millis)
      c.expectNext("tick")
      c.expectNoMessage(200.millis)
      c.expectNext("tick")
      cancellable.cancel()
      awaitCond(cancellable.isCancelled)
      sub.request(3)
      c.expectComplete()
    }

    "acknowledge cancellation only once" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[String]()
      val cancellable = Source.tick(1.second, 500.millis, "tick").to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext("tick")
      cancellable.cancel() should be(true)
      cancellable.cancel() should be(false)
      c.expectComplete()
    }

    "have isCancelled mirror the cancellation state" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[String]()
      val cancellable = Source.tick(1.second, 500.millis, "tick").to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext("tick")
      cancellable.isCancelled should be(false)
      cancellable.cancel() should be(true)
      cancellable.isCancelled should be(true)
      c.expectComplete()
    }

    "support being cancelled immediately after its materialization" in {
      val c = TestSubscriber.manualProbe[String]()
      val cancellable = Source.tick(1.second, 500.millis, "tick").to(Sink.fromSubscriber(c)).run()
      cancellable.cancel() should be(true)
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectComplete()
    }

  }
}
