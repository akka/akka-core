/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await

import akka.stream.impl.SubscriptionTimeoutException
import akka.stream.testkit._

class SubstreamSubscriptionTimeoutSpec extends StreamSpec("""
  akka.stream.materializer {
    initial-input-buffer-size = 2
    max-input-buffer-size = 2
    subscription-timeout {
      mode = cancel
      timeout = 300 ms
    }
  }
""") {
  import FlowGroupBySpec._
  import system.dispatcher

  "groupBy and splitwhen" must {

    "timeout and cancel substream publishers when no-one subscribes to them after some time (time them out)" in {
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      val publisherProbe = TestPublisher.probe[Int]()
      Source.fromPublisher(publisherProbe).groupBy(3, _ % 3).lift(_ % 3).runWith(Sink.fromSubscriber(subscriber))

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      publisherProbe.sendNext(1)
      publisherProbe.sendNext(2)
      publisherProbe.sendNext(3)

      val (_, s1) = subscriber.expectNext()
      // should not break normal usage
      val s1SubscriberProbe = TestSubscriber.manualProbe[Int]()
      s1.runWith(Sink.fromSubscriber(s1SubscriberProbe))
      val s1Subscription = s1SubscriberProbe.expectSubscription()
      s1Subscription.request(100)
      s1SubscriberProbe.expectNext(1)

      val (_, s2) = subscriber.expectNext()
      // should not break normal usage
      val s2SubscriberProbe = TestSubscriber.manualProbe[Int]()
      s2.runWith(Sink.fromSubscriber(s2SubscriberProbe))
      val s2Subscription = s2SubscriberProbe.expectSubscription()
      s2Subscription.request(100)
      s2SubscriberProbe.expectNext(2)

      val (_, s3) = subscriber.expectNext()

      // sleep long enough for it to be cleaned up
      Thread.sleep(1500)

      // Must be a Sink.seq, otherwise there is a race due to the concat in the `lift` implementation
      val f = s3.runWith(Sink.seq).recover[AnyRef] { case _: SubscriptionTimeoutException => "expected" }
      Await.result(f, remainingOrDefault) should equal("expected")

      publisherProbe.sendComplete()
    }

    "timeout and stop groupBy parent actor if none of the substreams are actually consumed" in {
      val publisherProbe = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      Source.fromPublisher(publisherProbe).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.fromSubscriber(subscriber))

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      publisherProbe.sendNext(1)
      publisherProbe.sendNext(2)
      publisherProbe.sendNext(3)
      publisherProbe.sendComplete()

      val (_, _) = subscriber.expectNext()
      val (_, _) = subscriber.expectNext()
    }

    "not timeout and cancel substream publishers when they have been subscribed to" in {
      val publisherProbe = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      Source.fromPublisher(publisherProbe).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.fromSubscriber(subscriber))

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      publisherProbe.sendNext(1)
      publisherProbe.sendNext(2)

      val (_, s1) = subscriber.expectNext()
      // should not break normal usage
      val s1SubscriberProbe = TestSubscriber.manualProbe[Int]()
      s1.runWith(Sink.fromSubscriber(s1SubscriberProbe))
      val s1Sub = s1SubscriberProbe.expectSubscription()
      s1Sub.request(1)
      s1SubscriberProbe.expectNext(1)

      val (_, s2) = subscriber.expectNext()
      // should not break normal usage
      val s2SubscriberProbe = TestSubscriber.manualProbe[Int]()
      s2.runWith(Sink.fromSubscriber(s2SubscriberProbe))
      val s2Sub = s2SubscriberProbe.expectSubscription()

      // sleep long enough for timeout to trigger if not canceled
      Thread.sleep(1000)

      s2Sub.request(100)
      s2SubscriberProbe.expectNext(2)
      s1Sub.request(100)
      publisherProbe.sendNext(3)
      publisherProbe.sendNext(4)
      s1SubscriberProbe.expectNext(3)
      s2SubscriberProbe.expectNext(4)
    }
  }

}
