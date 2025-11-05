/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.util.control.NoStackTrace

import akka.NotUsed
import akka.stream.testkit._
import akka.util.ConstantFun

class FlowConcatAllSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "ConcatAll" must {

    val testException = new Exception("test") with NoStackTrace

    "work in the happy case" in {
      val s1 = Source(1 to 2)
      val s2 = Source(List.empty[Int])
      val s3 = Source(List(3))
      val s4 = Source(4 to 6)
      val s5 = Source(7 to 10)

      val main = Source(List(s1, s2, s3, s4, s5))

      val subscriber = TestSubscriber.manualProbe[Int]()
      main.flatMapConcat(ConstantFun.scalaIdentityFunction).to(Sink.fromSubscriber(subscriber)).run()
      val subscription = subscriber.expectSubscription()
      subscription.request(10)
      for (i <- 1 to 10)
        subscriber.expectNext() shouldBe i
      subscription.request(1)
      subscriber.expectComplete()
    }

    "work together with SplitWhen" in {
      val subscriber = TestSubscriber.probe[Int]()
      Source(1 to 10)
        .splitWhen(_ % 2 == 0)
        .prefixAndTail(0)
        .map(_._2)
        .concatSubstreams
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .runWith(Sink.fromSubscriber(subscriber))

      for (i <- 1 to 10)
        subscriber.requestNext() shouldBe i

      subscriber.request(1)
      subscriber.expectComplete()
    }

    "on onError on master stream cancel the current open substream and signal error" in {
      val publisher = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source
        .fromPublisher(publisher)
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(subscriber))
        .run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int]()
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      val subUpstream = substreamPublisher.expectSubscription()

      upstream.sendError(testException)
      subscriber.expectError(testException)
      subUpstream.expectCancellation()
    }

    "on onError on master stream cancel the currently opening substream and signal error" in {
      val publisher = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source
        .fromPublisher(publisher)
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(subscriber))
        .run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int](autoOnSubscribe = false)
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      val subUpstream = substreamPublisher.expectSubscription()

      upstream.sendError(testException)

      subUpstream.sendOnSubscribe()

      subscriber.expectError(testException)
      subUpstream.expectCancellation()
    }

    "on onError on opening substream, cancel the master stream and signal error " in {
      val publisher = TestPublisher.manualProbe[Source[Int, _]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source.fromPublisher(publisher).flatMapConcat(_ => throw testException).to(Sink.fromSubscriber(subscriber)).run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int]()
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      subscriber.expectError(testException)
      upstream.expectCancellation()
    }

    "on onError on open substream, cancel the master stream and signal error " in {
      val publisher = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source
        .fromPublisher(publisher)
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(subscriber))
        .run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int]()
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      val subUpstream = substreamPublisher.expectSubscription()

      subUpstream.sendError(testException)
      subscriber.expectError(testException)
      upstream.expectCancellation()
    }

    "on cancellation cancel the current open substream and the master stream" in {
      val publisher = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source
        .fromPublisher(publisher)
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(subscriber))
        .run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int]()
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      val subUpstream = substreamPublisher.expectSubscription()

      downstream.cancel()

      subUpstream.expectCancellation()
      upstream.expectCancellation()
    }

    "on cancellation cancel the currently opening substream and the master stream" in {
      val publisher = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val subscriber = TestSubscriber.manualProbe[Int]()
      Source
        .fromPublisher(publisher)
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(subscriber))
        .run()

      val upstream = publisher.expectSubscription()
      val downstream = subscriber.expectSubscription()
      downstream.request(1000)

      val substreamPublisher = TestPublisher.manualProbe[Int](autoOnSubscribe = false)
      val substreamFlow = Source.fromPublisher(substreamPublisher)
      upstream.expectRequest()
      upstream.sendNext(substreamFlow)
      val subUpstream = substreamPublisher.expectSubscription()

      downstream.cancel()

      subUpstream.sendOnSubscribe()

      subUpstream.expectCancellation()
      upstream.expectCancellation()
    }

    "pass along early cancellation" in {
      val up = TestPublisher.manualProbe[Source[Int, NotUsed]]()
      val down = TestSubscriber.manualProbe[Int]()

      val flowSubscriber = Source
        .asSubscriber[Source[Int, NotUsed]]
        .flatMapConcat(ConstantFun.scalaIdentityFunction)
        .to(Sink.fromSubscriber(down))
        .run()

      val downstream = down.expectSubscription()
      downstream.cancel()
      up.subscribe(flowSubscriber)
      val upsub = up.expectSubscription()
      upsub.expectCancellation()
    }

  }

}
