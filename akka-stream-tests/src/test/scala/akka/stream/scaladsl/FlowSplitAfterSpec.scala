/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import org.reactivestreams.Publisher

import akka.NotUsed
import akka.stream._
import akka.stream.StreamSubscriptionTimeoutTerminationMode
import akka.stream.Supervision.resumingDecider
import akka.stream.impl.SubscriptionTimeoutException
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils._

object FlowSplitAfterSpec {

  implicit class Lift[M](val f: SubFlow[Int, M, Source[Int, M]#Repr, RunnableGraph[M]]) extends AnyVal {
    def lift = f.prefixAndTail(0).map(_._2).concatSubstreams
  }

}

class FlowSplitAfterSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
    akka.stream.materializer.subscription-timeout.timeout = 1s
    akka.stream.materializer.subscription-timeout.mode = cancel
  """) {
  import FlowSplitAfterSpec._

  case class StreamPuppet(p: Publisher[Int]) {
    val probe = TestSubscriber.manualProbe[Int]()
    p.subscribe(probe)
    val subscription = probe.expectSubscription()

    def request(demand: Int): Unit = subscription.request(demand)
    def expectNext(elem: Int): Unit = probe.expectNext(elem)
    def expectNoMessage(max: FiniteDuration): Unit = probe.expectNoMessage(max)
    def expectComplete(): Unit = probe.expectComplete()
    def expectError(e: Throwable) = probe.expectError(e)
    def cancel(): Unit = subscription.cancel()
  }

  class SubstreamsSupport(
      splitAfter: Int = 3,
      elementCount: Int = 6,
      substreamCancelStrategy: SubstreamCancelStrategy = SubstreamCancelStrategy.drain) {

    val source = Source(1 to elementCount)
    val groupStream = source.splitAfter(substreamCancelStrategy)(_ == splitAfter).lift.runWith(Sink.asPublisher(false))
    val masterSubscriber = TestSubscriber.manualProbe[Source[Int, _]]()

    groupStream.subscribe(masterSubscriber)
    val masterSubscription = masterSubscriber.expectSubscription()

    def expectSubFlow(): Source[Int, _] = {
      masterSubscription.request(1)
      expectSubPublisher()
    }

    def expectSubPublisher(): Source[Int, _] = {
      val substream = masterSubscriber.expectNext()
      substream
    }

  }

  "splitAfter" must {

    "work in the happy case" in {
      new SubstreamsSupport(3, elementCount = 5) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        masterSubscriber.expectNoMessage(100.millis)

        s1.request(2)
        s1.expectNext(1)
        s1.expectNext(2)
        s1.request(1)
        s1.expectNext(3)
        s1.request(1)
        s1.expectComplete()

        val s2 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))

        s2.request(2)
        s2.expectNext(4)
        s2.expectNext(5)
        s2.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "work when first element is split-by" in {
      new SubstreamsSupport(splitAfter = 1, elementCount = 3) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        masterSubscriber.expectNoMessage(100.millis)

        s1.request(3)
        s1.expectNext(1)
        s1.expectComplete()

        val s2 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))

        s2.request(3)
        s2.expectNext(2)
        s2.expectNext(3)
        s2.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "work with single elem splits" in {
      Await.result(
        Source(1 to 10)
          .splitAfter(_ => true)
          .lift
          .mapAsync(1)(_.runWith(Sink.head)) // Please note that this line *also* implicitly asserts nonempty substreams
          .grouped(10)
          .runWith(Sink.head),
        3.second) should ===(1 to 10)
    }

    "support cancelling substreams" in {
      new SubstreamsSupport(splitAfter = 5, elementCount = 8) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        s1.cancel()
        val s2 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))

        s2.request(4)
        s2.expectNext(6)
        s2.expectNext(7)
        s2.expectNext(8)
        s2.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "support cancelling the master stream" in {
      new SubstreamsSupport(splitAfter = 5, elementCount = 8) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        masterSubscription.cancel()
        s1.request(5)
        s1.expectNext(1)
        s1.expectNext(2)
        s1.expectNext(3)
        s1.expectNext(4)
        s1.expectNext(5)
        s1.request(1)
        s1.expectComplete()
      }
    }

    "fail stream when splitAfter function throws" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val exc = TE("test")
      val publisher = Source
        .fromPublisher(publisherProbeProbe)
        .splitAfter(elem => if (elem == 3) throw exc else elem % 3 == 0)
        .lift
        .runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[Source[Int, NotUsed]]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      upstreamSubscription.sendNext(1)

      val substream = subscriber.expectNext()
      val substreamPuppet = StreamPuppet(substream.runWith(Sink.asPublisher(false)))

      substreamPuppet.request(10)
      substreamPuppet.expectNext(1)

      upstreamSubscription.sendNext(2)
      substreamPuppet.expectNext(2)

      upstreamSubscription.sendNext(3)

      subscriber.expectError(exc)
      substreamPuppet.expectError(exc)
      upstreamSubscription.expectCancellation()
    }

    "resume stream when splitAfter function throws" in {
      info("Supervision is not supported fully by GraphStages yet")
      pending
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val exc = TE("test")
      val publisher = Source
        .fromPublisher(publisherProbeProbe)
        .splitAfter(elem => if (elem == 3) throw exc else elem % 3 == 0)
        .lift
        .withAttributes(ActorAttributes.supervisionStrategy(resumingDecider))
        .runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[Source[Int, NotUsed]]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      upstreamSubscription.sendNext(1)

      val substream1 = subscriber.expectNext()
      val substreamPuppet1 = StreamPuppet(substream1.runWith(Sink.asPublisher(false)))

      substreamPuppet1.request(10)
      substreamPuppet1.expectNext(1)

      upstreamSubscription.sendNext(2)
      substreamPuppet1.expectNext(2)

      upstreamSubscription.sendNext(3)
      upstreamSubscription.sendNext(4)
      substreamPuppet1.expectNext(4) // note that 3 was dropped

      upstreamSubscription.sendNext(5)
      substreamPuppet1.expectNext(5)

      upstreamSubscription.sendNext(6)
      substreamPuppet1.expectNext(6)
      substreamPuppet1.expectComplete()
      val substream2 = subscriber.expectNext()
      val substreamPuppet2 = StreamPuppet(substream2.runWith(Sink.asPublisher(false)))
      substreamPuppet2.request(10)
      upstreamSubscription.sendNext(7)
      substreamPuppet2.expectNext(7)

      upstreamSubscription.sendComplete()
      subscriber.expectComplete()
      substreamPuppet2.expectComplete()
    }

    "pass along early cancellation" in {
      val up = TestPublisher.manualProbe[Int]()
      val down = TestSubscriber.manualProbe[Source[Int, NotUsed]]()

      val flowSubscriber = Source.asSubscriber[Int].splitAfter(_ % 3 == 0).lift.to(Sink.fromSubscriber(down)).run()

      val downstream = down.expectSubscription()
      downstream.cancel()
      up.subscribe(flowSubscriber)
      val upsub = up.expectSubscription()
      upsub.expectCancellation()
    }

    "support eager cancellation of master stream on cancelling substreams" in {
      new SubstreamsSupport(splitAfter = 5, elementCount = 8, SubstreamCancelStrategy.propagate) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        s1.cancel()
        masterSubscriber.expectComplete()
      }
    }

    "work when last element is split-by" in {
      new SubstreamsSupport(splitAfter = 3, elementCount = 3) {
        val s1 = StreamPuppet(expectSubFlow().runWith(Sink.asPublisher(false)))
        masterSubscriber.expectNoMessage(100.millis)

        s1.request(3)
        s1.expectNext(1)
        s1.expectNext(2)
        s1.expectNext(3)
        s1.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "fail stream if substream not materialized in time" in {
      val testSource = Source.single(1).concat(Source.maybe).splitAfter(_ => true)
      val streamWithTightTimeout =
        testSource.lift
          .delay(1.second)
          .flatMapConcat(identity)
          .toMat(Sink.ignore)(Keep.right)
          .withAttributes(ActorAttributes
            .streamSubscriptionTimeout(500.milliseconds, StreamSubscriptionTimeoutTerminationMode.cancel))

      a[SubscriptionTimeoutException] mustBe thrownBy {
        Await.result(streamWithTightTimeout.run(), 3.seconds)
      }
    }

  }

}
