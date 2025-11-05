/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.reactivestreams.Publisher
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.Attributes._
import akka.stream.Supervision.resumingDecider
import akka.stream.impl.SinkModule
import akka.stream.impl.fusing.GroupBy
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestLatch
import akka.util.ByteString

object FlowGroupBySpec {

  implicit class Lift[M](val f: SubFlow[Int, M, Source[Int, M]#Repr, RunnableGraph[M]]) extends AnyVal {
    def lift(key: Int => Int) =
      f.prefixAndTail(1).map(p => key(p._1.head) -> (Source.single(p._1.head) ++ p._2)).concatSubstreams
  }

}

class FlowGroupBySpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {
  import FlowGroupBySpec._

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

  class SubstreamsSupport(groupCount: Int = 2, elementCount: Int = 6, maxSubstreams: Int = -1) {
    val source = Source(1 to elementCount).runWith(Sink.asPublisher(false))
    val max = if (maxSubstreams > 0) maxSubstreams else groupCount
    val groupStream =
      Source.fromPublisher(source).groupBy(max, _ % groupCount).lift(_ % groupCount).runWith(Sink.asPublisher(false))
    val masterSubscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()

    groupStream.subscribe(masterSubscriber)
    val masterSubscription = masterSubscriber.expectSubscription()

    def getSubFlow(expectedKey: Int): Source[Int, _] = {
      masterSubscription.request(1)
      expectSubFlow(expectedKey)
    }

    def expectSubFlow(expectedKey: Int): Source[Int, _] = {
      val (key, substream) = masterSubscriber.expectNext()
      key should be(expectedKey)
      substream
    }

  }

  def randomByteString(size: Int): ByteString = {
    val a = new Array[Byte](size)
    ThreadLocalRandom.current().nextBytes(a)
    ByteString(a)
  }

  "groupBy" must {
    "work in the happy case" in {
      new SubstreamsSupport(groupCount = 2) {
        val s1 = StreamPuppet(getSubFlow(1).runWith(Sink.asPublisher(false)))
        masterSubscriber.expectNoMessage(100.millis)

        s1.expectNoMessage(100.millis)
        s1.request(1)
        s1.expectNext(1)
        s1.expectNoMessage(100.millis)

        val s2 = StreamPuppet(getSubFlow(0).runWith(Sink.asPublisher(false)))

        s2.expectNoMessage(100.millis)
        s2.request(2)
        s2.expectNext(2)

        // Important to request here on the OTHER stream because the buffer space is exactly one without the fanout box
        s1.request(1)
        s2.expectNext(4)

        s2.expectNoMessage(100.millis)

        s1.expectNext(3)

        s2.request(1)
        // Important to request here on the OTHER stream because the buffer space is exactly one without the fanout box
        s1.request(1)
        s2.expectNext(6)
        s2.expectComplete()

        s1.expectNext(5)
        s1.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "work in normal user scenario" in {
      Source(List("Aaa", "Abb", "Bcc", "Cdd", "Cee"))
        .groupBy(3, _.substring(0, 1))
        .grouped(10)
        .mergeSubstreams
        .grouped(10)
        .runWith(Sink.head)
        .futureValue(Timeout(3.seconds))
        .sortBy(_.head) should ===(List(List("Aaa", "Abb"), List("Bcc"), List("Cdd", "Cee")))
    }

    "fail when key function return null" in {
      val down = Source(List("Aaa", "Abb", "Bcc", "Cdd", "Cee"))
        .groupBy(3, e => if (e.startsWith("A")) null else e.substring(0, 1))
        .grouped(10)
        .mergeSubstreams
        .runWith(TestSink[Seq[String]]())
      down.request(1)
      val ex = down.expectError()
      ex.getMessage.indexOf("Key cannot be null") should not be (-1)
      ex.isInstanceOf[IllegalArgumentException] should be(true)
    }

    "accept cancellation of substreams" in {
      new SubstreamsSupport(groupCount = 2, maxSubstreams = 3) {
        StreamPuppet(getSubFlow(1).runWith(Sink.asPublisher(false))).cancel()

        val substream = StreamPuppet(getSubFlow(0).runWith(Sink.asPublisher(false)))
        substream.request(2)
        substream.expectNext(2)
        substream.expectNext(4)
        substream.expectNoMessage(100.millis)

        substream.request(2)
        substream.expectNext(6)
        substream.expectComplete()

        masterSubscription.request(1)
        masterSubscriber.expectComplete()
      }
    }

    "accept cancellation of master stream when not consumed anything" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val publisher =
        Source.fromPublisher(publisherProbeProbe).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()
      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.cancel()
      upstreamSubscription.expectCancellation()
    }

    "work with empty input stream" in {
      val publisher = Source(List.empty[Int]).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      publisher.subscribe(subscriber)

      subscriber.expectSubscriptionAndComplete()
    }

    "abort on onError from upstream" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val publisher =
        Source.fromPublisher(publisherProbeProbe).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      val e = TE("test")
      upstreamSubscription.sendError(e)

      subscriber.expectError(e)
    }

    "abort on onError from upstream when substreams are running" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val publisher =
        Source.fromPublisher(publisherProbeProbe).groupBy(2, _ % 2).lift(_ % 2).runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, _])]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      upstreamSubscription.sendNext(1)

      val (_, substream) = subscriber.expectNext()
      val substreamPuppet = StreamPuppet(substream.runWith(Sink.asPublisher(false)))

      substreamPuppet.request(1)
      substreamPuppet.expectNext(1)

      val e = TE("test")
      upstreamSubscription.sendError(e)

      substreamPuppet.expectError(e)
      subscriber.expectError(e)

    }

    "fail stream when groupBy function throws" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val exc = TE("test")
      val publisher = Source
        .fromPublisher(publisherProbeProbe)
        .groupBy(2, elem => if (elem == 2) throw exc else elem % 2)
        .lift(_ % 2)
        .runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, NotUsed])]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      upstreamSubscription.sendNext(1)

      val (_, substream) = subscriber.expectNext()
      val substreamPuppet = StreamPuppet(substream.runWith(Sink.asPublisher(false)))

      substreamPuppet.request(1)
      substreamPuppet.expectNext(1)

      upstreamSubscription.sendNext(2)

      subscriber.expectError(exc)
      substreamPuppet.expectError(exc)
      upstreamSubscription.expectCancellation()
    }

    "resume stream when groupBy function throws" in {
      val publisherProbeProbe = TestPublisher.manualProbe[Int]()
      val exc = TE("test")
      val publisher = Source
        .fromPublisher(publisherProbeProbe)
        .groupBy(2, elem => if (elem == 2) throw exc else elem % 2)
        .lift(_ % 2)
        .withAttributes(ActorAttributes.supervisionStrategy(resumingDecider))
        .runWith(Sink.asPublisher(false))
      val subscriber = TestSubscriber.manualProbe[(Int, Source[Int, NotUsed])]()
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbeProbe.expectSubscription()

      val downstreamSubscription = subscriber.expectSubscription()
      downstreamSubscription.request(100)

      upstreamSubscription.sendNext(1)

      val (_, substream1) = subscriber.expectNext()
      val substreamPuppet1 = StreamPuppet(substream1.runWith(Sink.asPublisher(false)))
      substreamPuppet1.request(10)
      substreamPuppet1.expectNext(1)

      upstreamSubscription.sendNext(2)
      upstreamSubscription.sendNext(4)

      val (_, substream2) = subscriber.expectNext()
      val substreamPuppet2 = StreamPuppet(substream2.runWith(Sink.asPublisher(false)))
      substreamPuppet2.request(10)
      substreamPuppet2.expectNext(4) // note that 2 was dropped

      upstreamSubscription.sendNext(3)
      substreamPuppet1.expectNext(3)

      upstreamSubscription.sendNext(6)
      substreamPuppet2.expectNext(6)

      upstreamSubscription.sendComplete()
      subscriber.expectComplete()
      substreamPuppet1.expectComplete()
      substreamPuppet2.expectComplete()
    }

    "pass along early cancellation" in {
      val up = TestPublisher.manualProbe[Int]()
      val down = TestSubscriber.manualProbe[(Int, Source[Int, NotUsed])]()

      val flowSubscriber = Source.asSubscriber[Int].groupBy(2, _ % 2).lift(_ % 2).to(Sink.fromSubscriber(down)).run()

      val downstream = down.expectSubscription()
      downstream.cancel()
      up.subscribe(flowSubscriber)
      val upsub = up.expectSubscription()
      upsub.expectCancellation()
    }

    "fail when exceeding maxSubstreams" in {
      val (up, down) =
        Flow[Int].groupBy(1, _ % 2).prefixAndTail(0).mergeSubstreams.runWith(TestSource[Int](), TestSink())

      down.request(2)

      up.sendNext(1)
      val first = down.expectNext()
      val s1 = StreamPuppet(first._2.runWith(Sink.asPublisher(false)))

      s1.request(1)
      s1.expectNext(1)

      up.sendNext(2)
      val ex = down.expectError()
      ex.getMessage should include("too many substreams")
      s1.expectError(ex)
      up.expectCancellation()
    }

    "resume when exceeding maxSubstreams" in {
      val (up, down) = Flow[Int]
        .groupBy(0, identity)
        .mergeSubstreams
        .withAttributes(ActorAttributes.supervisionStrategy(resumingDecider))
        .runWith(TestSource[Int](), TestSink())

      down.request(1)

      up.sendNext(1)
      down.expectNoMessage(1.second)
      up.sendComplete()
      down.expectComplete()
    }

    "emit subscribe before completed" in {
      val futureGroupSource =
        Source.single(0).groupBy(1, _ => "all").prefixAndTail(0).map(_._2).concatSubstreams.runWith(Sink.head)
      val pub: Publisher[Int] = Await.result(futureGroupSource, 3.seconds).runWith(Sink.asPublisher(false))
      val probe = TestSubscriber.manualProbe[Int]()
      pub.subscribe(probe)
      val sub = probe.expectSubscription()
      sub.request(1)
      probe.expectNext(0)
      probe.expectComplete()

    }

    "work under fuzzing stress test" in {
      val publisherProbe = TestPublisher.manualProbe[ByteString]()
      val subscriber = TestSubscriber.manualProbe[ByteString]()

      val publisher = Source
        .fromPublisher[ByteString](publisherProbe)
        .groupBy(256, elem => elem.head)
        .map(_.reverse)
        .mergeSubstreams
        .groupBy(256, elem => elem.head)
        .map(_.reverse)
        .mergeSubstreams
        .runWith(Sink.asPublisher(false))
      publisher.subscribe(subscriber)

      val upstreamSubscription = publisherProbe.expectSubscription()
      val downstreamSubscription = subscriber.expectSubscription()

      downstreamSubscription.request(300)
      for (_ <- 1 to 300) {
        val byteString = randomByteString(10)
        upstreamSubscription.expectRequest()
        upstreamSubscription.sendNext(byteString)
        subscriber.expectNext() should ===(byteString)
      }
      upstreamSubscription.sendComplete()
    }

    "work if pull is exercised from both substream and main stream (#20829)" in {
      val upstream = TestPublisher.probe[Int]()
      val downstreamMaster = TestSubscriber.probe[Source[Int, NotUsed]]()

      Source
        .fromPublisher(upstream)
        .via(new GroupBy[Int, Boolean](2, elem => elem == 0))
        .runWith(Sink.fromSubscriber(downstreamMaster))

      val substream = TestSubscriber.probe[Int]()

      downstreamMaster.request(1)
      upstream.sendNext(1)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream))

      // Read off first buffered element from subsource
      substream.request(1)
      substream.expectNext(1)

      // Both will attempt to pull upstream
      substream.request(1)
      substream.expectNoMessage(100.millis)
      downstreamMaster.request(1)
      downstreamMaster.expectNoMessage(100.millis)

      // Cleanup, not part of the actual test
      substream.cancel()
      downstreamMaster.cancel()
      upstream.sendComplete()
    }

    "work if pull is exercised from multiple substreams while downstream is backpressuring (#24353)" in {
      val upstream = TestPublisher.probe[Int]()
      val downstreamMaster = TestSubscriber.probe[Source[Int, NotUsed]]()

      Source
        .fromPublisher(upstream)
        .via(new GroupBy[Int, Int](10, elem => elem))
        .runWith(Sink.fromSubscriber(downstreamMaster))

      val substream1 = TestSubscriber.probe[Int]()
      downstreamMaster.request(1)
      upstream.sendNext(1)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream1))

      val substream2 = TestSubscriber.probe[Int]()
      downstreamMaster.request(1)
      upstream.sendNext(2)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream2))

      substream1.request(1)
      substream1.expectNext(1)
      substream2.request(1)
      substream2.expectNext(2)

      // Both substreams pull
      substream1.request(1)
      substream2.request(1)

      // Upstream sends new groups
      upstream.sendNext(3)
      upstream.sendNext(4)

      val substream3 = TestSubscriber.probe[Int]()
      val substream4 = TestSubscriber.probe[Int]()
      downstreamMaster.request(1)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream3))
      downstreamMaster.request(1)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream4))

      substream3.request(1)
      substream3.expectNext(3)
      substream4.request(1)
      substream4.expectNext(4)

      // Cleanup, not part of the actual test
      substream1.cancel()
      substream2.cancel()
      substream3.cancel()
      substream4.cancel()
      downstreamMaster.cancel()
      upstream.sendComplete()
    }

    "allow to recreate an already closed substream (#24758)" in {
      val (up, down) = Flow[Int]
        .groupBy(2, identity, true)
        .take(1) // close the substream after 1 element
        .mergeSubstreams
        .runWith(TestSource[Int](), TestSink())

      down.request(4)

      // Creates and closes substream "1"
      up.sendNext(1)
      down.expectNext(1)

      // Creates and closes substream "2"
      up.sendNext(2)
      down.expectNext(2)

      // Recreates and closes substream "1" twice
      up.sendNext(1)
      down.expectNext(1)
      up.sendNext(1)
      down.expectNext(1)

      // Cleanup, not part of the actual test
      up.sendComplete()
      down.expectComplete()
    }

    "cancel if downstream has cancelled & all substreams cancel" in {
      val upstream = TestPublisher.probe[Int]()
      val downstreamMaster = TestSubscriber.probe[Source[Int, NotUsed]]()

      Source
        .fromPublisher(upstream)
        .via(new GroupBy[Int, Int](10, elem => elem))
        .runWith(Sink.fromSubscriber(downstreamMaster))

      val substream1 = TestSubscriber.probe[Int]()
      downstreamMaster.request(1)
      upstream.sendNext(1)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream1))

      val substream2 = TestSubscriber.probe[Int]()
      downstreamMaster.request(1)
      upstream.sendNext(2)
      downstreamMaster.expectNext().runWith(Sink.fromSubscriber(substream2))

      // Cancel downstream
      downstreamMaster.cancel()

      // Both substreams still work
      substream1.request(1)
      substream1.expectNext(1)
      substream2.request(1)
      substream2.expectNext(2)

      // New keys are ignored
      upstream.sendNext(3)
      upstream.sendNext(4)

      // Cancel all substreams
      substream1.cancel()
      substream2.cancel()

      // Upstream gets cancelled
      upstream.expectCancellation()
    }

    "work with random demand" in {
      val probes = IndexedSeq.fill(100)(Promise[TestSubscriber.Probe[ByteString]]())

      final class ProbeSink(val attributes: Attributes, shape: SinkShape[ByteString])(implicit system: ActorSystem)
          extends SinkModule[ByteString, TestSubscriber.Probe[ByteString]](shape) {

        // materialized on demand by GroupBy so we need thread safety here
        val materializationCounter = new AtomicInteger(0)

        override def create(context: MaterializationContext) = {
          val index = materializationCounter.getAndIncrement()
          val promise = probes(index)
          val probe = TestSubscriber.probe[ByteString]()
          promise.success(probe)
          (probe, probe)
        }
        override def withAttributes(attr: Attributes): SinkModule[ByteString, TestSubscriber.Probe[ByteString]] =
          new ProbeSink(attr, amendShape(attr))
      }

      case class SubFlowState(probe: TestSubscriber.Probe[ByteString], hasDemand: Boolean, firstElement: ByteString)
      val map = mutable.Map.empty[Int, SubFlowState]
      var blockingNextElement: ByteString = null.asInstanceOf[ByteString]
      @tailrec
      def randomDemand(): Unit = {
        val nextIndex = ThreadLocalRandom.current().nextInt(0, map.size)
        val key = map.keySet.toIndexedSeq(nextIndex)
        if (!map(key).hasDemand) {
          val state = map(key)
          map.put(key, SubFlowState(state.probe, true, state.firstElement))

          state.probe.request(1)

          //need to verify elements that are first element in subFlow or is in nextElement buffer before
          // pushing next element from upstream
          if (state.firstElement != null) {
            state.probe.expectNext() should ===(state.firstElement)
            map.put(key, SubFlowState(state.probe, false, null))
            randomDemand()
          } else if (blockingNextElement != null && Math.abs(blockingNextElement.head % 100) == key) {
            state.probe.expectNext() should ===(blockingNextElement)
            blockingNextElement = null
            map.put(key, SubFlowState(state.probe, false, null))
            randomDemand()
          } else if (blockingNextElement != null) randomDemand()
        } else randomDemand()
      }

      val publisherProbe = TestPublisher.manualProbe[ByteString]()
      val runnable = Source
        .fromPublisher[ByteString](publisherProbe)
        .groupBy(100, elem => Math.abs(elem.head % 100))
        .to(Sink.fromGraph(new ProbeSink(none, SinkShape(Inlet("ProbeSink.in")))))

      runnable.withAttributes(Attributes.inputBuffer(1, 1)).run()

      val upstreamSubscription = publisherProbe.expectSubscription()

      var probeIndex = 0
      for (_ <- 1 to 400) {
        val byteString = randomByteString(10)
        val index = Math.abs(byteString.head % 100)

        upstreamSubscription.expectRequest()
        upstreamSubscription.sendNext(byteString)

        if (!map.contains(index)) {
          val probe: TestSubscriber.Probe[ByteString] = Await.result(probes(probeIndex).future, 300.millis)
          probeIndex += 1
          map.put(index, SubFlowState(probe, false, byteString))
          //stream automatically requests next element
        } else {
          val state = map(index)
          if (state.firstElement != null) { //first element in subFlow
            if (!state.hasDemand) blockingNextElement = byteString
            randomDemand()
          } else if (state.hasDemand) {
            if (blockingNextElement == null) {
              state.probe.expectNext() should ===(byteString)
              map.put(index, SubFlowState(state.probe, false, null))
              randomDemand()
            } else fail("INVALID CASE")
          } else {
            blockingNextElement = byteString
            randomDemand()
          }
        }
      }

      // complete has some try to feed downstream logic, we're not testing that here
      // we just want to kill all substreams so lets fail the stream instead
      upstreamSubscription.sendError(TE("killing this stream"))
      map.values.foreach { subFlowState =>
        // not all may have seen a subscription because random selection above
        subFlowState.probe.ensureSubscription()
        subFlowState.probe.expectError()

      }
    }

    "not block all substreams when one is blocked but has a buffer in front" in {
      case class Elem(id: Int, substream: Int, f: () => Any)
      val queue = Source
        .queue[Elem](3)
        .groupBy(2, _.substream)
        .buffer(2, OverflowStrategy.backpressure)
        .map { _.f() }
        .async
        .to(Sink.ignore)
        .run()

      val threeProcessed = Promise[Done]()
      val blockSubStream1 = TestLatch()
      List(Elem(1, 1, () => {
        // timeout just to not wait forever if something is wrong, not really relevant for test
        Await.result(blockSubStream1, 10.seconds)
        1
      }), Elem(2, 1, () => 2), Elem(3, 2, () => {
        threeProcessed.success(Done)
        3
      })).foreach(queue.offer)
      // two and three are processed as fast as possible, not blocked by substream 1 being clogged
      threeProcessed.future.futureValue should ===(Done)
      // let 1 pass so stream can complete
      blockSubStream1.open()
      queue.complete()
    }

    "not throw tooManySubstreamsOpenException for element on closed substream" in {
      val publisher = TestPublisher.Probe[(Int, Boolean)]()
      val outProbe =
        Source.fromPublisher(publisher).groupBy(2, _._1).takeWhile(_._2 != false).mergeSubstreams.runWith(TestSink())
      outProbe.request(4)
      publisher.sendNext((1, true))
      outProbe.expectNext((1, true))
      publisher.sendNext((2, true))
      outProbe.expectNext((2, true))
      publisher.sendNext((2, false)) // substream 2 completed
      publisher.sendNext((2, false)) // should be dropped, not crash the stream
      publisher.sendNext((1, true))
      outProbe.expectNext((1, true))

      outProbe.cancel()
    }

  }

}
