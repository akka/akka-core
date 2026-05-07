/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent._
import scala.concurrent.duration._

import org.scalatest.exceptions.TestFailedException

import akka.NotUsed
import akka.stream._
import akka.stream.impl.TraversalBuilder
import akka.stream.impl.fusing.GraphStages.SingleSource
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestLatch
import akka.util.OptionVal

class FlowFlattenMergeSpec extends StreamSpec {
  import system.dispatcher

  def src10(i: Int) = Source(i until (i + 10))
  def blocked = Source.future(Promise[Int]().future)

  val toSeq = Flow[Int].grouped(1000).toMat(Sink.head)(Keep.right)
  val toSet = toSeq.mapMaterializedValue(_.map(_.toSet))

  "A FattenMerge" must {

    "work in the nominal case" in {
      Source(List(src10(0), src10(10), src10(20), src10(30)))
        .flatMapMerge(4, identity)
        .runWith(toSet)
        .futureValue should ===((0 until 40).toSet)
    }

    "not be held back by one slow stream" in {
      Source(List(src10(0), src10(10), blocked, src10(20), src10(30)))
        .flatMapMerge(3, identity)
        .take(40)
        .runWith(toSet)
        .futureValue should ===((0 until 40).toSet)
    }

    "respect breadth" in {
      val seq = Source(List(src10(0), src10(10), src10(20), blocked, blocked, src10(30)))
        .flatMapMerge(3, identity)
        .take(40)
        .runWith(toSeq)
        .futureValue

      seq.take(30).toSet should ===((0 until 30).toSet)
      seq.drop(30).toSet should ===((30 until 40).toSet)
    }

    "propagate early failure from main stream" in {
      val ex = new Exception("buh")
      intercept[TestFailedException] {
        Source.failed(ex).flatMapMerge(1, (i: Int) => Source.single(i)).runWith(Sink.head).futureValue
      }.cause.get should ===(ex)
    }

    "propagate late failure from main stream" in {
      val ex = new Exception("buh")
      intercept[TestFailedException] {
        (Source(List(blocked, blocked)) ++ Source.failed(ex)).flatMapMerge(10, identity).runWith(Sink.head).futureValue
      }.cause.get should ===(ex)
    }

    "propagate failure from map function" in {
      val ex = new Exception("buh")
      intercept[TestFailedException] {
        Source(1 to 3).flatMapMerge(10, i => if (i == 3) throw ex else blocked).runWith(Sink.head).futureValue
      }.cause.get should ===(ex)
    }

    "bubble up substream exceptions" in {
      val ex = new Exception("buh")
      intercept[TestFailedException] {
        Source(List(blocked, blocked, Source.failed(ex))).flatMapMerge(10, identity).runWith(Sink.head).futureValue
      }.cause.get should ===(ex)
    }

    "bubble up substream materialization exception" in {
      val matFail = TE("fail!")
      object FailingInnerMat extends GraphStage[SourceShape[String]] {
        val out = Outlet[String]("out")
        val shape = SourceShape(out)
        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          if ("confuse IntellIJ dead code checker".length > 2) {
            throw matFail
          }
        }
      }

      val result = Source.single(()).flatMapMerge(4, _ => Source.fromGraph(FailingInnerMat)).runWith(Sink.ignore)

      result.failed.futureValue should ===(matFail)

    }

    "cancel substreams when failing from main stream" in {
      val p1, p2 = TestPublisher.probe[Int]()
      val ex = new Exception("buh")
      val p = Promise[Source[Int, NotUsed]]()
      (Source(List(Source.fromPublisher(p1), Source.fromPublisher(p2))) ++ Source.future(p.future))
        .flatMapMerge(5, identity)
        .runWith(Sink.head)
      p1.expectRequest()
      p2.expectRequest()
      p.failure(ex)
      p1.expectCancellation()
      p2.expectCancellation()
    }

    "cancel substreams when failing from substream" in {
      val p1, p2 = TestPublisher.probe[Int]()
      val ex = new Exception("buh")
      val p = Promise[Int]()
      Source(List(Source.fromPublisher(p1), Source.fromPublisher(p2), Source.future(p.future)))
        .flatMapMerge(5, identity)
        .runWith(Sink.head)
      p1.expectRequest()
      p2.expectRequest()
      p.failure(ex)
      p1.expectCancellation()
      p2.expectCancellation()
    }

    "cancel substreams when failing map function" in {
      val p = TestPublisher.probe[Int]()
      val ex = new Exception("buh")
      val latch = TestLatch()
      Source(1 to 3)
        .flatMapMerge(10, {
          case 1 => Source.fromPublisher(p)
          case 2 =>
            Await.ready(latch, 3.seconds)
            throw ex
        })
        .toMat(Sink.head)(Keep.right)
        .withAttributes(ActorAttributes.syncProcessingLimit(1) and Attributes.inputBuffer(1, 1))
        .run()
      p.expectRequest()
      latch.countDown()
      p.expectCancellation()
    }

    "cancel substreams when being cancelled" in {
      val p1, p2 = TestPublisher.probe[Int]()
      val sink =
        Source(List(Source.fromPublisher(p1), Source.fromPublisher(p2))).flatMapMerge(5, identity).runWith(TestSink())
      sink.request(1)
      p1.expectRequest()
      p2.expectRequest()
      sink.cancel()
      p1.expectCancellation()
      p2.expectCancellation()
    }

    "work with many concurrently queued events" in {
      val p = Source((0 until 100).map(i => src10(10 * i))).flatMapMerge(Int.MaxValue, identity).runWith(TestSink())
      p.within(1.second) {
        p.ensureSubscription()
        p.expectNoMessage(remainingOrDefault)
      }
      val elems = p.within(1.second)((1 to 1000).map(_ => p.requestNext()).toSet)
      p.expectComplete()
      elems should ===((0 until 1000).toSet)
    }

    val attributesSource = Source.fromGraph(new GraphStage[SourceShape[Attributes]] {
      val out = Outlet[Attributes]("AttributesSource.out")
      override val shape: SourceShape[Attributes] = SourceShape(out)
      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with OutHandler {
          override def onPull(): Unit = {
            push(out, inheritedAttributes)
            completeStage()
          }
          setHandler(out, this)
        }
    })

    "propagate attributes to inner streams" in {
      val f = Source
        .single(attributesSource.addAttributes(Attributes.name("inner")))
        .flatMapMerge(1, identity)
        .addAttributes(Attributes.name("outer"))
        .runWith(Sink.head)

      val attributes = Await.result(f, 3.seconds).attributeList
      attributes should contain(Attributes.Name("inner"))
      attributes should contain(Attributes.Name("outer"))
      attributes.indexOf(Attributes.Name("inner")) < attributes.indexOf(Attributes.Name("outer")) should be(true)
    }

    "work with optimized Source.single" in {
      Source(0 to 3).flatMapConcat(Source.single).runWith(toSeq).futureValue should ===(0 to 3)
    }

    "work with optimized Source.single when slow demand" in {
      val probe = Source(0 to 4).flatMapConcat(Source.single).runWith(TestSink())

      probe.request(3)
      probe.expectNext(0)
      probe.expectNext(1)
      probe.expectNext(2)
      probe.expectNoMessage(100.millis)

      probe.request(10)
      probe.expectNext(3)
      probe.expectNext(4)
      probe.expectComplete()
    }

    "work with mix of Source.single and other sources when slow demand" in {
      val sources: Source[Source[Int, NotUsed], NotUsed] = Source(
        List(
          Source.single(0),
          Source.single(1),
          Source(2 to 4),
          Source.single(5),
          Source(6 to 6),
          Source.single(7),
          Source(8 to 10),
          Source.single(11)))

      val probe =
        sources.flatMapConcat(identity).runWith(TestSink())

      probe.request(3)
      probe.expectNext(0)
      probe.expectNext(1)
      probe.expectNext(2)
      probe.expectNoMessage(100.millis)

      probe.request(1)
      probe.expectNext(3)
      probe.expectNoMessage(100.millis)

      probe.request(1)
      probe.expectNext(4)
      probe.expectNoMessage(100.millis)

      probe.request(3)
      probe.expectNext(5)
      probe.expectNext(6)
      probe.expectNext(7)
      probe.expectNoMessage(100.millis)

      probe.request(10)
      probe.expectNext(8)
      probe.expectNext(9)
      probe.expectNext(10)
      probe.expectNext(11)
      probe.expectComplete()
    }

    "find Source.single via TraversalBuilder" in {
      TraversalBuilder.getSingleSource(Source.single("a")).get.elem should ===("a")
      TraversalBuilder.getSingleSource(Source(List("a", "b"))) should be(OptionVal.None)

      val singleSourceA = new SingleSource("a")
      TraversalBuilder.getSingleSource(singleSourceA) should be(OptionVal.Some(singleSourceA))

      TraversalBuilder.getSingleSource(Source.single("c").async) should be(OptionVal.None)
      TraversalBuilder.getSingleSource(Source.single("d").mapMaterializedValue(_ => "Mat")) should be(OptionVal.None)
    }

  }
}
