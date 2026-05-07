/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.TimeoutException

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.Attributes.Attribute
import akka.stream.scaladsl.AttributesSpec.AttributesSink
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink

@nowarn("msg=deprecated")
class LazySinkSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """) {

  import system.dispatcher
  val ex = TE("")
  case class MyAttribute() extends Attribute
  val myAttributes = Attributes(MyAttribute())

  "A LazySink" must {
    "work in happy case" in {
      val futureProbe = Source(0 to 10).runWith(Sink.lazyInitAsync(() => Future.successful(TestSink[Int]())))
      val probe = Await.result(futureProbe, remainingOrDefault).get
      probe.request(100)
      (0 to 10).foreach(probe.expectNext)
    }

    "work with slow sink init" in {
      val p = Promise[Sink[Int, Probe[Int]]]()
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val futureProbe = Source.fromPublisher(sourceProbe).runWith(Sink.lazyInitAsync(() => p.future))

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectRequest(1)
      sourceProbe.expectNoMessage(200.millis)
      a[TimeoutException] shouldBe thrownBy { Await.result(futureProbe, remainingOrDefault) }

      p.success(TestSink[Int]())
      val probe = Await.result(futureProbe, remainingOrDefault).get
      probe.request(100)
      probe.expectNext(0)
      (1 to 10).foreach(i => {
        sourceSub.sendNext(i)
        probe.expectNext(i)
      })
      sourceSub.sendComplete()
    }

    "complete when there was no elements in stream" in {
      val futureProbe = Source.empty.runWith(Sink.lazyInitAsync(() => Future.successful(Sink.fold[Int, Int](0)(_ + _))))
      val futureResult = Await.result(futureProbe, remainingOrDefault)
      futureResult should ===(None)
    }

    "complete normally when upstream is completed" in {
      val futureProbe = Source.single(1).runWith(Sink.lazyInitAsync(() => Future.successful(TestSink[Int]())))
      val futureResult = Await.result(futureProbe, remainingOrDefault).get
      futureResult.request(1).expectNext(1).expectComplete()
    }

    "failed gracefully when sink factory method failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val futureProbe = Source.fromPublisher(sourceProbe).runWith(Sink.lazyInitAsync[Int, Probe[Int]](() => throw ex))

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectCancellation()
      a[RuntimeException] shouldBe thrownBy { Await.result(futureProbe, remainingOrDefault) }
    }

    "fail gracefully when upstream failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val futureProbe =
        Source.fromPublisher(sourceProbe).runWith(Sink.lazyInitAsync(() => Future.successful(TestSink[Int]())))

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      val probe = Await.result(futureProbe, remainingOrDefault).get
      probe.request(1).expectNext(0)
      sourceSub.sendError(ex)
      probe.expectError(ex)
    }

    "fail gracefully when factory future failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      @nowarn("cat=lint-infer-any")
      val futureProbe = Source.fromPublisher(sourceProbe).runWith(Sink.lazyInitAsync(() => Future.failed(ex)))

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      a[TE] shouldBe thrownBy { Await.result(futureProbe, remainingOrDefault) }
    }

    "cancel upstream when internal sink is cancelled" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val futureProbe =
        Source.fromPublisher(sourceProbe).runWith(Sink.lazyInitAsync(() => Future.successful(TestSink[Int]())))
      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectRequest(1)
      val probe = Await.result(futureProbe, remainingOrDefault).get
      probe.request(1).expectNext(0)
      probe.cancel()
      sourceSub.expectCancellation()
    }

    "fail correctly when materialization of inner sink fails" in {
      val matFail = TE("fail!")
      object FailingInnerMat extends GraphStage[SinkShape[String]] {
        val in = Inlet[String]("in")
        val shape = SinkShape(in)
        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          if ("confuse IntellIJ dead code checker".length > 2) {
            throw matFail
          }
        }
      }

      val result = Source(List("whatever")).runWith(Sink.lazyInitAsync[String, NotUsed](() => {
        Future.successful(Sink.fromGraph(FailingInnerMat))
      }))

      result.failed.futureValue should ===(matFail)
    }

    // reproducer for #25410
    "lazily propagate failure" in {
      case object MyException extends Exception
      val lazyMatVal = Source(List(1))
        .concat(Source.lazily(() => Source.failed(MyException)))
        .runWith(Sink.lazyInitAsync(() => Future.successful(Sink.seq[Int])))

      // lazy init async materialized a sink, so we should have a some here
      val innerMatVal: Future[immutable.Seq[Int]] = lazyMatVal.futureValue.get

      // the actual matval from Sink.seq should be failed when the stream fails
      innerMatVal.failed.futureValue should ===(MyException)

    }

    "provide attributes to inner sink" in {
      val attributes = Source
        .single(Done)
        .toMat(Sink.lazyFutureSink(() => Future(Sink.fromGraph(new AttributesSink()))))(Keep.right)
        .addAttributes(myAttributes)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute should contain(MyAttribute())
    }
  }

}
