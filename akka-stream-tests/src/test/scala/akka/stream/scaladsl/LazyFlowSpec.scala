/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.{ Done, NotUsed }
import akka.stream.{ AbruptStageTerminationException, Attributes, Materializer, NeverMaterializedException }
import akka.stream.Attributes.Attribute
import akka.stream.scaladsl.AttributesSpec.AttributesFlow
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestProbe

@nowarn("msg=deprecated") // tests deprecated API as well
class LazyFlowSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """) {

  import system.dispatcher
  val ex = TE("")
  case class MyAttribute() extends Attribute
  val myAttributes = Attributes(MyAttribute())

  "Flow.lazyFlow" must {
    // more complete test coverage is for lazyFutureFlow since this is composition of that
    "work in the happy case" in {
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFlow(() => Flow.fromFunction((n: Int) => n.toString)))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.futureValue should equal(Seq("1", "2", "3"))
      deferredMatVal.isCompleted should ===(true)
    }

    "provide attributes to inner flow" in {
      val attributes = Source
        .single(Done)
        .viaMat(Flow.lazyFlow(() => Flow.fromGraph(new AttributesFlow())))(Keep.right)
        .addAttributes(myAttributes)
        .to(Sink.head)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute shouldBe Some(MyAttribute())
    }
  }

  "Flow.futureFlow" must {
    // more complete test coverage is for lazyFutureFlow since this is composition of that
    "work in the happy case" in {
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.futureFlow(Future.successful(Flow.fromFunction((n: Int) => n.toString))))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.futureValue should equal(Seq("1", "2", "3"))
      deferredMatVal.isCompleted should ===(true)
    }

    "provide attributes to inner flow" in {
      val attributes = Source
        .single(Done)
        .viaMat(Flow.futureFlow(Future(Flow.fromGraph(new AttributesFlow()))))(Keep.right)
        .addAttributes(myAttributes)
        .to(Sink.head)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute shouldBe Some(MyAttribute())
    }
  }

  "Flow.lazyFutureFlow" must {

    "work in the happy case" in {
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() => Future.successful(Flow.fromFunction((n: Int) => n.toString))))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.futureValue should equal(Seq("1", "2", "3"))
      deferredMatVal.isCompleted should ===(true)
    }

    "complete without creating internal flow when there was no elements in the stream" in {
      val probe = TestProbe()
      val result: (Future[NotUsed], Future[immutable.Seq[Int]]) = Source
        .empty[Int]
        .viaMat(Flow.lazyFutureFlow { () =>
          probe.ref ! "constructed"
          Future.successful(Flow[Int])
        })(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      val deferredMatVal = result._1
      val list = result._2
      list.futureValue should equal(Seq.empty)
      // and failing the matval
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      probe.expectNoMessage(30.millis) // would have gotten it by now
    }

    "complete without creating internal flow when the stream failed with no elements" in {
      val probe = TestProbe()
      val result: (Future[NotUsed], Future[immutable.Seq[Int]]) = Source
        .failed[Int](TE("no-elements"))
        .viaMat(Flow.lazyFutureFlow { () =>
          probe.ref ! "constructed"
          Future.successful(Flow[Int])
        })(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      val deferredMatVal = result._1
      val list = result._2
      list.failed.futureValue shouldBe a[TE]
      // and failing the matval
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      probe.expectNoMessage(30.millis) // would have gotten it by now
    }

    "fail the flow when the factory function fails" in {
      @nowarn("cat=lint-infer-any")
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() => throw TE("no-flow-for-you")))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.failed.futureValue shouldBe a[TE]
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      deferredMatVal.failed.futureValue.getCause shouldBe a[TE]
    }

    "fail the flow when the future is initially failed" in {
      @nowarn("cat=lint-infer-any")
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() => Future.failed(TE("no-flow-for-you"))))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.failed.futureValue shouldBe a[TE]
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      deferredMatVal.failed.futureValue.getCause shouldBe a[TE]
    }

    "fail the flow when the future is failed after the fact" in {
      val promise = Promise[Flow[Int, String, NotUsed]]()
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() => promise.future))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2

      promise.failure(TE("later-no-flow-for-you"))
      list.failed.futureValue shouldBe a[TE]
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      deferredMatVal.failed.futureValue.getCause shouldBe a[TE]
    }

    "work for a single element when the future is completed after the fact" in {
      import system.dispatcher
      val flowPromise = Promise[Flow[Int, String, NotUsed]]()
      val firstElementArrived = Promise[Done]()

      val result: Future[immutable.Seq[String]] =
        Source(List(1))
          .via(Flow.lazyFutureFlow { () =>
            firstElementArrived.success(Done)
            flowPromise.future
          })
          .runWith(Sink.seq)

      firstElementArrived.future.map { _ =>
        flowPromise.success(Flow[Int].map(_.toString))
      }

      result.futureValue shouldBe List("1")
    }

    "fail the flow when the future materialization fails" in {
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() =>
            Future.successful(Flow[Int].map(_.toString).mapMaterializedValue(_ => throw TE("mat-failed")))))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2
      list.failed.futureValue shouldBe a[TE]
      //futureFlow's behaviour in case of mat failure (follows flatMapPrefix)
      deferredMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
      deferredMatVal.failed.futureValue.getCause shouldEqual TE("mat-failed")
    }

    "fail the flow when there was elements but the inner flow failed" in {
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source(List(1, 2, 3))
          .viaMat(Flow.lazyFutureFlow(() => Future.successful(Flow[Int].map(_ => throw TE("inner-stream-fail")))))(
            Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val deferredMatVal = result._1
      val list = result._2

      list.failed.futureValue shouldBe a[TE]
      deferredMatVal.futureValue should ===(NotUsed) // inner materialization did succeed
    }

    "fail the mat val when the stream is abruptly terminated before it got materialized" in {
      val expendableMaterializer = Materializer(system)
      val promise = Promise[Flow[Int, String, NotUsed]]()
      val result: (Future[NotUsed], Future[immutable.Seq[String]]) =
        Source
          .maybe[Int]
          .viaMat(Flow.lazyFutureFlow(() => promise.future))(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .run()(expendableMaterializer)

      val deferredMatVal = result._1
      val list = result._2

      expendableMaterializer.shutdown()

      list.failed.futureValue shouldBe an[AbruptStageTerminationException]
      deferredMatVal.failed.futureValue shouldBe an[AbruptStageTerminationException]
    }

    "provide attributes to inner flow" in {
      val attributes = Source
        .single(Done)
        .viaMat(Flow.lazyFutureFlow(() => Future(Flow.fromGraph(new AttributesFlow()))))(Keep.right)
        .addAttributes(myAttributes)
        .to(Sink.head)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute shouldBe Some(MyAttribute())
    }
  }

  "The deprecated LazyFlow ops" must {
    def mapF(e: Int): () => Future[Flow[Int, String, NotUsed]] =
      () => Future.successful(Flow.fromFunction[Int, String](i => (i * e).toString))
    val flowF = Future.successful(Flow[Int])
    "work in happy case" in {
      val probe = Source(2 to 10).via(Flow.lazyInitAsync[Int, String, NotUsed](mapF(2))).runWith(TestSink[String]())
      probe.request(100)
      (2 to 10).map(i => (i * 2).toString).foreach(probe.expectNext)
    }

    "work with slow flow init" in {
      val p = Promise[Flow[Int, Int, NotUsed]]()
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val flowProbe = Source
        .fromPublisher(sourceProbe)
        .via(Flow.lazyInitAsync[Int, Int, NotUsed](() => p.future))
        .runWith(TestSink[Int]())

      val sourceSub = sourceProbe.expectSubscription()
      flowProbe.request(1)
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectRequest(1)
      sourceProbe.expectNoMessage(200.millis)

      p.success(Flow[Int])
      flowProbe.request(99)
      flowProbe.expectNext(0)
      (1 to 10).foreach(i => {
        sourceSub.sendNext(i)
        flowProbe.expectNext(i)
      })
      sourceSub.sendComplete()
    }

    "complete when there was no elements in the stream" in {
      def flowMaker() = flowF
      val probe = Source.empty.via(Flow.lazyInitAsync(() => flowMaker())).runWith(TestSink[Int]())
      probe.request(1).expectComplete()
    }

    "complete normally when upstream completes BEFORE the stage has switched to the inner flow" in {
      val promise = Promise[Flow[Int, Int, NotUsed]]()
      val (pub, sub) =
        TestSource[Int]().viaMat(Flow.lazyInitAsync(() => promise.future))(Keep.left).toMat(TestSink())(Keep.both).run()
      sub.request(1)
      pub.sendNext(1).sendComplete()
      promise.success(Flow[Int])
      sub.expectNext(1).expectComplete()
    }

    "complete normally when upstream completes AFTER the stage has switched to the inner flow" in {
      val (pub, sub) = TestSource[Int]()
        .viaMat(Flow.lazyInitAsync(() => Future.successful(Flow[Int])))(Keep.left)
        .toMat(TestSink())(Keep.both)
        .run()
      sub.request(1)
      pub.sendNext(1)
      sub.expectNext(1)
      pub.sendComplete()
      sub.expectComplete()
    }

    "fail gracefully when flow factory function failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val probe = Source
        .fromPublisher(sourceProbe)
        .via(Flow.lazyInitAsync[Int, Int, NotUsed](() => throw ex))
        .runWith(TestSink[Int]())

      val sourceSub = sourceProbe.expectSubscription()
      probe.request(1)
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectCancellation()
      probe.expectError(ex)
    }

    "fail gracefully when upstream failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val probe = Source.fromPublisher(sourceProbe).via(Flow.lazyInitAsync(() => flowF)).runWith(TestSink())

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      probe.request(1).expectNext(0)
      sourceSub.sendError(ex)
      probe.expectError(ex)
    }

    "fail gracefully when factory future failed" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val flowProbe = Source
        .fromPublisher(sourceProbe)
        .via(Flow.lazyInitAsync[Int, Int, NotUsed](() => Future.failed(ex)))
        .runWith(TestSink())

      val sourceSub = sourceProbe.expectSubscription()
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      flowProbe.request(1).expectError(ex)
    }

    "cancel upstream when the downstream is cancelled" in {
      val sourceProbe = TestPublisher.manualProbe[Int]()
      val probe =
        Source
          .fromPublisher(sourceProbe)
          .via(Flow.lazyInitAsync[Int, Int, NotUsed](() => flowF))
          .runWith(TestSink[Int]())

      val sourceSub = sourceProbe.expectSubscription()
      probe.request(1)
      sourceSub.expectRequest(1)
      sourceSub.sendNext(0)
      sourceSub.expectRequest(1)
      probe.expectNext(0)
      probe.cancel()
      sourceSub.expectCancellation()
    }
  }

}
