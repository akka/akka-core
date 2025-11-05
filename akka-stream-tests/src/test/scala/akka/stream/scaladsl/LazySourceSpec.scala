/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.Promise

import org.scalatest.concurrent.ScalaFutures

import akka.Done
import akka.NotUsed
import akka.stream._
import akka.stream.Attributes.Attribute
import akka.stream.scaladsl.AttributesSpec.AttributesSource
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils.TE
import akka.testkit.DefaultTimeout
import akka.testkit.TestProbe

class LazySourceSpec extends StreamSpec with DefaultTimeout with ScalaFutures {

  import system.dispatcher
  case class MyAttribute() extends Attribute
  val myAttributes = Attributes(MyAttribute())

  "Source.lazySingle" must {
    "work like a normal source, happy path" in {
      val seq = Source.lazySingle(() => 1).runWith(Sink.seq)
      seq.futureValue should ===(Seq(1))
    }

    "never construct the source when there was no demand" in {
      val constructed = new AtomicBoolean(false)
      Source
        .lazySingle { () =>
          constructed.set(true)
          1
        }
        .toMat(Sink.cancelled)(Keep.left)
        .run()

      constructed.get() should ===(false)
    }

    "fail correctly when factory function fails" in {
      val failure = TE("couldn't create")
      val termination: Future[Done] =
        Source.lazySingle(() => throw failure).watchTermination()(Keep.right).toMat(Sink.ignore)(Keep.left).run()

      termination.failed.futureValue should ===(failure)
    }

  }

  "Source.lazyFuture" must {
    "work like a normal source, happy path, already completed future" in {
      val seq = Source.lazyFuture(() => Future.successful(1)).runWith(Sink.seq)

      seq.futureValue should ===(Seq(1))
    }

    "work like a normal source, happy path, completing future" in {
      val promise = Promise[Int]()
      val seq = Source.lazyFuture(() => promise.future).runWith(Sink.seq)
      promise.success(1)
      seq.futureValue should ===(Seq(1))
    }

    "never construct the source when there was no demand" in {
      val constructed = new AtomicBoolean(false)
      val termination = Source
        .lazyFuture { () =>
          constructed.set(true)
          Future.successful(1)
        }
        .watchTermination()(Keep.right)
        .toMat(Sink.cancelled)(Keep.left)
        .run()

      termination.futureValue // stream should terminate
      constructed.get() should ===(false)
    }

    "fail correctly when factory function fails" in {
      val failure = TE("couldn't create")
      val termination =
        Source.lazyFuture(() => throw failure).watchTermination()(Keep.right).toMat(Sink.ignore)(Keep.left).run()

      termination.failed.futureValue should ===(failure)
    }

    "fail correctly when factory function returns a failed future" in {
      val failure = TE("couldn't create")
      val termination =
        Source
          .lazyFuture(() => Future.failed(failure))
          .watchTermination()(Keep.right)
          .toMat(Sink.ignore)(Keep.left)
          .run()

      termination.failed.futureValue should ===(failure)
    }

    "fail correctly when factory function returns a future that fails" in {
      val failure = TE("couldn't create")
      val promise = Promise[Int]()
      val termination =
        Source.lazyFuture(() => promise.future).watchTermination()(Keep.right).toMat(Sink.ignore)(Keep.left).run()
      promise.failure(failure)
      termination.failed.futureValue should ===(failure)
    }
  }

  "Source.lazySource" must {
    "work like a normal source, happy path" in {
      val result = Source.lazySource(() => Source(List(1, 2, 3))).runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
    }

    "never construct the source when there was no demand" in {
      val constructed = new AtomicBoolean(false)
      val (lazySourceMatVal, termination) = Source
        .lazySource { () =>
          constructed.set(true); Source(List(1, 2, 3))
        }
        .watchTermination()(Keep.both)
        .toMat(Sink.cancelled)(Keep.left)
        .run()

      termination.futureValue // stream should terminate
      constructed.get() should ===(false)
      lazySourceMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
    }

    "fail the materialized value when downstream cancels without ever consuming any element" in {
      val lazyMatVal = Source.lazySource(() => Source(List(1, 2, 3))).toMat(Sink.cancelled)(Keep.left).run()

      lazyMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
    }

    "stop consuming when downstream has cancelled" in {
      val outProbe = TestSubscriber.probe[Int]()
      val inProbe = TestPublisher.probe[Int]()

      Source.lazySource(() => Source.fromPublisher(inProbe)).runWith(Sink.fromSubscriber(outProbe))

      outProbe.request(1)
      inProbe.expectRequest()
      inProbe.sendNext(27)
      outProbe.expectNext(27)
      outProbe.cancel()
      inProbe.expectCancellation()
    }

    "materialize when the source has been created" in {
      val probe = TestSubscriber.probe[Int]()

      val matF: Future[Done] = Source
        .lazySource { () =>
          Source(List(1, 2, 3)).mapMaterializedValue(_ => Done)
        }
        .to(Sink.fromSubscriber(probe))
        .run()

      matF.value shouldEqual None
      probe.request(1)
      probe.expectNext(1)
      matF.futureValue should ===(Done)

      probe.cancel()
    }

    "fail stage when upstream fails" in {
      val outProbe = TestSubscriber.probe[Int]()
      val inProbe = TestPublisher.probe[Int]()

      val lazyMatVal =
        Source.lazySource(() => Source.fromPublisher(inProbe)).toMat(Sink.fromSubscriber(outProbe))(Keep.left).run()

      outProbe.request(1)
      inProbe.expectRequest()

      lazyMatVal.futureValue should ===(NotUsed) // was completed

      inProbe.sendNext(27)
      outProbe.expectNext(27)
      val failure = TE("OMG Who set that on fire!?!")
      inProbe.sendError(failure)
      outProbe.expectError() should ===(failure)
    }

    "fail when lazy source is failed" in {
      val failure = TE("OMG Who set that on fire!?!")
      val result = Source.lazySource(() => Source.failed(failure)).runWith(Sink.seq)
      result.failed.futureValue should ===(failure)
    }

    "fail correctly when factory function fails" in {
      val failure = TE("couldn't create")
      val lazyMatVal = Source.lazySource(() => throw failure).toMat(Sink.ignore)(Keep.left).run()

      lazyMatVal.failed.futureValue should ===(failure)
    }

    "fail correctly when materialization of inner source fails" in {
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

      val (lazyMatVal, done) =
        Source.lazySource(() => Source.fromGraph(FailingInnerMat)).toMat(Sink.ignore)(Keep.both).run()

      done.failed.futureValue should ===(matFail)
      lazyMatVal.failed.futureValue should ===(matFail)
    }

    "propagate downstream cancellation cause when inner source has been materialized" in {
      val probe = TestProbe()
      val (doneF, killswitch) =
        Source
          .lazySource(() =>
            Source.maybe[Int].watchTermination()(Keep.right).mapMaterializedValue { done =>
              probe.ref ! Done
              done
            })
          .mapMaterializedValue(_.flatten)
          .viaMat(KillSwitches.single)(Keep.both)
          .to(Sink.ignore)
          .run()
      val boom = TE("boom")
      probe.expectMsg(Done)
      killswitch.abort(boom)
      doneF.failed.futureValue should ===(boom)
    }

    "provide attributes to inner source" in {
      // This stage never stops, but that's OK, that's not what we're testing here.
      val attributes = Source
        .lazySource(() => Source.fromGraph(new AttributesSource()))
        .addAttributes(myAttributes)
        .to(Sink.ignore)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute shouldBe Some(MyAttribute())
    }
  }

  "Source.lazyFutureSource" must {
    "work like a normal source, happy path" in {
      val result = Source.lazyFutureSource(() => Future { Source(List(1, 2, 3)) }).runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
    }

    "work like a normal source, happy path, already completed future" in {
      val result = Source.lazyFutureSource(() => Future.successful { Source(List(1, 2, 3)) }).runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
    }

    "never construct the source when there was no demand" in {
      val constructed = new AtomicBoolean(false)
      val (lazyFutureSourceMatval, termination) = Source
        .lazyFutureSource { () =>
          Future {
            constructed.set(true)
            Source(List(1, 2, 3))
          };
        }
        .watchTermination()(Keep.both)
        .toMat(Sink.cancelled)(Keep.left)
        .run()

      termination.futureValue // stream should terminate
      constructed.get() should ===(false)
      lazyFutureSourceMatval.failed.futureValue shouldBe a[NeverMaterializedException]
    }

    "fail the materialized value when downstream cancels without ever consuming any element" in {
      val lazyMatVal: Future[NotUsed] =
        Source.lazyFutureSource(() => Future { Source(List(1, 2, 3)) }).toMat(Sink.cancelled)(Keep.left).run()

      lazyMatVal.failed.futureValue shouldBe a[NeverMaterializedException]
    }

    "stop consuming when downstream has cancelled" in {
      val outProbe = TestSubscriber.probe[Int]()
      val inProbe = TestPublisher.probe[Int]()

      Source.lazyFutureSource(() => Future { Source.fromPublisher(inProbe) }).runWith(Sink.fromSubscriber(outProbe))

      outProbe.request(1)
      inProbe.expectRequest()
      inProbe.sendNext(27)
      outProbe.expectNext(27)
      outProbe.cancel()
      inProbe.expectCancellation()
    }

    "materialize when the source has been created" in {
      val probe = TestSubscriber.probe[Int]()

      val matF: Future[Done] = Source
        .lazyFutureSource { () =>
          Future {
            Source(List(1, 2, 3)).mapMaterializedValue(_ => Done)
          }
        }
        .to(Sink.fromSubscriber(probe))
        .run()

      matF.value shouldEqual None
      probe.request(1)
      probe.expectNext(1)
      matF.futureValue should ===(Done)

      probe.cancel()
    }

    "fail stage when upstream fails" in {
      val outProbe = TestSubscriber.probe[Int]()
      val inProbe = TestPublisher.probe[Int]()

      val lazyMatVal: Future[NotUsed] =
        Source
          .lazyFutureSource(() =>
            Future {
              Source.fromPublisher(inProbe)
            })
          .toMat(Sink.fromSubscriber(outProbe))(Keep.left)
          .run()

      outProbe.request(1)
      lazyMatVal.futureValue should ===(NotUsed) // but completed
      inProbe.expectRequest()
      inProbe.sendNext(27)
      outProbe.expectNext(27)
      val failure = TE("OMG Who set that on fire!?!")
      inProbe.sendError(failure)
      outProbe.expectError() should ===(failure)
    }

    "fail correctly when factory function fails" in {
      val failure = TE("couldn't create")
      val lazyMatVal: Future[NotUsed] =
        Source.lazyFutureSource[Int, NotUsed](() => throw failure).toMat(Sink.ignore)(Keep.left).run()

      lazyMatVal.failed.futureValue should ===(failure)
    }

    "fail correctly when factory function returns a failed future" in {
      val failure = TE("couldn't create")
      val lazyMatVal: Future[NotUsed] =
        Source.lazyFutureSource[Int, NotUsed](() => Future.failed(failure)).toMat(Sink.ignore)(Keep.left).run()

      lazyMatVal.failed.futureValue should ===(failure)
    }

    "fail correctly when factory function returns a future that fails" in {
      val failure = TE("couldn't create")
      val promise = Promise[Source[Int, NotUsed]]()
      val lazyMatVal: Future[NotUsed] =
        Source.lazyFutureSource(() => promise.future).toMat(Sink.ignore)(Keep.left).run()
      promise.failure(failure)
      lazyMatVal.failed.futureValue should ===(failure)
    }

    "fail correctly when materialization of inner source fails" in {
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

      val (lazyMatVal, done) =
        Source
          .lazyFutureSource(() =>
            Future {
              Source.fromGraph(FailingInnerMat)
            })
          .toMat(Sink.ignore)(Keep.both)
          .run()

      done.failed.futureValue should ===(matFail)
      lazyMatVal.failed.futureValue should ===(matFail)
    }

    "propagate downstream cancellation cause when inner source has been materialized" in {
      val probe = TestProbe()
      val (terminationF, killswitch) =
        Source
          .lazyFutureSource(() =>
            Future {
              Source.maybe[Int].watchTermination()(Keep.right).mapMaterializedValue { done =>
                probe.ref ! Done
                done
              }
            })
          .mapMaterializedValue(_.flatten)
          .viaMat(KillSwitches.single)(Keep.both)
          .to(Sink.ignore)
          .run()
      val boom = TE("boom")
      probe.expectMsg(Done)
      killswitch.abort(boom)
      terminationF.failed.futureValue should ===(boom)
    }

    "provide attributes to inner source" in {
      // This stage never stops, but that's OK, that's not what we're testing here.
      val attributes = Source
        .lazyFutureSource(() => Future(Source.fromGraph(new AttributesSource())))
        .addAttributes(myAttributes)
        .to(Sink.ignore)
        .run()

      val attribute = attributes.futureValue.get[MyAttribute]
      attribute shouldBe Some(MyAttribute())
    }
  }

}
