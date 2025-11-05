/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.{ Done, NotUsed }
import akka.stream.{
  AbruptStageTerminationException,
  AbruptTerminationException,
  Attributes,
  FlowShape,
  Inlet,
  Materializer,
  NeverMaterializedException,
  Outlet,
  SubscriptionWithCancelException
}
import akka.stream.Attributes.Attribute
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.testkit.{ StreamSpec, TestPublisher, TestSubscriber }
import akka.stream.testkit.Utils.TE

// Debug loglevel to diagnose https://github.com/akka/akka/issues/30469
class FlowFlatMapPrefixSpec extends StreamSpec("akka.loglevel = debug") {
  def src10(i: Int = 0) = Source(i until (i + 10))

  for {
    att <- List(
      Attributes.NestedMaterializationCancellationPolicy.EagerCancellation,
      Attributes.NestedMaterializationCancellationPolicy.PropagateToNested)
    delayDownstreamCancellation = att.propagateToNestedMaterialization
    attributes = Attributes(att)
  } {

    s"A PrefixAndDownstream with $att" must {

      "work in the simple identity case" in {
        src10()
          .flatMapPrefixMat(2) { _ =>
            Flow[Int]
          }(Keep.left)
          .withAttributes(attributes)
          .runWith(Sink.seq[Int])
          .futureValue should ===(2 until 10)
      }

      "expose mat value in the simple identity case" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(2) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 2)
        suffixF.futureValue should ===(2 until 10)
      }

      "work when source is exactly the required prefix" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(10) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 10)
        suffixF.futureValue should be(empty)
      }

      "work when source has less than the required prefix" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(20) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 10)
        suffixF.futureValue should be(empty)
      }

      "simple identity case when downstream completes before consuming the entire stream" in {
        val (prefixF, suffixF) = Source(0 until 100)
          .flatMapPrefixMat(10) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .take(10)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 10)
        suffixF.futureValue should ===(10 until 20)
      }

      "propagate failure to create the downstream flow" in {
        val suffixF = Source(0 until 100)
          .flatMapPrefixMat(10) { prefix =>
            throw TE(s"I hate mondays! (${prefix.size})")
          }(Keep.right)
          .to(Sink.ignore)
          .withAttributes(attributes)
          .run()

        val ex = suffixF.failed.futureValue
        ex.getCause should not be null
        ex.getCause should ===(TE("I hate mondays! (10)"))
      }

      "propagate flow failures" in {
        val (prefixF, suffixF) = Source(0 until 100)
          .flatMapPrefixMat(10) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).map {
              case 15 => throw TE("don't like 15 either!")
              case n  => n
            }
          }(Keep.right)
          .toMat(Sink.ignore)(Keep.both)
          .withAttributes(attributes)
          .run()
        prefixF.futureValue should ===(0 until 10)
        val ex = suffixF.failed.futureValue
        ex should ===(TE("don't like 15 either!"))
      }

      "produce multiple elements per input" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(7) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).mapConcat(n => List.fill(n - 6)(n))
          }(Keep.right)
          .toMat(Sink.seq[Int])(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 7)
        suffixF.futureValue should ===(7 :: 8 :: 8 :: 9 :: 9 :: 9 :: Nil)
      }

      "succeed when upstream produces no elements" in {
        val (prefixF, suffixF) = Source
          .empty[Int]
          .flatMapPrefixMat(7) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).mapConcat(n => List.fill(n - 6)(n))
          }(Keep.right)
          .toMat(Sink.seq[Int])(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should be(empty)
        suffixF.futureValue should be(empty)
      }

      "apply materialized flow's semantics when upstream produces no elements" in {
        val (prefixF, suffixF) = Source
          .empty[Int]
          .flatMapPrefixMat(7) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).mapConcat(n => List.fill(n - 6)(n)).prepend(Source(100 to 101))
          }(Keep.right)
          .toMat(Sink.seq[Int])(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should be(empty)
        suffixF.futureValue should ===(100 :: 101 :: Nil)
      }

      "handles upstream completion" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[Int]()

        val matValue = Source
          .fromPublisher(publisher)
          .flatMapPrefixMat(2) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).prepend(Source(100 to 101))
          }(Keep.right)
          .to(Sink.fromSubscriber(subscriber))
          .withAttributes(attributes)
          .run()

        matValue.value should be(empty)

        val upstream = publisher.expectSubscription()
        val downstream = subscriber.expectSubscription()

        downstream.request(1000)

        upstream.expectRequest()
        //completing publisher
        upstream.sendComplete()

        matValue.futureValue should ===(Nil)

        subscriber.expectNext(100)

        subscriber.expectNext(101).expectComplete()

      }

      "work when materialized flow produces no downstream elements" in {
        val (prefixF, suffixF) = Source(0 until 100)
          .flatMapPrefixMat(4) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).filter(_ => false)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 4)
        suffixF.futureValue should be(empty)
      }

      "work when materialized flow does not consume upstream" in {
        val (prefixF, suffixF) = Source(0 until 100)
          .map { i =>
            i should be <= 4
            i
          }
          .flatMapPrefixMat(4) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).take(0)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 4)
        suffixF.futureValue should be(empty)
      }

      "work when materialized flow cancels upstream but keep producing" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(4) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix).take(0).concat(Source(11 to 12))
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should ===(0 until 4)
        suffixF.futureValue should ===(11 :: 12 :: Nil)
      }

      "propagate materialization failure (when application of 'f' succeeds)" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(4) { prefix =>
            Flow[Int].mapMaterializedValue(_ => throw TE(s"boom-bada-bang (${prefix.size})"))
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.failed.futureValue should be(a[NeverMaterializedException])
        prefixF.failed.futureValue.getCause should ===(TE("boom-bada-bang (4)"))
        suffixF.failed.futureValue should ===(TE("boom-bada-bang (4)"))
      }

      "succeed when materialized flow completes downstream but keep consuming elements" in {
        val (prefixAndTailF, suffixF) = src10()
          .flatMapPrefixMat(4) { prefix =>
            Flow[Int]
              .mapMaterializedValue(_ => prefix)
              .viaMat {
                Flow.fromSinkAndSourceMat(Sink.seq[Int], Source.empty[Int])(Keep.left)
              }(Keep.both)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        suffixF.futureValue should be(empty)
        val (prefix, suffix) = prefixAndTailF.futureValue
        prefix should ===(0 until 4)
        suffix.futureValue should ===(4 until 10)
      }

      "propagate downstream cancellation via the materialized flow" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[Int]()

        val ((srcWatchTermF, innerMatVal), sinkMatVal) = src10()
          .watchTermination()(Keep.right)
          .flatMapPrefixMat(2) { prefix =>
            prefix should ===(0 until 2)
            Flow.fromSinkAndSource(Sink.fromSubscriber(subscriber), Source.fromPublisher(publisher))
          }(Keep.both)
          .take(1)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        val subUpstream = publisher.expectSubscription()
        val subDownstream = subscriber.expectSubscription()

        // inner stream was materialized
        innerMatVal.futureValue should ===(NotUsed)

        subUpstream.expectRequest() should be >= (1L)
        subDownstream.request(1)
        subscriber.expectNext(2)
        subUpstream.sendNext(22)
        subUpstream.expectCancellation() // because take(1)
        // this should not automatically pass the cancellation upstream of nested flow
        srcWatchTermF.isCompleted should ===(false)
        sinkMatVal.futureValue should ===(Seq(22))

        // the nested flow then decides to cancel, which moves upstream
        subDownstream.cancel()
        srcWatchTermF.futureValue should ===(Done)
      }

      "early downstream cancellation is later handed out to materialized flow" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[Int]()

        val (srcWatchTermF, matFlowWatchTermFF) = Source
          .fromPublisher(publisher)
          .watchTermination()(Keep.right)
          .flatMapPrefixMat(3) { prefix =>
            prefix should ===(0 until 3)
            Flow[Int].watchTermination()(Keep.right)
          }(Keep.both)
          .to(Sink.fromSubscriber(subscriber))
          .withAttributes(attributes)
          .run()
        val matFlowWatchTerm = matFlowWatchTermFF.flatten

        matFlowWatchTerm.value should be(empty)
        srcWatchTermF.value should be(empty)

        val subDownstream = subscriber.expectSubscription()
        val subUpstream = publisher.expectSubscription()
        subDownstream.request(1)
        subUpstream.expectRequest() should be >= (1L)
        subUpstream.sendNext(0)
        subUpstream.sendNext(1)

        //subflow not materialized yet, hence mat value (future) isn't ready yet
        matFlowWatchTerm.value should be(empty)

        if (delayDownstreamCancellation) {
          srcWatchTermF.value should be(empty)
          //this one is sent AFTER downstream cancellation
          subUpstream.sendNext(2)

          subDownstream.cancel()
          subUpstream.expectCancellation()

          matFlowWatchTerm.futureValue should ===(Done)
          srcWatchTermF.futureValue should ===(Done)
        } else {
          subDownstream.cancel()
          srcWatchTermF.futureValue should ===(Done)
          matFlowWatchTerm.failed.futureValue should be(a[NeverMaterializedException])
        }
      }

      "early downstream failure is deferred until prefix completion" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[Int]()

        val (srcWatchTermF, matFlowWatchTermFF) = Source
          .fromPublisher(publisher)
          .watchTermination()(Keep.right)
          .flatMapPrefixMat(3) { prefix =>
            prefix should ===(0 until 3)
            Flow[Int].watchTermination()(Keep.right)
          }(Keep.both)
          .to(Sink.fromSubscriber(subscriber))
          .withAttributes(attributes)
          .run()
        val matFlowWatchTerm = matFlowWatchTermFF.flatten

        matFlowWatchTerm.value should be(empty)
        srcWatchTermF.value should be(empty)

        val subDownstream = subscriber.expectSubscription()
        val subUpstream = publisher.expectSubscription()
        subDownstream.request(1)
        subUpstream.expectRequest() should be >= (1L)
        subUpstream.sendNext(0)
        subUpstream.sendNext(1)
        subDownstream.asInstanceOf[SubscriptionWithCancelException].cancel(TE("that again?!"))

        if (delayDownstreamCancellation) {
          matFlowWatchTerm.value should be(empty)
          srcWatchTermF.value should be(empty)

          subUpstream.sendNext(2)

          matFlowWatchTerm.failed.futureValue should ===(TE("that again?!"))
          srcWatchTermF.failed.futureValue should ===(TE("that again?!"))

          subUpstream.expectCancellation()
        } else {
          subUpstream.expectCancellation()
          srcWatchTermF.failed.futureValue should ===(TE("that again?!"))
          matFlowWatchTerm.failed.futureValue should be(a[NeverMaterializedException])
          matFlowWatchTerm.failed.futureValue.getCause should ===(TE("that again?!"))
        }
      }

      "downstream failure is propagated via the materialized flow" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[Int]()

        val ((srcWatchTermF, notUsedF), suffixF) = src10()
          .watchTermination()(Keep.right)
          .flatMapPrefixMat(2) { prefix =>
            prefix should ===(0 until 2)
            Flow.fromSinkAndSourceCoupled(Sink.fromSubscriber(subscriber), Source.fromPublisher(publisher))
          }(Keep.both)
          .map {
            case 2 => 2
            case 3 => throw TE("3!?!?!?")
            case i => fail(s"unexpected value $i")
          }
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        suffixF.value should be(empty)
        srcWatchTermF.value should be(empty)

        val subUpstream = publisher.expectSubscription()
        val subDownstream = subscriber.expectSubscription()

        notUsedF.futureValue should ===(NotUsed)

        subUpstream.expectRequest() should be >= (1L)
        subDownstream.request(1)
        subscriber.expectNext(2)
        subUpstream.sendNext(2)
        subDownstream.request(1)
        subscriber.expectNext(3)
        subUpstream.sendNext(3)
        subUpstream.expectCancellation() should ===(TE("3!?!?!?"))
        subscriber.expectError(TE("3!?!?!?"))

        suffixF.failed.futureValue should ===(TE("3!?!?!?"))
        srcWatchTermF.failed.futureValue should ===(TE("3!?!?!?"))
      }

      "complete mat value with failures on abrupt termination before materializing the flow" in {
        val mat = Materializer(system)
        val publisher = TestPublisher.manualProbe[Int]()

        val flow = Source
          .fromPublisher(publisher)
          .flatMapPrefixMat(2) { prefix =>
            fail(s"unexpected prefix (length = ${prefix.size})")
          }(Keep.right)
          .toMat(Sink.ignore)(Keep.both)
          .withAttributes(attributes)

        val (prefixF, doneF) = flow.run()(mat)

        publisher.expectSubscription()
        prefixF.value should be(empty)
        doneF.value should be(empty)

        mat.shutdown()

        prefixF.failed.futureValue match {
          case _: AbruptTerminationException =>
          case ex: NeverMaterializedException =>
            ex.getCause should not be null
            ex.getCause should be(a[AbruptTerminationException])
          case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
        }
        doneF.failed.futureValue should be(a[AbruptTerminationException])
      }

      "respond to abrupt termination after flow materialization" in {
        val mat = Materializer(system)
        val countFF = src10()
          .flatMapPrefixMat(2) { prefix =>
            prefix should ===(0 until 2)
            Flow[Int]
              .concat(Source.repeat(3))
              .fold(0L) {
                case (acc, _) => acc + 1
              }
              .alsoToMat(Sink.head)(Keep.right)
          }(Keep.right)
          .to(Sink.ignore)
          .withAttributes(attributes)
          .run()(mat)
        val countF = countFF.futureValue
        //at this point we know the flow was materialized, now we can stop the materializer
        mat.shutdown()
        //expect the nested flow to be terminated abruptly.
        countF.failed.futureValue should be(a[AbruptStageTerminationException])
      }

      "behave like via when n = 0" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(0) { prefix =>
            prefix should be(empty)
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should be(empty)
        suffixF.futureValue should ===(0 until 10)
      }

      "behave like via when n = 0 and upstream produces no elements" in {
        val (prefixF, suffixF) = Source
          .empty[Int]
          .flatMapPrefixMat(0) { prefix =>
            prefix should be(empty)
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.futureValue should be(empty)
        suffixF.futureValue should be(empty)
      }

      "propagate errors during flow's creation when n = 0" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(0) { prefix =>
            prefix should be(empty)
            throw TE("not this time my friend!")
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.failed.futureValue should be(a[NeverMaterializedException])
        prefixF.failed.futureValue.getCause === (TE("not this time my friend!"))
        suffixF.failed.futureValue should ===(TE("not this time my friend!"))
      }

      "propagate materialization failures when n = 0" in {
        val (prefixF, suffixF) = src10()
          .flatMapPrefixMat(0) { prefix =>
            prefix should be(empty)
            Flow[Int].mapMaterializedValue(_ => throw TE("Bang! no materialization this time"))
          }(Keep.right)
          .toMat(Sink.seq)(Keep.both)
          .withAttributes(attributes)
          .run()

        prefixF.failed.futureValue should be(a[NeverMaterializedException])
        prefixF.failed.futureValue.getCause === (TE("Bang! no materialization this time"))
        suffixF.failed.futureValue should ===(TE("Bang! no materialization this time"))
      }

      "run a detached flow" in {
        val publisher = TestPublisher.manualProbe[Int]()
        val subscriber = TestSubscriber.manualProbe[String]()

        val detachedFlow = Flow.fromSinkAndSource(Sink.cancelled[Int], Source(List("a", "b", "c"))).via {
          Flow.fromSinkAndSource(Sink.fromSubscriber(subscriber), Source.empty[Int])
        }
        val fHeadOpt = Source
          .fromPublisher(publisher)
          .flatMapPrefix(2) { prefix =>
            prefix should ===(0 until 2)
            detachedFlow
          }
          .withAttributes(attributes)
          .runWith(Sink.headOption)

        subscriber.expectNoMessage()
        val subsc = publisher.expectSubscription()
        subsc.expectRequest() should be >= 2L
        subsc.sendNext(0)
        subscriber.expectNoMessage()
        subsc.sendNext(1)
        val sinkSubscription = subscriber.expectSubscription()
        //this indicates
        fHeadOpt.futureValue should be(empty)

        //materialize flow immediately cancels upstream
        subsc.expectCancellation()
        //at this point both ends of the 'external' fow are closed

        sinkSubscription.request(10)
        subscriber.expectNext("a", "b", "c")
        subscriber.expectComplete()
      }

      "complete newShells registration when all active interpreters are done" in {
        @volatile var closeSink: () => Unit = null

        val (fNotUsed, qOut) = Source
          .empty[Int]
          .flatMapPrefixMat(1) { seq =>
            log.debug("waiting for closer to be set")
            while (null == closeSink) Thread.sleep(50)
            log.debug("closing sink")
            closeSink()
            log.debug("sink closed")
            //closing the sink before returning means that it's higly probably
            //for the flatMapPrefix stage to receive the downstream cancellation before the actor graph interpreter
            //gets a chance to complete the new interpreter shell's registration.
            //this in turn exposes a bug in the actor graph interpreter when all active flows complete
            //but there are pending new interpreter shells to be registered.
            Flow[Int].prepend(Source(seq))
          }(Keep.right)
          .toMat(Sink.queue(10))(Keep.both)
          .run()

        log.debug("assigning closer")
        closeSink = () => qOut.cancel()

        log.debug("closer assigned, waiting for completion")
        fNotUsed.futureValue should be(NotUsed)
      }

      "complete when downstream cancels before pulling" in {
        val fSeq = Source
          .single(1)
          .flatMapPrefixMat(1) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .to(Sink.cancelled)
          .withAttributes(attributes)
          .run()

        if (att.propagateToNestedMaterialization) {
          fSeq.futureValue should equal(Seq(1))
        } else {
          fSeq.failed.futureValue should be(a[NeverMaterializedException])
        }
      }

      "complete when downstream cancels before pulling, prefix=0" in {
        val fSeq = Source
          .single(1)
          .flatMapPrefixMat(0) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .to(Sink.cancelled)
          .withAttributes(attributes)
          .run()

        if (att.propagateToNestedMaterialization) {
          fSeq.futureValue should equal(Nil)
        } else {
          fSeq.failed.futureValue should be(a[NeverMaterializedException])
        }
      }

      "complete when downstream cancels before pulling and upstream does not produce" in {
        val fSeq = Source(List.empty[Int])
          .flatMapPrefixMat(1) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .to(Sink.cancelled)
          .withAttributes(attributes)
          .run()

        if (att.propagateToNestedMaterialization) {
          fSeq.futureValue should equal(Nil)
        } else {
          fSeq.failed.futureValue should be(a[NeverMaterializedException])
        }
      }

      "complete when downstream cancels before pulling and upstream does not produce, prefix=0" in {
        val fSeq = Source(List.empty[Int])
          .flatMapPrefixMat(0) { prefix =>
            Flow[Int].mapMaterializedValue(_ => prefix)
          }(Keep.right)
          .to(Sink.cancelled)
          .withAttributes(attributes)
          .run()

        if (att.propagateToNestedMaterialization) {
          fSeq.futureValue should equal(Nil)
        } else {
          fSeq.failed.futureValue should be(a[NeverMaterializedException])
        }
      }
    }
  }

  "attributes propagation" must {
    case class CustomAttribute(n: Int) extends Attribute
    class WithAttr[A] extends GraphStage[FlowShape[A, (A, Option[CustomAttribute])]] {
      override val shape: FlowShape[A, (A, Option[CustomAttribute])] =
        FlowShape(Inlet[A]("in"), Outlet[(A, Option[CustomAttribute])]("out"))
      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with InHandler with OutHandler {
          val attr = inheritedAttributes.get[CustomAttribute]

          setHandlers(shape.in, shape.out, this)

          override def onPush(): Unit = push(shape.out, grab(shape.in) -> attr)

          override def onPull(): Unit = pull(shape.in)
        }
    }
    def withAttr[A] = Flow.fromGraph(new WithAttr[A])
    "baseline behaviour" in {
      Source
        .single("1")
        .via(withAttr)
        .map(_._2)
        .withAttributes(Attributes(CustomAttribute(42)))
        .runWith(Sink.head)
        .futureValue should be(Some(CustomAttribute(42)))
    }

    "propagate attribute applied to flatMapPrefix" in {
      Source
        .single("1")
        .flatMapPrefix(0) { _ =>
          Flow[String].via(withAttr).map(_._2)
        }
        .withAttributes(Attributes(CustomAttribute(42)))
        .runWith(Sink.head)
        .futureValue should be(Some(CustomAttribute(42)))
    }

    "respect attributes override" in {
      Source
        .single("1")
        .flatMapPrefix(0) { _ =>
          Flow[String].via(withAttr).map(_._2).withAttributes(Attributes(CustomAttribute(24)))
        }
        .withAttributes(Attributes(CustomAttribute(42)))
        .runWith(Sink.head)
        .futureValue should be(Some(CustomAttribute(24)))
    }
  }
}
