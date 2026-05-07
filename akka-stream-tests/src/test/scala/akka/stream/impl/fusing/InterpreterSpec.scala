/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.annotation.nowarn

import akka.stream._
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._
import akka.stream.testkit.StreamSpec
import akka.testkit.EventFilter
import akka.util.ConstantFun

class InterpreterSpec extends StreamSpec with GraphInterpreterSpecKit {

  /*
   * These tests were written for the previous version of the interpreter, the so called OneBoundedInterpreter.
   * These stages are now properly emulated by the GraphInterpreter and many of the edge cases were relevant to
   * the execution model of the old one. Still, these tests are very valuable, so please do not remove.
   */

  val takeOne = Take(1)
  val takeTwo = Take(2)

  "Interpreter" must {

    "implement map correctly" in new OneBoundedSetup[Int](Map((x: Int) => x + 1)) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(2)))

      upstream.onComplete()
      lastEvents() should be(Set(OnComplete))
    }

    "implement chain of maps correctly" in new OneBoundedSetup[Int](
      Map((x: Int) => x + 1),
      Map((x: Int) => x * 2),
      Map((x: Int) => x + 1)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(3)))

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(5)))

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))
    }

    "work with only boundary ops" in new OneBoundedSetup[Int]() {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(0)))

      upstream.onComplete()
      lastEvents() should be(Set(OnComplete))
    }

    "implement one-to-many many-to-one chain correctly" in new OneBoundedSetup[Int](
      Doubler(),
      Filter((x: Int) => x != 0)) {

      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1), RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onComplete()
      lastEvents() should be(Set(OnComplete))
    }

    "implement many-to-one one-to-many chain correctly" in new OneBoundedSetup[Int](
      Filter((x: Int) => x != 0),
      Doubler()) {

      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(1), RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))
    }

    "implement take" in new OneBoundedSetup[Int](takeTwo) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(0)))

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(1), Cancel(SubscriptionWithCancelException.StageWasCompleted), OnComplete))
    }

    "implement take inside a chain" in new OneBoundedSetup[Int](
      Filter((x: Int) => x != 0),
      takeTwo,
      Map((x: Int) => x + 1)) {

      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(2), RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(2)
      lastEvents() should be(
        Set(RequestOne, Cancel(SubscriptionWithCancelException.StageWasCompleted), OnComplete, OnNext(3)))
    }

    "implement fold" in new OneBoundedSetup[Int](Fold(0, (agg: Int, x: Int) => agg + x)) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      upstream.onComplete()
      lastEvents() should be(Set(OnNext(3), OnComplete))
    }

    "implement fold with proper cancel" in new OneBoundedSetup[Int](Fold(0, (agg: Int, x: Int) => agg + x)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))
    }

    "work if fold completes while not in a push position" in new OneBoundedSetup[Int](
      Fold(0, (agg: Int, x: Int) => agg + x)) {

      lastEvents() should be(Set.empty)

      upstream.onComplete()
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(OnComplete, OnNext(0)))
    }

    "implement grouped" in new OneBoundedSetup[Int](GroupedWeighted(3, ConstantFun.oneLong)) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(OnNext(Vector(0, 1, 2))))

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(3)
      lastEvents() should be(Set(RequestOne))

      upstream.onComplete()
      lastEvents() should be(Set(OnNext(Vector(3)), OnComplete))
    }

    "implement batch (conflate)" in new OneBoundedSetup[Int](
      Batch(1L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, x: Int) => agg + x)) {

      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(0), RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(3)))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(4)
      lastEvents() should be(Set(OnNext(4), RequestOne))

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))
    }

    "implement expand" in new OneBoundedSetup[Int](new Expand(Iterator.continually(_: Int))) {

      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne, OnNext(0)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(0)))

      upstream.onNext(1)
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne, OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1)))

      upstream.onComplete()
      lastEvents() should be(Set(OnComplete))
    }

    "work with batch-batch (conflate-conflate)" in new OneBoundedSetup[Int](
      Batch(1L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, x: Int) => agg + x),
      Batch(1L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, x: Int) => agg + x)) {

      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(0), RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(3)))

      downstream.requestOne()
      lastEvents() should be(Set.empty)

      upstream.onNext(4)
      lastEvents() should be(Set(OnNext(4), RequestOne))

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))

    }

    "work with expand-expand" in new OneBoundedSetup[Int](
      new Expand((x: Int) => Iterator.from(x)),
      new Expand((x: Int) => Iterator.from(x))) {

      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(0)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1)))

      upstream.onNext(10)
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne, OnNext(2))) // One element is still in the pipeline

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(10)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(11)))

      upstream.onComplete()
      downstream.requestOne()
      // This is correct! If you don't believe, run the interpreter with Debug on
      lastEvents() should be(Set(OnComplete, OnNext(12)))
    }

    "implement batch-expand (conflate-expand)" in new OneBoundedSetup[Int](
      Batch(1L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, x: Int) => agg + x),
      new Expand(Iterator.continually(_: Int))) {

      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(0)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(1)))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(2)))

      downstream.cancel()
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)))
    }

    "implement doubler-conflate (doubler-batch)" in new OneBoundedSetup[Int](
      Doubler(),
      Batch(1L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, x: Int) => agg + x)) {
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(RequestOne))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(6)))

    }

    // Note, the new interpreter has no jumpback table, still did not want to remove the test
    "work with jumpback table and completed elements" in new OneBoundedSetup[Int](
      Map((x: Int) => x),
      Map((x: Int) => x),
      KeepGoing(),
      Map((x: Int) => x),
      Map((x: Int) => x)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(1)))

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(2)
      lastEvents() should be(Set(OnNext(2)))

      upstream.onComplete()
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(2)))

      downstream.requestOne()
      lastEvents() should be(Set(OnNext(2)))

    }

    "work with pushAndFinish if upstream completes with pushAndFinish" in new OneBoundedSetup[Int](new PushFinishStage) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete(0)
      lastEvents() should be(Set(OnNext(0), OnComplete))
    }

    "work with pushAndFinish if indirect upstream completes with pushAndFinish" in new OneBoundedSetup[Int](
      Map((x: Any) => x),
      new PushFinishStage,
      Map((x: Any) => x)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete(1)
      lastEvents() should be(Set(OnNext(1), OnComplete))
    }

    "work with pushAndFinish if upstream completes with pushAndFinish and downstream immediately pulls" in new OneBoundedSetup[
      Int](new PushFinishStage, Fold(0, (x: Int, y: Int) => x + y)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete(1)
      lastEvents() should be(Set(OnNext(1), OnComplete))
    }

    "report error if pull is called while op is terminating" in new OneBoundedSetup[Int](
      new SimpleLinearGraphStage[Any] {
        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
          new GraphStageLogic(shape) with InHandler with OutHandler {
            override def onPush(): Unit = pull(in)
            override def onPull(): Unit = pull(in)
            override def onUpstreamFinish(): Unit = if (!hasBeenPulled(in)) pull(in)

            setHandlers(in, out, this)
          }
      }) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      EventFilter[IllegalArgumentException](pattern = ".*Cannot pull closed port.*", occurrences = 1).intercept {
        upstream.onComplete()
      }
      val ev = lastEvents()
      ev.nonEmpty should be(true)
      ev.forall {
        case OnError(_: IllegalArgumentException) => true
        case _                                    => false
      } should be(true)
    }

    "implement take-take" in new OneBoundedSetup[Int](takeOne, takeOne) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(1)
      lastEvents() should be(Set(OnNext(1), OnComplete, Cancel(SubscriptionWithCancelException.StageWasCompleted)))

    }

    "implement take-take with pushAndFinish from upstream" in new OneBoundedSetup[Int](takeOne, takeOne) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete(1)
      lastEvents() should be(Set(OnNext(1), OnComplete))
    }

  }

  @nowarn
  private[akka] final case class Doubler[T]() extends SimpleLinearGraphStage[T] {

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        var latest: T = _
        var oneMore = false

        override def onPush(): Unit = {
          latest = grab(in)
          oneMore = true
          push(out, latest)
        }

        /**
         * Called when the output port has received a pull, and therefore ready to emit an element, i.e. [[GraphStageLogic.push]]
         * is now allowed to be called on this port.
         */
        override def onPull(): Unit = {
          if (oneMore) {
            push(out, latest)
            oneMore = false
          } else {
            pull(in)
          }
        }

        setHandlers(in, out, this)
      }

  }

  @nowarn
  private[akka] final case class KeepGoing[T]() extends SimpleLinearGraphStage[T] {

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        var lastElem: T = _

        override def onPush(): Unit = {
          lastElem = grab(in)
          push(out, lastElem)
        }

        // note that the default value of lastElem will be always pushed if the upstream closed at the very beginning without a pulling
        override def onPull(): Unit = {
          if (isClosed(in)) {
            push(out, lastElem)
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {}

        setHandlers(in, out, this)
      }

  }

  private[akka] class PushFinishStage(onPostStop: () => Unit = () => ()) extends SimpleLinearGraphStage[Any] {
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onPush(): Unit = {
          push(out, grab(in))
          completeStage()
        }
        override def onPull(): Unit = pull(in)
        override def onUpstreamFinish(): Unit = failStage(akka.stream.testkit.Utils.TE("Cannot happen"))
        override def postStop(): Unit = onPostStop()

        setHandlers(in, out, this)
      }
  }

}
