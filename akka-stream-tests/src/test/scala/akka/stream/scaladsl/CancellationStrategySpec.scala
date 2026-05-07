/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.Attributes.CancellationStrategy
import akka.stream.Attributes.CancellationStrategy.FailStage
import akka.stream.BidiShape
import akka.stream.ClosedShape
import akka.stream.Inlet
import akka.stream.Materializer
import akka.stream.Outlet
import akka.stream.SharedKillSwitch
import akka.stream.SubscriptionWithCancelException
import akka.stream.UniformFanOutShape
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils.TE
import akka.testkit._
import akka.testkit.WithLogCapturing

class CancellationStrategySpec extends StreamSpec("""akka.loglevel = DEBUG
       akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]""") with WithLogCapturing {
  "CancellationStrategyAttribute" should {
    "support strategies" should {
      "CompleteStage" should {
        "complete if no failure cancellation" in new TestSetup(CancellationStrategy.CompleteStage) {
          out1Probe.cancel()
          inProbe.expectCancellation()
          out2Probe.expectComplete()
        }
        "complete and propagate cause if failure cancellation" in new TestSetup(CancellationStrategy.CompleteStage) {
          val theError = TE("This is a TestException")
          out1Probe.cancel(theError)
          inProbe.expectCancellationWithCause(theError)
          out2Probe.expectComplete()
        }
      }
      "FailStage" should {
        "fail if no failure cancellation" in new TestSetup(CancellationStrategy.FailStage) {
          out1Probe.cancel()
          inProbe.expectCancellationWithCause(SubscriptionWithCancelException.NoMoreElementsNeeded)
          out2Probe.expectError(SubscriptionWithCancelException.NoMoreElementsNeeded)
        }
        "fail if failure cancellation" in new TestSetup(CancellationStrategy.FailStage) {
          val theError = TE("This is a TestException")
          out1Probe.cancel(theError)
          inProbe.expectCancellationWithCause(theError)
          out2Probe.expectError(theError)
        }
      }
      "PropagateFailure" should {
        "complete if no failure" in new TestSetup(CancellationStrategy.PropagateFailure) {
          out1Probe.cancel()
          inProbe.expectCancellationWithCause(SubscriptionWithCancelException.NoMoreElementsNeeded)
          out2Probe.expectComplete()
        }
        "propagate failure" in new TestSetup(CancellationStrategy.PropagateFailure) {
          val theError = TE("This is a TestException")
          out1Probe.cancel(theError)
          inProbe.expectCancellationWithCause(theError)
          out2Probe.expectError(theError)
        }
      }
      "AfterDelay" should {
        "apply given strategy after delay" in new TestSetup(CancellationStrategy.AfterDelay(500.millis, FailStage)) {
          out1Probe.cancel()
          inProbe.expectNoMessage(200.millis)
          out2Probe.expectNoMessage(200.millis)

          inProbe.expectCancellationWithCause(SubscriptionWithCancelException.NoMoreElementsNeeded)
          out2Probe.expectError(SubscriptionWithCancelException.NoMoreElementsNeeded)
        }
        "prevent further elements from coming through" in new TestSetup(
          CancellationStrategy.AfterDelay(500.millis, FailStage)) {
          out1Probe.request(1)
          out2Probe.request(1)
          out1Probe.cancel()
          inProbe.sendNext(B(123))
          inProbe.expectNoMessage(200.millis) // cancellation should not have propagated yet
          out2Probe.expectNext(B(123)) // so the element still goes to out2
          out1Probe.expectNoMessage(200.millis) // but not to out1 which has already cancelled

          // after delay cancellation and error should have propagated
          inProbe.expectCancellationWithCause(SubscriptionWithCancelException.NoMoreElementsNeeded)
          out2Probe.expectError(SubscriptionWithCancelException.NoMoreElementsNeeded)
        }
      }
    }

    "cancellation races with BidiStacks" should {
      "accidentally convert errors to completions when CompleteStage strategy is chosen (2.5 default)" in new RaceTestSetup(
        CancellationStrategy.CompleteStage) {
        val theError = TE("Duck meowed")
        killSwitch.abort(theError)
        toStream.expectCancellationWithCause(theError)

        // this asserts the previous broken behavior (which can still be seen with CompleteStage strategy)
        fromStream.expectComplete()
      }
      "be prevented by PropagateFailure strategy (default in 2.6)" in new RaceTestSetup(
        CancellationStrategy.PropagateFailure) {
        val theError = TE("Duck meowed")
        killSwitch.abort(theError)
        toStream.expectCancellationWithCause(theError)
        fromStream.expectError(theError)
      }
      "be prevented by AfterDelay strategy" in new RaceTestSetup(
        CancellationStrategy.AfterDelay(500.millis.dilated, CancellationStrategy.CompleteStage)) {
        val theError = TE("Duck meowed")
        killSwitch.abort(theError)
        toStream.expectCancellationWithCause(theError)
        fromStream.expectError(theError)
      }

      class RaceTestSetup(cancellationStrategy: CancellationStrategy.Strategy) {
        val toStream = TestPublisher.probe[A]()
        val fromStream = TestSubscriber.probe[B]()

        val bidi: BidiFlow[A, A, B, B, NotUsed] = BidiFlow.fromGraph(new NaiveBidiStage)

        val killSwitch = new SharedKillSwitch("test")
        def errorPropagationDelay: FiniteDuration = 200.millis.dilated

        Source
          .fromPublisher(toStream)
          .via(
            bidi
              .atop(BidiFlow.fromFlows(
                new DelayCompletionSignal[A](errorPropagationDelay),
                new DelayCompletionSignal[B](errorPropagationDelay)))
              .join(Flow[A].via(killSwitch.flow).map(_.toB)))
          .to(Sink.fromSubscriber(fromStream))
          .addAttributes(Attributes(CancellationStrategy(cancellationStrategy))) // fails for `CompleteStage`
          .run()

        fromStream.request(1)
        toStream.sendNext(A("125"))
        fromStream.expectNext(B(125))
      }
    }
  }

  case class A(str: String) {
    def toB: B = B(str.toInt)
  }
  case class B(i: Int)

  class TestSetup(cancellationStrategy: Option[CancellationStrategy.Strategy]) {
    def this(strategy: CancellationStrategy.Strategy) = this(Some(strategy))

    val inProbe = TestPublisher.probe[B]()
    val out1Probe = TestSubscriber.probe[B]()
    val out2Probe = TestSubscriber.probe[B]()

    def materializer: Materializer = Materializer.matFromSystem(system)

    RunnableGraph
      .fromGraph {
        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val fanOut = b.add(new TestFanOut)

          Source.fromPublisher(inProbe) ~> fanOut.in
          fanOut.out(0) ~> Sink.fromSubscriber(out1Probe)
          fanOut.out(1) ~> Sink.fromSubscriber(out2Probe)

          ClosedShape
        }
      }
      .addAttributes(Attributes(cancellationStrategy.toList.map(CancellationStrategy(_))))
      .run()(materializer)

    // some basic testing that data flow
    out1Probe.request(1)
    out2Probe.request(1)

    inProbe.expectRequest()
    inProbe.sendNext(B(42))
    out1Probe.expectNext(B(42))
    out2Probe.expectNext(B(42))
  }

  // a simple broadcast stage
  class TestFanOut extends GraphStage[UniformFanOutShape[B, B]] {
    val in = Inlet[B]("in")
    val out1 = Outlet[B]("out1")
    val out2 = Outlet[B]("out2")

    val shape = UniformFanOutShape(in, out1, out2)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
        setHandler(in, this)
        setHandler(out1, this)
        setHandler(out2, this)

        var waitingForPulls = 2
        override def onPush(): Unit = {
          val el = grab(in)
          push(out1, el)
          push(out2, el)
          waitingForPulls = 2
        }

        override def onPull(): Unit = {
          waitingForPulls -= 1
          require(waitingForPulls >= 0)
          if (waitingForPulls == 0)
            pull(in)
        }
      }
  }
  class NaiveBidiStage extends GraphStage[BidiShape[A, A, B, B]] {
    val upIn = Inlet[A]("upIn")
    val upOut = Outlet[A]("upOut")

    val downIn = Inlet[B]("downIn")
    val downOut = Outlet[B]("downOut")

    val shape = BidiShape(upIn, upOut, downIn, downOut)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        def connect[T](in: Inlet[T], out: Outlet[T]): Unit = {
          val handler = new InHandler with OutHandler {
            override def onPull(): Unit = pull(in)
            override def onPush(): Unit = push(out, grab(in))
          }
          setHandlers(in, out, handler)
        }
        connect(upIn, upOut)
        connect(downIn, downOut)
      }
  }

  /** A simple stage that delays completion signals */
  class DelayCompletionSignal[T](delay: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
        setHandlers(in, out, this)

        override def onPull(): Unit = pull(in)
        override def onPush(): Unit = push(out, grab(in))

        val callback = getAsyncCallback[Option[Throwable]] { signal =>
          log.debug(s"Now executing delayed action $signal")
          signal match {
            case Some(ex) => failStage(ex)
            case None     => completeStage()
          }
        }

        override def onUpstreamFinish(): Unit = {
          log.debug(s"delaying completion")
          materializer.scheduleOnce(delay, () => callback.invoke(None))
        }
        override def onUpstreamFailure(ex: Throwable): Unit = {
          log.debug(s"delaying error $ex")
          materializer.scheduleOnce(delay, () => callback.invoke(Some(ex)))
        }
      }
  }
}
