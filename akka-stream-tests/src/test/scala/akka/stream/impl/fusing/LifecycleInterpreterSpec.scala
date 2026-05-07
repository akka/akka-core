/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.concurrent.duration._

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE

class LifecycleInterpreterSpec extends StreamSpec with GraphInterpreterSpecKit {

  val boom = TE("Boom!")

  "Interpreter" must {

    "call preStart in order on stages" in new OneBoundedSetup[String](
      PreStartAndPostStopIdentity(onStart = () => testActor ! "start-a"),
      PreStartAndPostStopIdentity(onStart = () => testActor ! "start-b"),
      PreStartAndPostStopIdentity(onStart = () => testActor ! "start-c")) {
      expectMsg("start-a")
      expectMsg("start-b")
      expectMsg("start-c")
      expectNoMessage(300.millis)
      upstream.onComplete()
    }

    "call postStop in order on stages - when upstream completes" in new OneBoundedSetup[String](
      PreStartAndPostStopIdentity(
        onUpstreamCompleted = () => testActor ! "complete-a",
        onStop = () => testActor ! "stop-a"),
      PreStartAndPostStopIdentity(
        onUpstreamCompleted = () => testActor ! "complete-b",
        onStop = () => testActor ! "stop-b"),
      PreStartAndPostStopIdentity(
        onUpstreamCompleted = () => testActor ! "complete-c",
        onStop = () => testActor ! "stop-c")) {
      upstream.onComplete()
      expectMsg("complete-a")
      expectMsg("stop-a")
      expectMsg("complete-b")
      expectMsg("stop-b")
      expectMsg("complete-c")
      expectMsg("stop-c")
      expectNoMessage(300.millis)
    }

    "call postStop in order on stages - when upstream onErrors" in new OneBoundedSetup[String](
      PreStartAndPostStopIdentity(
        onUpstreamFailed = ex => testActor ! ex.getMessage,
        onStop = () => testActor ! "stop-c")) {
      val msg = "Boom! Boom! Boom!"
      upstream.onError(TE(msg))
      expectMsg(msg)
      expectMsg("stop-c")
      expectNoMessage(300.millis)
    }

    "call postStop in order on stages - when downstream cancels" in new OneBoundedSetup[String](
      PreStartAndPostStopIdentity(onStop = () => testActor ! "stop-a"),
      PreStartAndPostStopIdentity(onStop = () => testActor ! "stop-b"),
      PreStartAndPostStopIdentity(onStop = () => testActor ! "stop-c")) {
      downstream.cancel()
      expectMsg("stop-c")
      expectMsg("stop-b")
      expectMsg("stop-a")
      expectNoMessage(300.millis)
    }

    "call preStart before postStop" in new OneBoundedSetup[String](
      PreStartAndPostStopIdentity(onStart = () => testActor ! "start-a", onStop = () => testActor ! "stop-a")) {
      expectMsg("start-a")
      expectNoMessage(300.millis)
      upstream.onComplete()
      expectMsg("stop-a")
      expectNoMessage(300.millis)
    }

    "onError when preStart fails" in new OneBoundedSetup[String](PreStartFailer(() => throw boom)) {
      lastEvents() should ===(Set(Cancel(boom), OnError(boom)))
    }

    "not blow up when postStop fails" in new OneBoundedSetup[String](PostStopFailer(() => throw TE("Boom!"))) {
      upstream.onComplete()
      lastEvents() should ===(Set(OnComplete))
    }

    "onError when preStart fails with stages after" in new OneBoundedSetup[String](
      Map((x: Int) => x),
      PreStartFailer(() => throw boom),
      Map((x: Int) => x)) {
      lastEvents() should ===(Set(Cancel(boom), OnError(boom)))
    }

    "continue with stream shutdown when postStop fails" in new OneBoundedSetup[String](PostStopFailer(() =>
      throw TE("Boom!"))) {
      lastEvents() should ===(Set.empty[TestEvent])

      upstream.onComplete()
      lastEvents() should ===(Set(OnComplete))
    }

    "postStop when pushAndFinish called if upstream completes with pushAndFinish" in new OneBoundedSetup[String](
      new PushFinishStage(onPostStop = () => testActor ! "stop")) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete("foo")
      lastEvents() should be(Set(OnNext("foo"), OnComplete))
      expectMsg("stop")
    }

    "postStop when pushAndFinish called with pushAndFinish if indirect upstream completes with pushAndFinish" in new OneBoundedSetup[
      String](Map((x: Any) => x), new PushFinishStage(onPostStop = () => testActor ! "stop"), Map((x: Any) => x)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete("foo")
      lastEvents() should be(Set(OnNext("foo"), OnComplete))
      expectMsg("stop")
    }

    "postStop when pushAndFinish called with pushAndFinish if upstream completes with pushAndFinish and downstream immediately pulls" in new OneBoundedSetup[
      String](new PushFinishStage(onPostStop = () => testActor ! "stop"), Fold("", (x: String, y: String) => x + y)) {

      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNextAndComplete("foo")
      lastEvents() should be(Set(OnNext("foo"), OnComplete))
      expectMsg("stop")
    }

  }

  private[akka] case class PreStartAndPostStopIdentity[T](
      onStart: () => Unit = () => (),
      onStop: () => Unit = () => (),
      onUpstreamCompleted: () => Unit = () => (),
      onUpstreamFailed: Throwable => Unit = _ => ())
      extends SimpleLinearGraphStage[T] {

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        override def preStart(): Unit = onStart()
        override def postStop(): Unit = onStop()

        override def onPush(): Unit = push(out, grab(in))
        override def onPull(): Unit = pull(in)

        override def onUpstreamFinish(): Unit = {
          onUpstreamCompleted()
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(cause: Throwable): Unit = {
          onUpstreamFailed(cause)
          super.onUpstreamFailure(cause)
        }

        setHandlers(in, out, this)
      }

    override def toString = "PreStartAndPostStopIdentity"
  }

  private[akka] case class PreStartFailer[T](pleaseThrow: () => Unit) extends SimpleLinearGraphStage[T] {

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        override def preStart(): Unit = pleaseThrow()

        override def onPush(): Unit = push(out, grab(in))
        override def onPull(): Unit = pull(in)

        setHandlers(in, out, this)
      }

    override def toString = "PreStartFailer"
  }

  private[akka] case class PostStopFailer[T](pleaseThrow: () => Unit) extends SimpleLinearGraphStage[T] {

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onUpstreamFinish(): Unit = completeStage()

        override def onPush(): Unit = push(out, grab(in))
        override def onPull(): Unit = pull(in)

        override def postStop(): Unit = pleaseThrow()

        setHandlers(in, out, this)
      }

    override def toString = "PostStopFailer"
  }

  // This test is related to issue #17351
  private[akka] class PushFinishStage(onPostStop: () => Unit = () => ()) extends SimpleLinearGraphStage[Any] {

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onPush(): Unit = {
          push(out, grab(in))
          completeStage()
        }
        override def onPull(): Unit = pull(in)

        override def onUpstreamFinish(): Unit = failStage(TE("Cannot happen"))

        override def postStop(): Unit = onPostStop()

        setHandlers(in, out, this)
      }

    override def toString = "PushFinish"
  }
}
