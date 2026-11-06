/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import akka.stream.{ Attributes, FlowShape, Inlet, Materializer, Outlet, SourceShape }
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.impl.SubFusingActorMaterializerImpl
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink

object GraphInterpreterReleaseSpec {

  // A source that emits a single element and then completes, while holding on to a chunk of state.
  // The weak reference lets the test observe whether the finished logic is still reachable.
  class PayloadSource(captured: AtomicReference[WeakReference[AnyRef]]) extends GraphStage[SourceShape[String]] {
    val out = Outlet[String]("out")
    override val shape = SourceShape(out)

    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
      // state captured by the logic, only reachable through it
      private val payload: AnyRef = new Array[Byte](8 * 1024)
      captured.set(new WeakReference(payload))

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          push(out, "one")
          complete(out)
        }
      })
    }
  }

  // Pass through, counting how many times the logic is started and stopped.
  class LifecycleCounting(preStarts: AtomicInteger, postStops: AtomicInteger) extends GraphStage[FlowShape[Int, Int]] {
    val in = Inlet[Int]("in")
    val out = Outlet[Int]("out")
    override val shape = FlowShape(in, out)

    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
      override def preStart(): Unit = preStarts.incrementAndGet()
      override def postStop(): Unit = postStops.incrementAndGet()
      override def onPush(): Unit = push(out, grab(in))
      override def onPull(): Unit = pull(in)
      setHandlers(in, out, this)
    }
  }
}

class GraphInterpreterReleaseSpec extends StreamSpec {
  import GraphInterpreterReleaseSpec._

  "the interpreter" must {

    "release a finished stage while the rest of the fused island keeps running" in {
      val captured = new AtomicReference[WeakReference[AnyRef]]()

      // the single-element source finishes, concat + Source.maybe + sink keep the island running
      val probe = Source.fromGraph(new PayloadSource(captured)).concat(Source.maybe[String]).runWith(TestSink[String]())

      probe.requestNext("one")

      // the stage that produced the element is done now, but the stream is still running
      awaitAssert {
        System.gc()
        withClue("finished stage logic was still strongly reachable: ") {
          captured.get().get() should ===(null)
        }
      }

      probe.cancel()
    }

    "not start a released logic again when an aborted shell is initialized a second time" in {
      // ActorGraphInterpreter.postStop initializes every shell in newShells, and a shell ends up in both
      // activeInterpreters and newShells when an event for it is processed before its registration is
      // finished. Such a shell is aborted first, so by the time it is initialized again all its logics are
      // finalized and released. Driving the shell directly here, the ordering that leads to it is a race.
      val preStarts = new AtomicInteger()
      val postStops = new AtomicInteger()

      val shell = new AtomicReference[GraphInterpreterShell]()
      val subFusingMaterializer = new SubFusingActorMaterializerImpl(
        Materializer(system).asInstanceOf[PhasedFusingActorMaterializer],
        registeredShell => {
          shell.set(registeredShell)
          testActor
        })

      // Source.maybe never completes, so the logics are still running when the shell is aborted
      subFusingMaterializer.materialize(
        Source.maybe[Int].via(new LifecycleCounting(preStarts, postStops)).to(Sink.ignore))

      shell.get().init(testActor, subFusingMaterializer, _ => (), eventLimit = 1000)
      preStarts.get() should ===(1)
      postStops.get() should ===(0)

      shell.get().tryAbort(TE("abrupt termination"))
      postStops.get() should ===(1)

      shell.get().init(testActor, subFusingMaterializer, _ => (), eventLimit = 1000)
      withClue("a released logic must not be started or stopped again: ") {
        preStarts.get() should ===(1)
        postStops.get() should ===(1)
      }
    }
  }
}
