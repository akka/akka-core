/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicReference

import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.stream.scaladsl.Source
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }
import akka.stream.testkit.StreamSpec
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
        System.runFinalization()
        withClue("finished stage logic was still strongly reachable: ") {
          captured.get().get() should ===(null)
        }
      }

      probe.cancel()
    }
  }
}
