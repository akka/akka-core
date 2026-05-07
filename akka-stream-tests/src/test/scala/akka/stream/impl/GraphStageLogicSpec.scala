/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.concurrent.duration.Duration

import org.scalatest.concurrent.ScalaFutures

import akka.NotUsed
import akka.stream._
import akka.stream.impl.fusing._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.stage.GraphStageLogic.{ EagerTerminateInput, EagerTerminateOutput }
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink

class GraphStageLogicSpec extends StreamSpec with GraphInterpreterSpecKit with ScalaFutures {

  object emit1234 extends GraphStage[FlowShape[Int, Int]] {
    val in = Inlet[Int]("in")
    val out = Outlet[Int]("out")
    override val shape = FlowShape(in, out)
    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
      setHandler(in, eagerTerminateInput)
      setHandler(out, eagerTerminateOutput)
      override def preStart(): Unit = {
        emit(out, 1, () => emit(out, 2))
        emit(out, 3, () => emit(out, 4))
      }
    }
  }

  class SubstreamEmit extends GraphStage[SourceShape[Source[Int, NotUsed]]] {
    val out = Outlet[Source[Int, NotUsed]]("out")
    override val shape = SourceShape(out)

    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) with OutHandler {

      setHandler(out, this)

      override def onPull(): Unit = {
        val subOut = new SubSourceOutlet[Int]("subOut")
        subOut.setHandler(new OutHandler {
          override def onPull(): Unit = {
            ()
          }
        })
        subOut.push(1)
        subOut.push(2) // expecting this to fail!

        ???
      }
    }
  }

  object emit5678 extends GraphStage[FlowShape[Int, Int]] {
    val in = Inlet[Int]("in")
    val out = Outlet[Int]("out")
    override val shape = FlowShape(in, out)
    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = push(out, grab(in))
        override def onUpstreamFinish(): Unit = {
          emit(out, 5, () => emit(out, 6))
          emit(out, 7, () => emit(out, 8))
          completeStage()
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
  }

  object passThrough extends GraphStage[FlowShape[Int, Int]] {
    val in = Inlet[Int]("in")
    val out = Outlet[Int]("out")
    override val shape = FlowShape(in, out)
    override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = push(out, grab(in))
        override def onUpstreamFinish(): Unit = complete(out)
        override def toString = "InHandler"
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
        override def toString = "OutHandler"
      })
      override def toString = "GraphStageLogicSpec.passthroughLogic"
    }
    override def toString = "GraphStageLogicSpec.passthrough"
  }

  object emitEmptyIterable extends GraphStage[SourceShape[Int]] {
    val out = Outlet[Int]("out")
    override val shape = SourceShape(out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      setHandler(out, new OutHandler {
        override def onPull(): Unit = emitMultiple(out, Iterator.empty, () => emit(out, 42, () => completeStage()))
      })
    }
    override def toString = "GraphStageLogicSpec.emitEmptyIterable"
  }

  private case class ReadNEmitN(n: Int) extends GraphStage[FlowShape[Int, Int]] {
    override val shape = FlowShape(Inlet[Int]("readN.in"), Outlet[Int]("readN.out"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(shape.in, EagerTerminateInput)
        setHandler(shape.out, EagerTerminateOutput)
        override def preStart(): Unit =
          readN(shape.in, n)(e => emitMultiple(shape.out, e.iterator, () => completeStage()), (_) => ())
      }
  }

  private case class ReadNEmitRestOnComplete(n: Int) extends GraphStage[FlowShape[Int, Int]] {
    override val shape = FlowShape(Inlet[Int]("readN.in"), Outlet[Int]("readN.out"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(shape.in, EagerTerminateInput)
        setHandler(shape.out, EagerTerminateOutput)
        override def preStart(): Unit =
          readN(shape.in, n)(
            _ => failStage(new IllegalStateException("Shouldn't happen!")),
            e => emitMultiple(shape.out, e.iterator, () => completeStage()))
      }
  }

  "A GraphStageLogic" must {

    "read N and emit N before completing" in {
      Source(1 to 10).via(ReadNEmitN(2)).runWith(TestSink()).request(10).expectNext(1, 2).expectComplete()
    }

    "read N should not emit if upstream completes before N is sent" in {
      Source(1 to 5).via(ReadNEmitN(6)).runWith(TestSink()).request(10).expectComplete()
    }

    "read N should not emit if upstream fails before N is sent" in {
      val error = new IllegalArgumentException("Don't argue like that!")
      Source(1 to 5)
        .map(x => if (x > 3) throw error else x)
        .via(ReadNEmitN(6))
        .runWith(TestSink())
        .request(10)
        .expectError(error)
    }

    "read N should provide elements read if onComplete happens before N elements have been seen" in {
      Source(1 to 5)
        .via(ReadNEmitRestOnComplete(6))
        .runWith(TestSink())
        .request(10)
        .expectNext(1, 2, 3, 4, 5)
        .expectComplete()
    }

    "emit all things before completing" in {

      Source.empty
        .via(emit1234.named("testStage"))
        .runWith(TestSink())
        .request(5)
        .expectNext(1)
        //emitting with callback gives nondeterminism whether 2 or 3 will be pushed first
        .expectNextUnordered(2, 3)
        .expectNext(4)
        .expectComplete()

    }

    "emit properly after empty iterable" in {

      Source.fromGraph(emitEmptyIterable).runWith(Sink.seq).futureValue should ===(List(42))

    }

    "invoke lifecycle hooks in the right order" in {
      val g = new GraphStage[FlowShape[Int, Int]] {
        val in = Inlet[Int]("in")
        val out = Outlet[Int]("out")
        override val shape = FlowShape(in, out)
        override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
          setHandler(in, eagerTerminateInput)
          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              completeStage()
              testActor ! "pulled"
            }
          })
          override def preStart(): Unit = testActor ! "preStart"
          override def postStop(): Unit = testActor ! "postStop"
        }
      }
      Source.single(1).via(g).runWith(Sink.ignore)
      expectMsg("preStart")
      expectMsg("pulled")
      expectMsg("postStop")
    }

    "not double-terminate a single stage" in new Builder {
      object g extends GraphStage[FlowShape[Int, Int]] {
        val in = Inlet[Int]("in")
        val out = Outlet[Int]("out")
        override val shape = FlowShape(in, out)
        override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
          setHandler(in, eagerTerminateInput)
          setHandler(out, eagerTerminateOutput)
          override def postStop(): Unit = testActor ! "postStop2"
        }
      }

      builder(g, passThrough)
        .connect(Upstream, g.in)
        .connect(g.out, passThrough.in)
        .connect(passThrough.out, Downstream)
        .init()

      // note: a bit dangerous assumptions about connection and logic positions here
      // if anything around creating the logics and connections in the builder changes this may fail
      interpreter.complete(interpreter.connections(0))
      interpreter.cancel(interpreter.connections(1), SubscriptionWithCancelException.NoMoreElementsNeeded)
      interpreter.execute(2)

      expectMsg("postStop2")
      expectNoMessage(Duration.Zero)

      interpreter.isCompleted should ===(false)
      interpreter.isSuspended should ===(false)
      interpreter.isStageCompleted(interpreter.logics(1)) should ===(true)
      interpreter.isStageCompleted(interpreter.logics(2)) should ===(false)
    }

    "not allow push from constructor" in {
      object source extends GraphStage[SourceShape[Int]] {
        val out = Outlet[Int]("out")
        override val shape = SourceShape(out)
        override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
          push(out, 1)
        }
      }

      val ex = intercept[IllegalStateException] { Source.fromGraph(source).runWith(Sink.ignore) }
      ex.getMessage should startWith("not yet initialized: only setHandler is allowed in GraphStageLogic constructor")
    }

    "not allow pull from constructor" in {
      object sink extends GraphStage[SinkShape[Int]] {
        val in = Inlet[Int]("in")
        override val shape = SinkShape(in)
        override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
          pull(in)
        }
      }

      val ex = intercept[IllegalStateException] { Source.single(1).runWith(sink) }
      ex.getMessage should startWith("not yet initialized: only setHandler is allowed in GraphStageLogic constructor")
    }

    "give a good error message if in handler missing" in {
      val ex = intercept[IllegalStateException] {
        Source
          .maybe[String]
          .via(new GraphStage[FlowShape[String, String]] {
            val in = Inlet[String]("in")
            val out = Outlet[String]("out")
            override val shape: FlowShape[String, String] = FlowShape(in, out)

            override def createLogic(inheritedAttributes: Attributes) =
              new GraphStageLogic(shape) {
                // ups, we forgot to actually set the handlers
              }

            override def toString = "stage-name"
          })
          .runWith(Sink.ignore)
      }
      ex.getMessage should startWith("No handler defined in stage [").and(include("] for in port [in"))
    }

    "give a good error message if out handler missing" in {
      val ex = intercept[IllegalStateException] {
        Source
          .maybe[String]
          .via(new GraphStage[FlowShape[String, String]] {
            val in = Inlet[String]("in")
            val out = Outlet[String]("out")
            override val shape: FlowShape[String, String] = FlowShape(in, out)

            override def createLogic(inheritedAttributes: Attributes) =
              new GraphStageLogic(shape) {
                setHandler(in, new InHandler {
                  override def onPush() = ???
                })
                // ups we forgot the out handler
              }

            override def toString = "stage-name"
          })
          // just to have another graphstage downstream
          .map(_ => "whatever")
          .runWith(Sink.ignore)
      }
      ex.getMessage should startWith("No handler defined in stage [").and(include("] for out port [out"))
    }

    "give a good error message if out handler missing with downstream boundary" in {
      val ex = intercept[IllegalStateException] {
        Source
          .maybe[String]
          .via(new GraphStage[FlowShape[String, String]] {
            val in = Inlet[String]("in")
            val out = Outlet[String]("out")
            override val shape: FlowShape[String, String] = FlowShape(in, out)

            override def createLogic(inheritedAttributes: Attributes) =
              new GraphStageLogic(shape) {
                setHandler(in, new InHandler {
                  override def onPush() = ???
                })
                // ups we forgot the out handler
              }

            override def toString = "stage-name"
          })
          .runWith(Sink.ignore.async)
      }
      ex.getMessage should startWith("No handler defined in stage [").and(include("] for out port [out"))
    }

    "give a good error message if handler missing with downstream publisher" in {
      val ex = intercept[IllegalStateException] {
        Source
          .maybe[String]
          .async
          .via(new GraphStage[FlowShape[String, String]] {
            val in = Inlet[String]("in")
            val out = Outlet[String]("out")
            override val shape: FlowShape[String, String] = FlowShape(in, out)

            override def createLogic(inheritedAttributes: Attributes) =
              new GraphStageLogic(shape) {
                setHandler(in, new InHandler {
                  override def onPush() = ???
                })
                // ups we forgot the out handler
              }

            override def toString = "stage-name"
          })
          .runWith(Sink.ignore)
      }
      ex.getMessage should startWith("No handler defined in stage [").and(include("] for out port [out"))
    }

    "give a good error message if handler missing when stage is an island" in {
      val ex = intercept[IllegalStateException] {
        Source
          .maybe[String]
          .via(new GraphStage[FlowShape[String, String]] {
            val in = Inlet[String]("in")
            val out = Outlet[String]("out")
            override val shape: FlowShape[String, String] = FlowShape(in, out)

            override def createLogic(inheritedAttributes: Attributes) =
              new GraphStageLogic(shape) {
                setHandler(in, new InHandler {
                  override def onPush() = ???
                })
                // ups we forgot the out handler
              }

            override def toString = "stage-name"
          })
          .async
          .runWith(Sink.ignore)
      }
      ex.getMessage should startWith("No handler defined in stage [").and(include("] for out port [out"))
    }

    "give a good error message if sub source is pushed twice" in {
      intercept[Exception] {
        Source.fromGraph(new SubstreamEmit()).async.runWith(Sink.ignore).futureValue
      }.getCause.getMessage should startWith(
        "Cannot push port (SubSourceOutlet(subOut)) twice, or before it being pulled")
    }

  }

}
