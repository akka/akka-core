/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.stage._
import akka.stream._
import akka.stream.testkit.{ TestPublisher, TestSubscriber }
import akka.testkit.AkkaSpec

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.collection.immutable.Iterable

@nowarn("msg=never used") // sample snippets
class GraphStageDocSpec extends AkkaSpec {

  "Demonstrate creation of GraphStage boilerplate" in {
    //#boilerplate-example
    import akka.stream.SourceShape
    import akka.stream.stage.GraphStage

    class NumbersSource extends GraphStage[SourceShape[Int]] {
      // Define the (sole) output port of this stage
      val out: Outlet[Int] = Outlet("NumbersSource")
      // Define the shape of this stage, which is SourceShape with the port we defined above
      override val shape: SourceShape[Int] = SourceShape(out)

      // This is where the actual (possibly stateful) logic will live
      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = ???
    }
    //#boilerplate-example

  }

  "Demonstrate creation of GraphStage Source" in {
    //#custom-source-example
    import akka.stream.Attributes
    import akka.stream.Outlet
    import akka.stream.SourceShape
    import akka.stream.stage.GraphStage
    import akka.stream.stage.GraphStageLogic
    import akka.stream.stage.OutHandler

    class NumbersSource extends GraphStage[SourceShape[Int]] {
      val out: Outlet[Int] = Outlet("NumbersSource")
      override val shape: SourceShape[Int] = SourceShape(out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {
          // All state MUST be inside the GraphStageLogic,
          // never inside the enclosing GraphStage.
          // This state is safe to access and modify from all the
          // callbacks that are provided by GraphStageLogic and the
          // registered handlers.
          private var counter = 1

          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              push(out, counter)
              counter += 1
            }
          })
        }
    }
    //#custom-source-example

    //#simple-source-usage
    // A GraphStage is a proper Graph, just like what GraphDSL.create would return
    val sourceGraph: Graph[SourceShape[Int], NotUsed] = new NumbersSource

    // Create a Source from the Graph to access the DSL
    val mySource: Source[Int, NotUsed] = Source.fromGraph(sourceGraph)

    // Returns 55
    val result1: Future[Int] = mySource.take(10).runFold(0)(_ + _)

    // The source is reusable. This returns 5050
    val result2: Future[Int] = mySource.take(100).runFold(0)(_ + _)
    //#simple-source-usage

    Await.result(result1, 3.seconds) should ===(55)
    Await.result(result2, 3.seconds) should ===(5050)
  }

  "Demonstrate creation of GraphStage Sink" in {
    //#custom-sink-example
    import akka.stream.Attributes
    import akka.stream.Inlet
    import akka.stream.SinkShape
    import akka.stream.stage.GraphStage
    import akka.stream.stage.GraphStageLogic
    import akka.stream.stage.InHandler

    class StdoutSink extends GraphStage[SinkShape[Int]] {
      val in: Inlet[Int] = Inlet("StdoutSink")
      override val shape: SinkShape[Int] = SinkShape(in)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          // This requests one element at the Sink startup.
          override def preStart(): Unit = pull(in)

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              println(grab(in))
              pull(in)
            }
          })
        }
    }
    //#custom-sink-example

    Source(List(0, 1, 2)).runWith(Sink.fromGraph(new StdoutSink))
  }

  //#one-to-one
  class Map[A, B](f: A => B) extends GraphStage[FlowShape[A, B]] {

    val in = Inlet[A]("Map.in")
    val out = Outlet[B]("Map.out")

    override val shape = FlowShape.of(in, out)

    override def createLogic(attr: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            push(out, f(grab(in)))
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }
  //#one-to-one

  "Demonstrate a one to one element GraphStage" in {
    // tests:
    val stringLength = Flow.fromGraph(new Map[String, Int](_.length))

    val result =
      Source(Vector("one", "two", "three")).via(stringLength).runFold(Seq.empty[Int])((elem, acc) => elem :+ acc)

    Await.result(result, 3.seconds) should ===(Seq(3, 3, 5))
  }

  //#many-to-one
  class Filter[A](p: A => Boolean) extends GraphStage[FlowShape[A, A]] {

    val in = Inlet[A]("Filter.in")
    val out = Outlet[A]("Filter.out")

    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (p(elem)) push(out, elem)
            else pull(in)
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }
  //#many-to-one

  "Demonstrate a many to one element GraphStage" in {

    // tests:
    val evenFilter = Flow.fromGraph(new Filter[Int](_ % 2 == 0))

    val result =
      Source(Vector(1, 2, 3, 4, 5, 6)).via(evenFilter).runFold(Seq.empty[Int])((elem, acc) => elem :+ acc)

    Await.result(result, 3.seconds) should ===(Seq(2, 4, 6))
  }

  //#one-to-many
  class Duplicator[A] extends GraphStage[FlowShape[A, A]] {

    val in = Inlet[A]("Duplicator.in")
    val out = Outlet[A]("Duplicator.out")

    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        // Again: note that all mutable state
        // MUST be inside the GraphStageLogic
        var lastElem: Option[A] = None

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            lastElem = Some(elem)
            push(out, elem)
          }

          override def onUpstreamFinish(): Unit = {
            if (lastElem.isDefined) emit(out, lastElem.get)
            complete(out)
          }

        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (lastElem.isDefined) {
              push(out, lastElem.get)
              lastElem = None
            } else {
              pull(in)
            }
          }
        })
      }
  }
  //#one-to-many

  "Demonstrate a one to many element GraphStage" in {
    // tests:
    val duplicator = Flow.fromGraph(new Duplicator[Int])

    val result =
      Source(Vector(1, 2, 3)).via(duplicator).runFold(Seq.empty[Int])((elem, acc) => elem :+ acc)

    Await.result(result, 3.seconds) should ===(Seq(1, 1, 2, 2, 3, 3))
  }

  "Demonstrate a simpler one to many stage" in {
    //#simpler-one-to-many
    class Duplicator[A] extends GraphStage[FlowShape[A, A]] {

      val in = Inlet[A]("Duplicator.in")
      val out = Outlet[A]("Duplicator.out")

      val shape = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              // this will temporarily suspend this handler until the two elems
              // are emitted and then reinstates it
              emitMultiple(out, Iterable(elem, elem))
            }
          })
          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              pull(in)
            }
          })
        }
    }
    //#simpler-one-to-many

    // tests:
    val duplicator = Flow.fromGraph(new Duplicator[Int])

    val result =
      Source(Vector(1, 2, 3)).via(duplicator).runFold(Seq.empty[Int])((elem, acc) => elem :+ acc)

    Await.result(result, 3.seconds) should ===(Seq(1, 1, 2, 2, 3, 3))

  }

  "Demonstrate chaining of graph stages" in {
    val sink = Sink.fold[List[Int], Int](List.empty[Int])((acc, n) => acc :+ n)

    //#graph-operator-chain
    val resultFuture =
      Source(1 to 5).via(new Filter(_ % 2 == 0)).via(new Duplicator()).via(new Map(_ / 2)).runWith(sink)

    //#graph-operator-chain

    Await.result(resultFuture, 3.seconds) should ===(List(1, 1, 2, 2))
  }

  "Demonstrate an asynchronous side channel" in {
    import system.dispatcher
    //#async-side-channel
    // will close upstream in all materializations of the graph stage instance
    // when the future completes
    class KillSwitch[A](switch: Future[Unit]) extends GraphStage[FlowShape[A, A]] {

      val in = Inlet[A]("KillSwitch.in")
      val out = Outlet[A]("KillSwitch.out")

      val shape = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          override def preStart(): Unit = {
            val callback = getAsyncCallback[Unit] { (_) =>
              completeStage()
            }
            switch.foreach(callback.invoke)
          }

          setHandler(in, new InHandler {
            override def onPush(): Unit = { push(out, grab(in)) }
          })
          setHandler(out, new OutHandler {
            override def onPull(): Unit = { pull(in) }
          })
        }
    }
    //#async-side-channel

    // tests:

    val switch = Promise[Unit]()
    val duplicator = Flow.fromGraph(new KillSwitch[Int](switch.future))

    val in = TestPublisher.Probe[Int]()
    val out = TestSubscriber.Probe[Int]()

    Source
      .fromPublisher(in)
      .via(duplicator)
      .to(Sink.fromSubscriber(out))
      .withAttributes(Attributes.inputBuffer(1, 1))
      .run()

    val sub = in.expectSubscription()

    out.request(1)

    sub.expectRequest()
    sub.sendNext(1)

    out.expectNext(1)

    switch.success(())

    out.expectComplete()
  }

  "Demonstrate a graph stage with a timer" in {

    //#timed
    // each time an event is pushed through it will trigger a period of silence
    class TimedGate[A](silencePeriod: FiniteDuration) extends GraphStage[FlowShape[A, A]] {

      val in = Inlet[A]("TimedGate.in")
      val out = Outlet[A]("TimedGate.out")

      val shape = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new TimerGraphStageLogic(shape) {

          var open = false

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              if (open) pull(in)
              else {
                push(out, elem)
                open = true
                scheduleOnce(None, silencePeriod)
              }
            }
          })
          setHandler(out, new OutHandler {
            override def onPull(): Unit = { pull(in) }
          })

          override protected def onTimer(timerKey: Any): Unit = {
            open = false
          }
        }
    }
    //#timed

    // tests:
    val result =
      Source(Vector(1, 2, 3))
        .via(new TimedGate[Int](2.second))
        .takeWithin(250.millis)
        .runFold(Seq.empty[Int])((elem, acc) => elem :+ acc)

    Await.result(result, 3.seconds) should ===(Seq(1))
  }

  "Demonstrate a custom materialized value" in {

    //#materialized
    class FirstValue[A] extends GraphStageWithMaterializedValue[FlowShape[A, A], Future[A]] {

      val in = Inlet[A]("FirstValue.in")
      val out = Outlet[A]("FirstValue.out")

      val shape = FlowShape.of(in, out)

      override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[A]) = {
        val promise = Promise[A]()
        val logic = new GraphStageLogic(shape) {

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              promise.success(elem)
              push(out, elem)

              // replace handler with one that only forwards elements
              setHandler(in, new InHandler {
                override def onPush(): Unit = {
                  push(out, grab(in))
                }
              })
            }
          })

          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              pull(in)
            }
          })

        }

        (logic, promise.future)
      }
    }
    //#materialized

    // tests:
    val flow = Source(Vector(1, 2, 3)).viaMat(new FirstValue)(Keep.right).to(Sink.ignore)

    val result: Future[Int] = flow.run()

    Await.result(result, 3.seconds) should ===(1)

  }

  "Demonstrate a detached graph stage" in {

    //#detached
    class TwoBuffer[A] extends GraphStage[FlowShape[A, A]] {

      val in = Inlet[A]("TwoBuffer.in")
      val out = Outlet[A]("TwoBuffer.out")

      val shape = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          val buffer = mutable.Queue[A]()
          def bufferFull = buffer.size == 2
          var downstreamWaiting = false

          override def preStart(): Unit = {
            // a detached stage needs to start upstream demand
            // itself as it is not triggered by downstream demand
            pull(in)
          }

          setHandler(
            in,
            new InHandler {
              override def onPush(): Unit = {
                val elem = grab(in)
                buffer.enqueue(elem)
                if (downstreamWaiting) {
                  downstreamWaiting = false
                  val bufferedElem = buffer.dequeue()
                  push(out, bufferedElem)
                }
                if (!bufferFull) {
                  pull(in)
                }
              }

              override def onUpstreamFinish(): Unit = {
                if (buffer.nonEmpty) {
                  // emit the rest if possible
                  emitMultiple(out, buffer.iterator)
                }
                completeStage()
              }
            })

          setHandler(
            out,
            new OutHandler {
              override def onPull(): Unit = {
                if (buffer.isEmpty) {
                  downstreamWaiting = true
                } else {
                  val elem = buffer.dequeue()
                  push(out, elem)
                }
                if (!bufferFull && !hasBeenPulled(in)) {
                  pull(in)
                }
              }
            })
        }

    }
    //#detached

    // tests:
    val result1 = Source(Vector(1, 2, 3)).via(new TwoBuffer).runFold(Vector.empty[Int])((acc, n) => acc :+ n)

    Await.result(result1, 3.seconds) should ===(Vector(1, 2, 3))

    val subscriber = TestSubscriber.ManualProbe[Int]()
    val publisher = TestPublisher.Probe[Int]()
    val flow2 =
      Source.fromPublisher(publisher).via(new TwoBuffer).to(Sink.fromSubscriber(subscriber))

    val result2 = flow2.run()

    val sub = subscriber.expectSubscription()
    // this happens even though the subscriber has not signalled any demand
    publisher.sendNext(1)
    publisher.sendNext(2)

    sub.cancel()
  }

  "Demonstrate stream extension" when {

    "targeting a Source" in {
      //#extending-source
      implicit class SourceDuplicator[Out, Mat](s: Source[Out, Mat]) {
        def duplicateElements: Source[Out, Mat] = s.via(new Duplicator)
      }

      val s = Source(1 to 3).duplicateElements

      s.runWith(Sink.seq).futureValue should ===(Seq(1, 1, 2, 2, 3, 3))
      //#extending-source
    }

    "targeting a Flow" in {
      //#extending-flow
      implicit class FlowDuplicator[In, Out, Mat](s: Flow[In, Out, Mat]) {
        def duplicateElements: Flow[In, Out, Mat] = s.via(new Duplicator)
      }

      val f = Flow[Int].duplicateElements

      Source(1 to 3).via(f).runWith(Sink.seq).futureValue should ===(Seq(1, 1, 2, 2, 3, 3))
      //#extending-flow
    }

  }

}
