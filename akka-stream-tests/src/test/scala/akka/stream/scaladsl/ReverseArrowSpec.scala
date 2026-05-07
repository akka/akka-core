/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._

class ReverseArrowSpec extends StreamSpec {
  import GraphDSL.Implicits._

  val source = Source(List(1, 2, 3))
  val sink = Flow[Int].limit(10).toMat(Sink.seq)(Keep.right)

  "Reverse Arrows in the Graph DSL" must {

    "work from Inlets" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            s.in <~ source
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work from SinkShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            s <~ source
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work from Sink" in {
      val sub = TestSubscriber.manualProbe[Int]()
      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          Sink.fromSubscriber(sub) <~ source
          ClosedShape
        })
        .run()
      sub.expectSubscription().request(10)
      sub.expectNext(1, 2, 3)
      sub.expectComplete()
    }

    "not work from Outlets" in {
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        val o: Outlet[Int] = b.add(source).out
        "o <~ source" shouldNot compile
        sink <~ o
        ClosedShape
      })
    }

    "not work from SourceShape" in {
      RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        val o: SourceShape[Int] = b.add(source)
        "o <~ source" shouldNot compile
        sink <~ o
        ClosedShape
      })
    }

    "not work from Source" in {
      "source <~ source" shouldNot compile
    }

    "work from FlowShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: FlowShape[Int, Int] = b.add(Flow[Int])
            f <~ source
            f ~> s
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work from UniformFanInShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanInShape[Int, Int] = b.add(Merge[Int](2))
            f <~ source
            f <~ Source.empty
            f ~> s
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work from UniformFanOutShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanOutShape[Int, Int] = b.add(Broadcast[Int](2))
            f <~ source
            f ~> Sink.ignore
            f ~> s
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards Outlets" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val o: Outlet[Int] = b.add(source).out
            s <~ o
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards SourceShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val o: SourceShape[Int] = b.add(source)
            s <~ o
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards Source" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            s <~ source
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards FlowShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: FlowShape[Int, Int] = b.add(Flow[Int])
            s <~ f
            source ~> f
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards UniformFanInShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanInShape[Int, Int] = b.add(Merge[Int](2))
            s <~ f
            Source.empty ~> f
            source ~> f
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "fail towards already full UniformFanInShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanInShape[Int, Int] = b.add(Merge[Int](2))
            val src = b.add(source)
            Source.empty ~> f
            src ~> f
            (the[IllegalArgumentException] thrownBy (s <~ f <~ src)).getMessage should include("no more inlets free")
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work towards UniformFanOutShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanOutShape[Int, Int] = b.add(Broadcast[Int](2))
            s <~ f
            Sink.ignore <~ f
            source ~> f
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "fail towards already full UniformFanOutShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            val f: UniformFanOutShape[Int, Int] = b.add(Broadcast[Int](2))
            val sink2: SinkShape[Int] = b.add(Sink.ignore)
            val src = b.add(source)
            src ~> f
            sink2 <~ f
            (the[IllegalArgumentException] thrownBy (s <~ f <~ src)).getMessage should include(
              "[IterableSource.out] is already connected")
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work across a Flow" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            s <~ Flow[Int] <~ source
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

    "work across a FlowShape" in {
      Await.result(
        RunnableGraph
          .fromGraph(GraphDSL.createGraph(sink) { implicit b => s =>
            s <~ b.add(Flow[Int]) <~ source
            ClosedShape
          })
          .run(),
        1.second) should ===(Seq(1, 2, 3))
    }

  }

}
