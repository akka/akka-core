/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn

import org.reactivestreams.Subscriber

import akka.stream._
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.testkit._

object GraphFlowSpec {
  val source1 = Source(0 to 3)

  val partialGraph = GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._
    val source2 = Source(4 to 9)
    val source3 = Source.empty[Int]
    val source4 = Source.empty[String]

    val inMerge = b.add(Merge[Int](2))
    val outMerge = b.add(Merge[String](2))
    val m2 = b.add(Merge[Int](2))

    inMerge.out.map(_ * 2) ~> m2.in(0)
    m2.out.map(_ / 2).map(i => (i + 1).toString) ~> outMerge.in(0)

    source2 ~> inMerge.in(0)
    source3 ~> m2.in(1)
    source4 ~> outMerge.in(1)
    FlowShape(inMerge.in(1), outMerge.out)
  }

  val stdRequests = 10
  val stdResult = Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

@nowarn
class GraphFlowSpec extends StreamSpec {

  import GraphFlowSpec._

  val settings = ActorMaterializerSettings(system).withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  def validateProbe(probe: TestSubscriber.ManualProbe[Int], requests: Int, result: Set[Int]): Unit = {
    val subscription = probe.expectSubscription()

    val collected = (1 to requests).map { _ =>
      subscription.request(1)
      probe.expectNext()
    }.toSet

    collected should be(result)
    probe.expectComplete()

  }

  "GraphDSLs" when {
    "turned into flows" should {
      "work with a Source and Sink" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val flow = Flow.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          FlowShape(partial.in, partial.out.map(_.toInt).outlet)
        })

        source1.via(flow).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "be transformable with a Pipe" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val flow = Flow.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          FlowShape(partial.in, partial.out)
        })

        source1.via(flow).map(_.toInt).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "work with another GraphFlow" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val flow1 = Flow.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          FlowShape(partial.in, partial.out)
        })

        val flow2 = Flow.fromGraph(GraphDSL.createGraph(Flow[String].map(_.toInt)) { implicit b => importFlow =>
          FlowShape(importFlow.in, importFlow.out)
        })

        source1.via(flow1).via(flow2).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "be reusable multiple times" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val flow = Flow.fromGraph(GraphDSL.createGraph(Flow[Int].map(_ * 2)) { implicit b => importFlow =>
          FlowShape(importFlow.in, importFlow.out)
        })

        RunnableGraph
          .fromGraph(GraphDSL.create() { implicit b =>
            import GraphDSL.Implicits._
            Source(1 to 5) ~> flow ~> flow ~> Sink.fromSubscriber(probe)
            ClosedShape
          })
          .run()

        validateProbe(probe, 5, Set(4, 8, 12, 16, 20))
      }
    }

    "turned into sources" should {
      "work with a Sink" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val source = Source.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          source1 ~> partial.in
          SourceShape(partial.out.map(_.toInt).outlet)
        })

        source.to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "work with a Sink when having KeyedSource inside" in {
        val probe = TestSubscriber.manualProbe[Int]()
        val source = Source.asSubscriber[Int]
        val mm: Subscriber[Int] = source.to(Sink.fromSubscriber(probe)).run()
        source1.to(Sink.fromSubscriber(mm)).run()

        validateProbe(probe, 4, (0 to 3).toSet)
      }

      "be transformable with a Pipe" in {

        val probe = TestSubscriber.manualProbe[Int]()

        val source = Source.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          source1 ~> partial.in
          SourceShape(partial.out)
        })

        source.map(_.toInt).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "work with an GraphFlow" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val source = Source.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          source1 ~> partial.in
          SourceShape(partial.out)
        })

        val flow = Flow.fromGraph(GraphDSL.createGraph(Flow[String].map(_.toInt)) { implicit b => importFlow =>
          FlowShape(importFlow.in, importFlow.out)
        })

        source.via(flow).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "be reusable multiple times" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val source = Source.fromGraph(GraphDSL.createGraph(Source(1 to 5)) { implicit b => s =>
          import GraphDSL.Implicits._
          SourceShape(s.out.map(_ * 2).outlet)
        })

        RunnableGraph
          .fromGraph(GraphDSL.createGraph(source, source)(Keep.both) { implicit b => (s1, s2) =>
            import GraphDSL.Implicits._
            val merge = b.add(Merge[Int](2))
            s1.out ~> merge.in(0)
            merge.out ~> Sink.fromSubscriber(probe)
            s2.out.map(_ * 10) ~> merge.in(1)
            ClosedShape
          })
          .run()

        validateProbe(probe, 10, Set(2, 4, 6, 8, 10, 20, 40, 60, 80, 100))
      }
    }

    "turned into sinks" should {
      "work with a Source" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val sink = Sink.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          partial.out.map(_.toInt) ~> Sink.fromSubscriber(probe)
          SinkShape(partial.in)
        })

        source1.to(sink).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "work with a Source when having KeyedSink inside" in {
        val probe = TestSubscriber.manualProbe[Int]()
        val pubSink = Sink.asPublisher[Int](false)

        val sink = Sink.fromGraph(GraphDSL.createGraph(pubSink) { implicit b => p =>
          SinkShape(p.in)
        })

        val mm = source1.runWith(sink)
        Source.fromPublisher(mm).to(Sink.fromSubscriber(probe)).run()

        validateProbe(probe, 4, (0 to 3).toSet)
      }

      "be transformable with a Pipe" in {
        val probe = TestSubscriber.manualProbe[Int]()

        val sink = Sink.fromGraph(GraphDSL.createGraph(partialGraph, Flow[String].map(_.toInt))(Keep.both) {
          implicit b => (partial, flow) =>
            import GraphDSL.Implicits._
            flow.out ~> partial.in
            partial.out.map(_.toInt) ~> Sink.fromSubscriber(probe)
            SinkShape(flow.in)
        })

        val iSink = Flow[Int].map(_.toString).to(sink)
        source1.to(iSink).run()

        validateProbe(probe, stdRequests, stdResult)
      }

      "work with a GraphFlow" in {

        val probe = TestSubscriber.manualProbe[Int]()

        val flow = Flow.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          FlowShape(partial.in, partial.out)
        })

        val sink = Sink.fromGraph(GraphDSL.createGraph(Flow[String].map(_.toInt)) { implicit b => flow =>
          import GraphDSL.Implicits._
          flow.out ~> Sink.fromSubscriber(probe)
          SinkShape(flow.in)
        })

        source1.via(flow).to(sink).run()

        validateProbe(probe, stdRequests, stdResult)
      }
    }

    "used together" should {
      "materialize properly" in {
        val probe = TestSubscriber.manualProbe[Int]()
        val inSource = Source.asSubscriber[Int]
        val outSink = Sink.asPublisher[Int](false)

        val flow = Flow.fromGraph(GraphDSL.createGraph(partialGraph) { implicit b => partial =>
          import GraphDSL.Implicits._
          FlowShape(partial.in, partial.out.map(_.toInt).outlet)
        })

        val source = Source.fromGraph(GraphDSL.createGraph(Flow[Int].map(_.toString), inSource)(Keep.right) {
          implicit b => (flow, src) =>
            import GraphDSL.Implicits._
            src.out ~> flow.in
            SourceShape(flow.out)
        })

        val sink = Sink.fromGraph(GraphDSL.createGraph(Flow[String].map(_.toInt), outSink)(Keep.right) {
          implicit b => (flow, snk) =>
            import GraphDSL.Implicits._
            flow.out ~> snk.in
            SinkShape(flow.in)
        })

        val (m1, _, m3) = RunnableGraph
          .fromGraph(GraphDSL.createGraph(source, flow, sink)(Tuple3.apply) { implicit b => (src, f, snk) =>
            import GraphDSL.Implicits._
            src.out.map(_.toInt) ~> f.in
            f.out.map(_.toString) ~> snk.in
            ClosedShape
          })
          .run()

        val subscriber = m1
        val publisher = m3
        source1.runWith(Sink.asPublisher(false)).subscribe(subscriber)
        publisher.subscribe(probe)

        validateProbe(probe, stdRequests, stdResult)
      }

      "allow connecting source to sink directly" in {
        val probe = TestSubscriber.manualProbe[Int]()
        val inSource = Source.asSubscriber[Int]
        val outSink = Sink.asPublisher[Int](false)

        val source = Source.fromGraph(GraphDSL.createGraph(inSource) { implicit b => src =>
          SourceShape(src.out)
        })

        val sink = Sink.fromGraph(GraphDSL.createGraph(outSink) { implicit b => snk =>
          SinkShape(snk.in)
        })

        val (m1, m2) = RunnableGraph
          .fromGraph(GraphDSL.createGraph(source, sink)(Keep.both) { implicit b => (src, snk) =>
            import GraphDSL.Implicits._
            src.out ~> snk.in
            ClosedShape
          })
          .run()

        val subscriber = m1
        val publisher = m2

        source1.runWith(Sink.asPublisher(false)).subscribe(subscriber)
        publisher.subscribe(probe)

        validateProbe(probe, 4, (0 to 3).toSet)
      }

    }
  }
}
