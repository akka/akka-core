/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.stream._
import akka.stream.testkit._

class GraphMatValueSpec extends StreamSpec {

  import GraphDSL.Implicits._

  val foldSink = Sink.fold[Int, Int](0)(_ + _)

  "A Graph with materialized value" must {

    "expose the materialized value as source" in {
      val sub = TestSubscriber.manualProbe[Int]()
      val f = RunnableGraph
        .fromGraph(GraphDSL.createGraph(foldSink) { implicit b => fold =>
          Source(1 to 10) ~> fold
          b.materializedValue.mapAsync(4)(identity) ~> Sink.fromSubscriber(sub)
          ClosedShape
        })
        .run()

      val r1 = Await.result(f, 3.seconds)
      sub.expectSubscription().request(1)
      val r2 = sub.expectNext()

      r1 should ===(r2)
    }

    "expose the materialized value as source multiple times" in {
      val sub = TestSubscriber.manualProbe[Int]()

      val f = RunnableGraph
        .fromGraph(GraphDSL.createGraph(foldSink) { implicit b => fold =>
          val zip = b.add(ZipWith[Int, Int, Int](_ + _))
          Source(1 to 10) ~> fold
          b.materializedValue.mapAsync(4)(identity) ~> zip.in0
          b.materializedValue.mapAsync(4)(identity) ~> zip.in1

          zip.out ~> Sink.fromSubscriber(sub)
          ClosedShape
        })
        .run()

      val r1 = Await.result(f, 3.seconds)
      sub.expectSubscription().request(1)
      val r2 = sub.expectNext()

      r1 should ===(r2 / 2)
    }

    // Exposes the materialized value as a stream value
    val foldFeedbackSource: Source[Future[Int], Future[Int]] = Source.fromGraph(GraphDSL.createGraph(foldSink) {
      implicit b => fold =>
        Source(1 to 10) ~> fold
        SourceShape(b.materializedValue)
    })

    "allow exposing the materialized value as port" in {
      val (f1, f2) = foldFeedbackSource.mapAsync(4)(identity).map(_ + 100).toMat(Sink.head)(Keep.both).run()
      Await.result(f1, 3.seconds) should ===(55)
      Await.result(f2, 3.seconds) should ===(155)
    }

    "allow exposing the materialized value as port even if wrapped and the final materialized value is Unit" in {
      val noMatSource: Source[Int, Unit] =
        foldFeedbackSource.mapAsync(4)(identity).map(_ + 100).mapMaterializedValue((_) => ())
      Await.result(noMatSource.runWith(Sink.head), 3.seconds) should ===(155)
    }

    "work properly with nesting and reusing" in {
      val compositeSource1 = Source.fromGraph(GraphDSL.createGraph(foldFeedbackSource, foldFeedbackSource)(Keep.both) {
        implicit b => (s1, s2) =>
          val zip = b.add(ZipWith[Int, Int, Int](_ + _))

          s1.out.mapAsync(4)(identity) ~> zip.in0
          s2.out.mapAsync(4)(identity).map(_ * 100) ~> zip.in1
          SourceShape(zip.out)
      })

      val compositeSource2 = Source.fromGraph(GraphDSL.createGraph(compositeSource1, compositeSource1)(Keep.both) {
        implicit b => (s1, s2) =>
          val zip = b.add(ZipWith[Int, Int, Int](_ + _))
          s1.out ~> zip.in0
          s2.out.map(_ * 10000) ~> zip.in1
          SourceShape(zip.out)
      })

      val (((f1, f2), (f3, f4)), result) = compositeSource2.toMat(Sink.head)(Keep.both).run()

      Await.result(result, 3.seconds) should ===(55555555)
      Await.result(f1, 3.seconds) should ===(55)
      Await.result(f2, 3.seconds) should ===(55)
      Await.result(f3, 3.seconds) should ===(55)
      Await.result(f4, 3.seconds) should ===(55)

    }

    "work also when the source’s module is copied" in {
      val foldFlow: Flow[Int, Int, Future[Int]] = Flow.fromGraph(GraphDSL.createGraph(foldSink) {
        implicit builder => fold =>
          FlowShape(fold.in, builder.materializedValue.mapAsync(4)(identity).outlet)
      })

      Await.result(Source(1 to 10).via(foldFlow).runWith(Sink.head), 3.seconds) should ===(55)
    }

    "work also when the source’s module is copied and the graph is extended before using the matValSrc" in {
      val foldFlow: Flow[Int, Int, Future[Int]] = Flow.fromGraph(GraphDSL.createGraph(foldSink) {
        implicit builder => fold =>
          val map = builder.add(Flow[Future[Int]].mapAsync(4)(identity))
          builder.materializedValue ~> map
          FlowShape(fold.in, map.outlet)
      })

      Await.result(Source(1 to 10).via(foldFlow).runWith(Sink.head), 3.seconds) should ===(55)
    }

    "perform side-effecting transformations even when not used as source" in {
      var done = false
      val g = GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        Source.empty.mapMaterializedValue(_ => done = true) ~> Sink.ignore
        ClosedShape
      }
      val r = RunnableGraph.fromGraph(GraphDSL.createGraph(Sink.ignore) { implicit b => (s) =>
        b.add(g)
        Source(1 to 10) ~> s
        ClosedShape
      })
      r.run().futureValue should ===(akka.Done)
      done should ===(true)
    }

    "produce NotUsed when not importing materialized value" in {
      val source = Source.fromGraph(GraphDSL.create() { implicit b =>
        SourceShape(b.materializedValue)
      })
      source.runWith(Sink.seq).futureValue should ===(List(akka.NotUsed))
    }

    "produce NotUsed when starting from Flow.via" in {
      Source.empty.viaMat(Flow[Int].map(_ * 2))(Keep.right).to(Sink.ignore).run() should ===(akka.NotUsed)
    }

    "produce NotUsed when starting from Flow.via with transformation" in {
      var done = false
      Source.empty
        .viaMat(Flow[Int].via(Flow[Int].mapMaterializedValue(_ => done = true)))(Keep.right)
        .to(Sink.ignore)
        .run() should ===(akka.NotUsed)
      done should ===(true)
    }

    "ignore materialized values for a graph with no materialized values exposed" in {
      // The bug here was that the default behavior for "compose" in Module is Keep.left, but
      // EmptyModule.compose broke this by always returning the other module intact, which means
      // that the materialized value was that of the other module (which is inconsistent with Keep.left behavior).

      val sink = Sink.ignore
      val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        val s = builder.add(sink)
        val src = builder.add(Source(1 to 3))
        val flow = builder.add(Flow[Int])

        src ~> flow ~> s
        ClosedShape
      })

      g.run()
    }

    "ignore materialized values for a graph with no materialized values exposed, but keep side-effects" in {
      // The bug here was that the default behavior for "compose" in Module is Keep.left, but
      // EmptyModule.compose broke this by always returning the other module intact, which means
      // that the materialized value was that of the other module (which is inconsistent with Keep.left behavior).

      val sink = Sink.ignore.mapMaterializedValue(_ => testActor ! "side effect!")
      val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._
        val s = builder.add(sink)
        val src = builder.add(Source(1 to 3))
        val flow = builder.add(Flow[Int])

        src ~> flow ~> s
        ClosedShape
      })

      g.run()

      expectMsg("side effect!")

    }

    "with Identity Flow optimization even if ports are wired in an arbitrary higher nesting level" in {
      val subflow = GraphDSL
        .create() { implicit b =>
          import GraphDSL.Implicits._
          val zip = b.add(Zip[String, String]())
          val bc = b.add(Broadcast[String](2))

          bc.out(0) ~> zip.in0
          bc.out(1) ~> zip.in1

          FlowShape(bc.in, zip.out)
        }
        .named("nestedFlow")

      val nest1 = Flow[String].via(subflow)
      val nest2 = Flow[String].via(nest1)
      val nest3 = Flow[String].via(nest2)
      val nest4 = Flow[String].via(nest3)

      //fails
      val matValue = Source(List("")).via(nest4).to(Sink.ignore).run()

      matValue should ===(NotUsed)
    }

    "not ignore materialized value of indentity flow which is optimized away" in {
      val (m1, m2) = Source.single(1).viaMat(Flow[Int])(Keep.both).to(Sink.ignore).run()
      m1 should ===(NotUsed)
      m2 should ===(NotUsed)

      // Fails with ClassCastException if value is wrong
      val m3: Promise[Option[Int]] = Source.maybe[Int].viaMat(Flow[Int])(Keep.left).to(Sink.ignore).run()
      m3.success(None)

      val m4 = Source.single(1).viaMat(Flow[Int])(Keep.right).to(Sink.ignore).run()
      m4 should ===(NotUsed)
    }

    "build more complicated graph with flows optimized for identity flows" in {
      val flow1 = Flow.fromSinkAndSourceMat(Sink.ignore, Source.single(1).viaMat(Flow[Int])(Keep.both))(Keep.both)
      val (mA, (m1, m2)) = Source.single(8).viaMat(flow1)(Keep.right).to(Sink.ignore).run()
      Await.result(mA, 1.second) should ===(Done) //from Sink.ignore
      m1 should ===(NotUsed) //from Source.single(1)
      m2 should ===(NotUsed) //from Flow[Int]

      val flow2 = Flow.fromSinkAndSourceMat(Sink.ignore, Source.maybe[Int].viaMat(Flow[Int])(Keep.left))(Keep.both)
      val (mB, m3) = Source.single(8).viaMat(flow2)(Keep.right).to(Sink.ignore).run()
      Await.result(mB, 1.second) should ===(Done) //from Sink.ignore
      // Fails with ClassCastException if value is wrong
      m3.success(None) //from Source.maybe[Int]

      val flow3 = Flow.fromSinkAndSourceMat(Sink.ignore, Source.single(1).viaMat(Flow[Int])(Keep.right))(Keep.both)
      val (mC, m4) = Source.single(8).viaMat(flow3)(Keep.right).to(Sink.ignore).run()
      Await.result(mC, 1.second) should ===(Done) //from Sink.ignore
      m4 should ===(NotUsed) //from Flow[Int]
    }

    "provide a new materialized value for each materialization" in {
      // coverage for #23577
      val matGen = new AtomicInteger(0)
      val source = Source.empty[Int].mapMaterializedValue(_ => matGen.incrementAndGet())

      val graph = Source.fromGraph(GraphDSL.createGraph(source) { implicit b => s =>
        import GraphDSL.Implicits._
        val merge = b.add(Merge[Int](2))

        s ~> merge
        b.materializedValue ~> merge

        SourceShape(merge.out)
      })

      graph.runWith(Sink.head).futureValue should ===(1)
      graph.runWith(Sink.head).futureValue should ===(2)
    }

  }

}
