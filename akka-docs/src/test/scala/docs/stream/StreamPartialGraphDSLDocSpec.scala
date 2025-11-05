/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl._
import akka.testkit.AkkaSpec

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class StreamPartialGraphDSLDocSpec extends AkkaSpec {

  implicit val ec: ExecutionContext = system.dispatcher

  "build with open ports" in {
    //#simple-partial-graph-dsl
    val pickMaxOfThree = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip1 = b.add(ZipWith[Int, Int, Int](math.max _))
      val zip2 = b.add(ZipWith[Int, Int, Int](math.max _))
      zip1.out ~> zip2.in0

      UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    val resultSink = Sink.head[Int]

    val g = RunnableGraph.fromGraph(GraphDSL.createGraph(resultSink) { implicit b => sink =>
      import GraphDSL.Implicits._

      // importing the partial graph will return its shape (inlets & outlets)
      val pm3 = b.add(pickMaxOfThree)

      Source.single(1) ~> pm3.in(0)
      Source.single(2) ~> pm3.in(1)
      Source.single(3) ~> pm3.in(2)
      pm3.out ~> sink.in
      ClosedShape
    })

    val max: Future[Int] = g.run()
    Await.result(max, 300.millis) should equal(3)
    //#simple-partial-graph-dsl
  }

  "build source from partial graph" in {
    //#source-from-partial-graph-dsl
    val pairs = Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val zip = b.add(Zip[Int, Int]())
      def ints = Source.fromIterator(() => Iterator.from(1))

      // connect the graph
      ints.filter(_ % 2 != 0) ~> zip.in0
      ints.filter(_ % 2 == 0) ~> zip.in1

      // expose port
      SourceShape(zip.out)
    })

    val firstPair: Future[(Int, Int)] = pairs.runWith(Sink.head)
    //#source-from-partial-graph-dsl
    Await.result(firstPair, 300.millis) should equal(1 -> 2)
  }

  "build flow from partial graph" in {
    //#flow-from-partial-graph-dsl
    val pairUpWithToString =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        // prepare graph elements
        val broadcast = b.add(Broadcast[Int](2))
        val zip = b.add(Zip[Int, String]())

        // connect the graph
        broadcast.out(0).map(identity) ~> zip.in0
        broadcast.out(1).map(_.toString) ~> zip.in1

        // expose ports
        FlowShape(broadcast.in, zip.out)
      })

    //#flow-from-partial-graph-dsl

    // format: OFF
    val (_, matSink: Future[(Int, String)]) =
      //#flow-from-partial-graph-dsl
    pairUpWithToString.runWith(Source(List(1)), Sink.head)
    //#flow-from-partial-graph-dsl
    // format: ON

    Await.result(matSink, 300.millis) should equal(1 -> "1")
  }

  "combine sources with simplified API" in {
    //#source-combine
    val sourceOne = Source(List(1))
    val sourceTwo = Source(List(2))
    val merged = Source.combine(sourceOne, sourceTwo)(Merge(_))

    val mergedResult: Future[Int] = merged.runWith(Sink.fold(0)(_ + _))
    //#source-combine
    Await.result(mergedResult, 300.millis) should equal(3)
  }

  "combine sinks with simplified API" in {
    val actorRef: ActorRef = testActor
    //#sink-combine
    val sendRemotely = Sink.actorRef(actorRef, "Done", _ => "Failed")
    val localProcessing = Sink.foreach[Int](_ => /* do something useful */ ())

    val sink = Sink.combine(sendRemotely, localProcessing)(Broadcast[Int](_))

    Source(List(0, 1, 2)).runWith(sink)
    //#sink-combine
    expectMsg(0)
    expectMsg(1)
    expectMsg(2)
  }
}
