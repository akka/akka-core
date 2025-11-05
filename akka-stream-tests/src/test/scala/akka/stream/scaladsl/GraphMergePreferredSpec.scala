/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit.TwoStreamsSetup

@nowarn // stream usage
class GraphMergePreferredSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val merge = b.add(MergePreferred[Outputs](1))

    override def left: Inlet[Outputs] = merge.preferred
    override def right: Inlet[Outputs] = merge.in(0)
    override def out: Outlet[Outputs] = merge.out

  }

  "preferred merge" must {
    commonTests()

    "prefer selected input more than others" in {
      val numElements = 10000

      val preferred = Source(Stream.fill(numElements)(1))
      val aux = Source(Stream.fill(numElements)(2))

      val result = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.head[Seq[Int]]) { implicit b => sink =>
          val merge = b.add(MergePreferred[Int](3))
          preferred ~> merge.preferred

          merge.out.grouped(numElements * 2) ~> sink.in
          aux ~> merge.in(0)
          aux ~> merge.in(1)
          aux ~> merge.in(2)
          ClosedShape
        })
        .run()

      Await.result(result, 3.seconds).filter(_ == 1).size should be(numElements)
    }

    "eventually pass through all elements without corrupting the ordering" in {
      val result = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.head[Seq[Int]]) { implicit b => sink =>
          val merge = b.add(MergePreferred[Int](3))
          Source(1 to 100) ~> merge.preferred

          merge.out.grouped(500) ~> sink.in
          Source(101 to 200) ~> merge.in(0)
          Source(201 to 300) ~> merge.in(1)
          Source(301 to 400) ~> merge.in(2)
          ClosedShape
        })
        .run()

      val resultSeq = Await.result(result, 3.seconds)
      resultSeq.toSet should ===((1 to 400).toSet)
      //test ordering of elements coming from each of the flows
      resultSeq.filter(_ <= 100) should ===(1 to 100)
      resultSeq.filter(e => e > 100 && e <= 200) should ===(101 to 200)
      resultSeq.filter(e => e > 200 && e <= 300) should ===(201 to 300)
      resultSeq.filter(e => e > 300 && e <= 400) should ===(301 to 400)
    }

    "disallow multiple preferred inputs" in {
      val s = Source(0 to 3)

      (the[IllegalArgumentException] thrownBy {
        RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
          val merge = b.add(MergePreferred[Int](1))

          s ~> merge.preferred
          s ~> merge.preferred
          s ~> merge.in(0)

          merge.out ~> Sink.head[Int]
          ClosedShape
        })
      }).getMessage should include("[MergePreferred.preferred] is already connected")
    }

    "disallow multiple outputs" in {
      val s = Source(0 to 3)

      (the[IllegalArgumentException] thrownBy {
        RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
          val merge = b.add(MergePreferred[Int](1))

          s ~> merge.preferred
          s ~> merge.in(0)

          merge.out ~> Sink.head[Int]
          merge.out ~> Sink.head[Int]
          ClosedShape
        })
      }).getMessage should include("[MergePreferred.out] is already connected")
    }

  }

}
