/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class GraphMergeSequenceSpec extends TwoStreamsSetup {

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val merge = b.add(MergeSequence[Outputs](2)(i => i))

    override def left: Inlet[Outputs] = merge.in(0)
    override def right: Inlet[Outputs] = merge.in(1)
    override def out: Outlet[Outputs] = merge.out

  }

  private def merge(seqs: immutable.Seq[Long]*): immutable.Seq[Long] =
    mergeSources(seqs.map(Source(_)): _*)

  private def mergeSources(sources: Source[Long, NotUsed]*): immutable.Seq[Long] = {
    val future = Source
      .fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        val merge = builder.add(new MergeSequence[Long](sources.size)(identity))
        sources.foreach { source =>
          source ~> merge
        }

        SourceShape(merge.out)
      })
      .runWith(Sink.seq)
    Await.result(future, 3.seconds)
  }

  "merge sequence" must {

    "merge interleaved streams" in assertAllStagesStopped {
      (merge(List(0L, 4L, 8L, 9L, 11L), List(1L, 3L, 6L, 10L, 13L), List(2L, 5L, 7L, 12L)) should contain)
        .theSameElementsInOrderAs(0L to 13L)
    }

    "merge non interleaved streams" in assertAllStagesStopped {
      (merge(List(5L, 6L, 7L, 8L, 9L), List(0L, 1L, 2L, 3L, 4L), List(10L, 11L, 12L, 13L)) should contain)
        .theSameElementsInOrderAs(0L to 13L)
    }

    "fail on duplicate sequence numbers" in assertAllStagesStopped {
      an[IllegalStateException] should be thrownBy merge(List(0L, 1L, 2L), List(2L))
    }

    "fail on missing sequence numbers" in assertAllStagesStopped {
      an[IllegalStateException] should be thrownBy merge(
        List(0L, 4L, 8L, 9L, 11L),
        List(1L, 3L, 10L, 13L),
        List(2L, 5L, 7L, 12L))
    }

    "fail on missing sequence numbers if some streams have completed" in assertAllStagesStopped {
      an[IllegalStateException] should be thrownBy merge(
        List(0L, 4L, 8L, 9L, 11L),
        List(1L, 3L, 6L, 10L, 13L, 15L),
        List(2L, 5L, 7L, 12L))
    }

    "fail on sequence regression in a single stream" in assertAllStagesStopped {
      an[IllegalStateException] should be thrownBy merge(
        List(0L, 4L, 8L, 7L, 9L, 11L),
        List(1L, 3L, 6L, 10L, 13L),
        List(2L, 5L, 7L, 12L))
    }

    commonTests()
  }

}
