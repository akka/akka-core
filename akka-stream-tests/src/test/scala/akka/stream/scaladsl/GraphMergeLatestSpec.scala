/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._
import akka.stream.testkit.scaladsl.TestSource

class GraphMergeLatestSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = List[Int]

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val merge = b.add(MergeLatest[Int](2))

    override def left: Inlet[Int] = merge.in(0)
    override def right: Inlet[Int] = merge.in(1)
    override def out: Outlet[Outputs] = merge.out

  }

  "mergeLatest" must {

    "start emit values only after each input stream emitted value" in assertAllStagesStopped {
      val up1 = TestSource[Int]()
      val up2 = TestSource[Int]()
      val up3 = TestSource[Int]()
      val probe = TestSubscriber.manualProbe[List[Int]]()

      val (in1, in2, in3) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(up1, up2, up3)((_, _, _)) { implicit b => (s1, s2, s3) =>
          val m = b.add(MergeLatest[Int](3))

          s1 ~> m
          s2 ~> m
          s3 ~> m
          m.out ~> Sink.fromSubscriber(probe)
          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(1)
      probe.expectNoMessage(10.millis)
      in1.sendNext(1)
      probe.expectNoMessage(10.millis)
      in2.sendNext(2)
      probe.expectNoMessage(10.millis)
      in3.sendNext(3)
      probe.expectNext(List(1, 2, 3))
      in1.sendComplete()
      in2.sendComplete()
      in3.sendComplete()
      probe.expectComplete()
    }

    "update values after message from one stream" in assertAllStagesStopped {
      val up1 = TestSource[Int]()
      val up2 = TestSource[Int]()
      val up3 = TestSource[Int]()
      val probe = TestSubscriber.manualProbe[List[Int]]()

      val (in1, in2, in3) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(up1, up2, up3)((_, _, _)) { implicit b => (s1, s2, s3) =>
          val m = b.add(MergeLatest[Int](3))

          s1 ~> m
          s2 ~> m
          s3 ~> m
          m.out ~> Sink.fromSubscriber(probe)
          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      in1.sendNext(1)
      in2.sendNext(2)
      in3.sendNext(3)
      subscription.request(1)
      probe.expectNext() should be(List(1, 2, 3))

      in1.sendNext(2)
      subscription.request(1)
      probe.expectNext() should be(List(2, 2, 3))

      in2.sendNext(4)
      subscription.request(1)
      probe.expectNext() should be(List(2, 4, 3))

      in3.sendNext(6)
      subscription.request(1)
      probe.expectNext() should be(List(2, 4, 6))

      in3.sendNext(9)
      subscription.request(1)
      probe.expectNext() should be(List(2, 4, 9))

      in1.sendNext(4)
      subscription.request(1)
      probe.expectNext() should be(List(4, 4, 9))

      in1.sendComplete()
      in2.sendComplete()
      in3.sendComplete()
      probe.expectComplete()
    }

    "work with one-way merge" in {
      val result = Source
        .fromGraph(GraphDSL.create() { implicit b =>
          val merge = b.add(MergeLatest[Int](1))
          val source = b.add(Source(1 to 3))

          source ~> merge
          SourceShape(merge.out)
        })
        .runFold(Seq[List[Int]]())(_ :+ _)

      Await.result(result, 3.seconds) should ===(Seq(List(1), List(2), List(3)))
    }

    "complete stage if eagerComplete is set and one of input stream finished" in assertAllStagesStopped {
      val up1 = TestSource[Int]()
      val up2 = TestSource[Int]()
      val probe = TestSubscriber.manualProbe[List[Int]]()

      val (in1, _) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(up1, up2)((_, _)) { implicit b => (s1, s2) =>
          val m = b.add(MergeLatest[Int](2, true))

          s1 ~> m
          s2 ~> m
          m.out ~> Sink.fromSubscriber(probe)
          ClosedShape
        })
        .run()

      probe.expectSubscription()

      in1.sendComplete()
      probe.expectComplete()
    }

    commonTests()

  }

}
