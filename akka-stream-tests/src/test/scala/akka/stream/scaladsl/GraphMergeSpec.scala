/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class GraphMergeSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val merge = b.add(Merge[Outputs](2))

    override def left: Inlet[Outputs] = merge.in(0)
    override def right: Inlet[Outputs] = merge.in(1)
    override def out: Outlet[Outputs] = merge.out

  }

  "merge" must {

    "work in the happy case" in assertAllStagesStopped {
      // Different input sizes (4 and 6)
      val source1 = Source(0 to 3)
      val source2 = Source(4 to 9)
      val source3 = Source(List[Int]())
      val probe = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val m1 = b.add(Merge[Int](2))
          val m2 = b.add(Merge[Int](2))

          source1 ~> m1.in(0)
          m1.out ~> Flow[Int].map(_ * 2) ~> m2.in(0)
          m2.out ~> Flow[Int].map(_ / 2).map(_ + 1) ~> Sink.fromSubscriber(probe)
          source2 ~> m1.in(1)
          source3 ~> m2.in(1)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      var collected = Seq.empty[Int]
      for (_ <- 1 to 10) {
        subscription.request(1)
        collected :+= probe.expectNext()
      }
      //test ordering of elements coming from each of nonempty flows
      collected.filter(_ <= 4) should ===(1 to 4)
      collected.filter(_ >= 5) should ===(5 to 10)

      collected.toSet should be(Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      probe.expectComplete()
    }

    "work with one-way merge" in {
      val result = Source
        .fromGraph(GraphDSL.create() { implicit b =>
          val merge = b.add(Merge[Int](1))
          val source = b.add(Source(1 to 3))

          source ~> merge.in(0)
          SourceShape(merge.out)
        })
        .runFold(Seq[Int]())(_ :+ _)

      Await.result(result, 3.seconds) should ===(Seq(1, 2, 3))
    }

    "work with n-way merge" in {
      val source1 = Source(List(1))
      val source2 = Source(List(2))
      val source3 = Source(List(3))
      val source4 = Source(List(4))
      val source5 = Source(List(5))
      val source6 = Source(List[Int]())

      val probe = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val merge = b.add(Merge[Int](6))

          source1 ~> merge.in(0)
          source2 ~> merge.in(1)
          source3 ~> merge.in(2)
          source4 ~> merge.in(3)
          source5 ~> merge.in(4)
          source6 ~> merge.in(5)
          merge.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      var collected = Set.empty[Int]
      for (_ <- 1 to 5) {
        subscription.request(1)
        collected += probe.expectNext()
      }

      collected should be(Set(1, 2, 3, 4, 5))
      probe.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one delayed completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one immediately failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

    "work with one delayed failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

    "pass along early cancellation" in assertAllStagesStopped {
      val up1 = TestPublisher.manualProbe[Int]()
      val up2 = TestPublisher.manualProbe[Int]()
      val down = TestSubscriber.manualProbe[Int]()

      val src1 = Source.asSubscriber[Int]
      val src2 = Source.asSubscriber[Int]

      val (graphSubscriber1, graphSubscriber2) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(src1, src2)((_, _)) { implicit b => (s1, s2) =>
          val merge = b.add(Merge[Int](2))
          s1.out ~> merge.in(0)
          s2.out ~> merge.in(1)
          merge.out ~> Sink.fromSubscriber(down)
          ClosedShape
        })
        .run()

      val downstream = down.expectSubscription()
      downstream.cancel()
      up1.subscribe(graphSubscriber1)
      up2.subscribe(graphSubscriber2)
      val upsub1 = up1.expectSubscription()
      upsub1.expectCancellation()
      val upsub2 = up2.expectSubscription()
      upsub2.expectCancellation()
    }

  }

}
