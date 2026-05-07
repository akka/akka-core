/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class GraphZipNSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = immutable.Seq[Int]

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val zipN = b.add(ZipN[Int](2))

    override def left: Inlet[Int] = zipN.in(0)
    override def right: Inlet[Int] = zipN.in(1)
    override def out: Outlet[immutable.Seq[Int]] = zipN.out
  }

  "ZipN" must {

    "work in the happy case" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zipN = b.add(ZipN[Int](2))

          Source(1 to 4) ~> zipN.in(0)
          Source(2 to 5) ~> zipN.in(1)

          zipN.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext(immutable.Seq(1, 2))
      probe.expectNext(immutable.Seq(2, 3))

      subscription.request(1)
      probe.expectNext(immutable.Seq(3, 4))
      subscription.request(1)
      probe.expectNext(immutable.Seq(4, 5))

      probe.expectComplete()
    }

    "complete if one side is available but other already completed" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zipN = b.add(ZipN[Int](2))

          Source.fromPublisher(upstream1) ~> zipN.in(0)
          Source.fromPublisher(upstream2) ~> zipN.in(1)
          zipN.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream1.sendNext(2)
      upstream2.sendNext(2)
      upstream2.sendComplete()

      downstream.requestNext(immutable.Seq(1, 2))
      downstream.expectComplete()
      upstream1.expectCancellation()
    }

    "complete even if no pending demand" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zipN = b.add(ZipN[Int](2))

          Source.fromPublisher(upstream1) ~> zipN.in(0)
          Source.fromPublisher(upstream2) ~> zipN.in(1)
          zipN.out ~> out

          ClosedShape
        })
        .run()

      downstream.request(1)

      upstream1.sendNext(1)
      upstream2.sendNext(2)
      downstream.expectNext(immutable.Seq(1, 2))

      upstream2.sendComplete()
      downstream.expectComplete()
      upstream1.expectCancellation()
    }

    "complete if both sides complete before requested with elements pending" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zipN = b.add(ZipN[Int](2))

          Source.fromPublisher(upstream1) ~> zipN.in(0)
          Source.fromPublisher(upstream2) ~> zipN.in(1)
          zipN.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream2.sendNext(2)

      upstream1.sendComplete()
      upstream2.sendComplete()

      downstream.requestNext(immutable.Seq(1, 2))
      downstream.expectComplete()
    }

    "complete if one side complete before requested with elements pending" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zipN = b.add(ZipN[Int](2))

          Source.fromPublisher(upstream1) ~> zipN.in(0)
          Source.fromPublisher(upstream2) ~> zipN.in(1)
          zipN.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream1.sendNext(2)
      upstream2.sendNext(2)

      upstream1.sendComplete()
      upstream2.sendComplete()

      downstream.requestNext(immutable.Seq(1, 2))
      downstream.expectComplete()
    }

    "complete if one side complete before requested with elements pending 2" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zipN = b.add(ZipN[Int](2))

          Source.fromPublisher(upstream1) ~> zipN.in(0)
          Source.fromPublisher(upstream2) ~> zipN.in(1)
          zipN.out ~> out

          ClosedShape
        })
        .run()

      downstream.ensureSubscription()

      upstream1.sendNext(1)
      upstream1.sendComplete()
      downstream.expectNoMessage(500.millis)

      upstream2.sendNext(2)
      upstream2.sendComplete()
      downstream.requestNext(immutable.Seq(1, 2))
      downstream.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      subscriber2.expectSubscriptionAndComplete()
    }

    "work with one delayed completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
      subscriber2.expectSubscriptionAndComplete()
    }

    "work with one immediately failed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(failedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndError(TestException)

      val subscriber2 = setup(nonemptyPublisher(1 to 4), failedPublisher)
      subscriber2.expectSubscriptionAndError(TestException)
    }

    "work with one delayed failed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(soonToFailPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndError(TestException)

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToFailPublisher)
      subscriber2.expectSubscriptionAndError(TestException)
    }

  }

}
