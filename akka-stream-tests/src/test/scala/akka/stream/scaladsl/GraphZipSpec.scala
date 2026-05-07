/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class GraphZipSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = (Int, Int)

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val zip = b.add(Zip[Int, Int]())

    override def left: Inlet[Int] = zip.in0
    override def right: Inlet[Int] = zip.in1
    override def out: Outlet[(Int, Int)] = zip.out
  }

  "Zip" must {

    "work in the happy case" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[(Int, String)]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(Zip[Int, String]())

          Source(1 to 4) ~> zip.in0
          Source(List("A", "B", "C", "D", "E", "F")) ~> zip.in1

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext((1, "A"))
      probe.expectNext((2, "B"))

      subscription.request(1)
      probe.expectNext((3, "C"))
      subscription.request(1)
      probe.expectNext((4, "D"))

      probe.expectComplete()
    }

    "complete if one side is available but other already completed" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[String]()

      val completed = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.ignore) { implicit b => out =>
          val zip = b.add(Zip[Int, String]())

          Source.fromPublisher(upstream1) ~> zip.in0
          Source.fromPublisher(upstream2) ~> zip.in1
          zip.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream1.sendNext(2)
      upstream2.sendNext("A")
      upstream2.sendComplete()

      Await.ready(completed, 3.seconds)
      upstream1.expectCancellation()

    }

    "complete even if no pending demand" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[(Int, String)]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zip = b.add(Zip[Int, String]())

          Source.fromPublisher(upstream1) ~> zip.in0
          Source.fromPublisher(upstream2) ~> zip.in1
          zip.out ~> out

          ClosedShape
        })
        .run()

      downstream.request(1)

      upstream1.sendNext(1)
      upstream2.sendNext("A")
      downstream.expectNext((1, "A"))

      upstream2.sendComplete()
      downstream.expectComplete()
      upstream1.expectCancellation()
    }

    "complete if both sides complete before requested with elements pending" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[(Int, String)]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zip = b.add(Zip[Int, String]())

          Source.fromPublisher(upstream1) ~> zip.in0
          Source.fromPublisher(upstream2) ~> zip.in1
          zip.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream2.sendNext("A")

      upstream1.sendComplete()
      upstream2.sendComplete()

      downstream.requestNext((1, "A"))
      downstream.expectComplete()
    }

    "complete if one side complete before requested with elements pending" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[(Int, String)]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zip = b.add(Zip[Int, String]())

          Source.fromPublisher(upstream1) ~> zip.in0
          Source.fromPublisher(upstream2) ~> zip.in1
          zip.out ~> out

          ClosedShape
        })
        .run()

      upstream1.sendNext(1)
      upstream1.sendNext(2)
      upstream2.sendNext("A")

      upstream1.sendComplete()
      upstream2.sendComplete()

      downstream.requestNext((1, "A"))
      downstream.expectComplete()
    }

    "complete if one side complete before requested with elements pending 2" in {
      val upstream1 = TestPublisher.probe[Int]()
      val upstream2 = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[(Int, String)]()

      RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.fromSubscriber(downstream)) { implicit b => out =>
          val zip = b.add(Zip[Int, String]())

          Source.fromPublisher(upstream1) ~> zip.in0
          Source.fromPublisher(upstream2) ~> zip.in1
          zip.out ~> out

          ClosedShape
        })
        .run()

      downstream.ensureSubscription()

      upstream1.sendNext(1)
      upstream1.sendComplete()
      downstream.expectNoMessage(500.millis)

      upstream2.sendNext("A")
      upstream2.sendComplete()
      downstream.requestNext((1, "A"))
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
