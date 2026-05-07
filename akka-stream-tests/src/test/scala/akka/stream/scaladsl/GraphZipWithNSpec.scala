/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.testkit.EventFilter

class GraphZipWithNSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val zip = b.add(ZipWithN((_: immutable.Seq[Int]).sum)(2))
    override def left: Inlet[Int] = zip.in(0)
    override def right: Inlet[Int] = zip.in(1)
    override def out: Outlet[Int] = zip.out
  }

  "ZipWithN" must {

    "work in the happy case" in {
      val probe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipWithN((_: immutable.Seq[Int]).sum)(2))
          Source(1 to 4) ~> zip.in(0)
          Source(10 to 40 by 10) ~> zip.in(1)

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext(11)
      probe.expectNext(22)

      subscription.request(1)
      probe.expectNext(33)
      subscription.request(1)
      probe.expectNext(44)

      probe.expectComplete()
    }

    "work in the sad case" in {
      val probe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipWithN((_: immutable.Seq[Int]).foldLeft(1)(_ / _))(2))

          Source(1 to 4) ~> zip.in(0)
          Source(-2 to 2) ~> zip.in(1)

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext(1 / 1 / -2)
      probe.expectNext(1 / 2 / -1)

      EventFilter[ArithmeticException](occurrences = 1).intercept {
        subscription.request(2)
      }
      probe.expectError() match {
        case ae: java.lang.ArithmeticException => ae.getMessage should be("/ by zero")
        case unexpected                        => throw new RuntimeException(s"Unexpected: $unexpected")
      }
      probe.expectNoMessage(200.millis)
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      subscriber2.expectSubscriptionAndComplete()
    }

    "work with one delayed completed and one nonempty publisher" in {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
      subscriber2.expectSubscriptionAndComplete()
    }

    "work with one immediately failed and one nonempty publisher" in {
      val subscriber1 = setup(failedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndError(TestException)

      val subscriber2 = setup(nonemptyPublisher(1 to 4), failedPublisher)
      subscriber2.expectSubscriptionAndError(TestException)
    }

    "work with one delayed failed and one nonempty publisher" in {
      val subscriber1 = setup(soonToFailPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndError(TestException)

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToFailPublisher)
      subscriber2.expectSubscriptionAndError(TestException)
    }

    "work with 3 inputs" in {
      val probe = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipWithN((_: immutable.Seq[Int]).sum)(3))

          Source.single(1) ~> zip.in(0)
          Source.single(2) ~> zip.in(1)
          Source.single(3) ~> zip.in(2)

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(5)
      probe.expectNext(6)

      probe.expectComplete()
    }

    "work with 30 inputs" in {
      val probe = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipWithN((_: immutable.Seq[Int]).sum)(30))

          (0 to 29).foreach { n =>
            Source.single(n) ~> zip.in(n)
          }

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(1)
      probe.expectNext((0 to 29).sum)

      probe.expectComplete()

    }

  }

}
