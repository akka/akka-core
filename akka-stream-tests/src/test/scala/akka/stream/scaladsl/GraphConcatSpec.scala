/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Promise

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.StreamTestKit._

class GraphConcatSpec extends TwoStreamsSetup {

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val concat = b.add(Concat[Outputs]())

    override def left: Inlet[Outputs] = concat.in(0)
    override def right: Inlet[Outputs] = concat.in(1)
    override def out: Outlet[Outputs] = concat.out

  }

  "Concat" must {
    import GraphDSL.Implicits._

    "work in the happy case" in assertAllStagesStopped {
      val probe = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val concat1 = b.add(Concat[Int]())
          val concat2 = b.add(Concat[Int]())

          Source(List.empty[Int]) ~> concat1.in(0)
          Source(1 to 4) ~> concat1.in(1)

          concat1.out ~> concat2.in(0)
          Source(5 to 10) ~> concat2.in(1)

          concat2.out ~> Sink.fromSubscriber(probe)
          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      for (i <- 1 to 10) {
        subscription.request(1)
        probe.expectNext(i)
      }

      probe.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(5)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(5)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one delayed completed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(5)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(5)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one immediately failed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber1 = setup(failedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndError(TestException)

      val subscriber2 = setup(nonemptyPublisher(1 to 4), failedPublisher)
      subscriber2.expectSubscription().request(5)

      var errorSignalled = false
      if (!errorSignalled) errorSignalled ||= subscriber2.expectNextOrError(1, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber2.expectNextOrError(2, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber2.expectNextOrError(3, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber2.expectNextOrError(4, TestException).isLeft
      if (!errorSignalled) subscriber2.expectError(TestException)
    }

    "work with one nonempty and one delayed failed publisher" in assertAllStagesStopped {
      val subscriber = setup(nonemptyPublisher(1 to 4), soonToFailPublisher)
      subscriber.expectSubscription().request(5)

      var errorSignalled = false
      if (!errorSignalled) errorSignalled ||= subscriber.expectNextOrError(1, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber.expectNextOrError(2, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber.expectNextOrError(3, TestException).isLeft
      if (!errorSignalled) errorSignalled ||= subscriber.expectNextOrError(4, TestException).isLeft
      if (!errorSignalled) subscriber.expectError(TestException)
    }

    "work with one delayed failed and one nonempty publisher" in assertAllStagesStopped {
      val subscriber = setup(soonToFailPublisher, nonemptyPublisher(1 to 4))
      subscriber.expectSubscriptionAndError(TestException)
    }

    "correctly handle async errors in secondary upstream" in assertAllStagesStopped {
      val promise = Promise[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val concat = b.add(Concat[Int]())
          Source(List(1, 2, 3)) ~> concat.in(0)
          Source.future(promise.future) ~> concat.in(1)
          concat.out ~> Sink.fromSubscriber(subscriber)
          ClosedShape
        })
        .run()

      val subscription = subscriber.expectSubscription()
      subscription.request(4)
      subscriber.expectNext(1)
      subscriber.expectNext(2)
      subscriber.expectNext(3)
      promise.failure(TestException)
      subscriber.expectError(TestException)
    }
  }
}
