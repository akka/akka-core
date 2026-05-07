/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._
import scala.language.postfixOps

import org.reactivestreams.Publisher

import akka.stream._
import akka.stream.testkit._
import akka.testkit.EventFilter

class GraphZipLatestWithSpec extends TwoStreamsSetup {
  import GraphDSL.Implicits._

  override type Outputs = Int

  override def fixture(b: GraphDSL.Builder[_]): Fixture = new Fixture {
    val zip = b.add(ZipWith((_: Int) + (_: Int)))
    override def left: Inlet[Int] = zip.in0
    override def right: Inlet[Int] = zip.in1
    override def out: Outlet[Int] = zip.out
  }

  override def setup(p1: Publisher[Int], p2: Publisher[Int]) = {
    setup(p1, p2, eagerComplete = true)
  }

  def setup(p1: Publisher[Int], p2: Publisher[Int], eagerComplete: Boolean) = {
    val subscriber = TestSubscriber.probe[Outputs]()
    Source
      .fromPublisher(p1)
      .zipLatestWith(Source.fromPublisher(p2), eagerComplete)(_ + _)
      .runWith(Sink.fromSubscriber(subscriber))
    subscriber
  }

  "ZipLatestWith" must {

    "work in the happy case" in {
      val upstreamProbe = TestPublisher.manualProbe[Int]()
      val downstreamProbe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zipLatest = b.add(ZipLatestWith((_: Int) + (_: Int)))
          val never = Source.single(3).initialDelay(1 day)
          Source(1 to 2).concat(never) ~> zipLatest.in0
          Source.fromPublisher(upstreamProbe) ~> zipLatest.in1
          zipLatest.out ~> Sink.fromSubscriber(downstreamProbe)
          ClosedShape
        })
        .run()

      val upstreamSubscription = upstreamProbe.expectSubscription()
      val downstreamSubscription = downstreamProbe.expectSubscription()

      upstreamSubscription.sendNext(10)
      downstreamSubscription.request(2)
      downstreamProbe.expectNext(11)
      downstreamProbe.expectNext(12)

      upstreamSubscription.sendNext(20)
      downstreamSubscription.request(1)
      downstreamProbe.expectNext(22)

      upstreamSubscription.sendNext(30)
      downstreamSubscription.request(1)
      downstreamProbe.expectNext(32)

      upstreamSubscription.sendComplete()

      downstreamProbe.expectComplete()
    }

    "work in the sad case" in {
      val probe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipLatestWith[Int, Int, Int]((_: Int) / (_: Int)))
          val never = Source.single(2).initialDelay(1 day)
          Source.single(1).concat(never) ~> zip.in0
          Source(-2 to 2) ~> zip.in1

          zip.out ~> Sink.fromSubscriber(probe)

          ClosedShape
        })
        .run()

      val subscription = probe.expectSubscription()

      subscription.request(2)
      probe.expectNext(1 / -2)
      probe.expectNext(1 / -1)

      EventFilter[ArithmeticException](occurrences = 1).intercept {
        subscription.request(2)
      }
      probe.expectError() match {
        case ae: java.lang.ArithmeticException =>
          ae.getMessage should be("/ by zero")
        case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
      }
      probe.expectNoMessage(200.millis)
    }

    "work in the non eager version" in {
      val upstreamProbe = TestPublisher.manualProbe[Int]()
      val upstreamProbe2 = TestPublisher.manualProbe[Int]()
      val downstreamProbe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zipLatest = b.add(ZipLatestWith((_: Int) + (_: Int), false))
          Source.fromPublisher(upstreamProbe) ~> zipLatest.in0
          Source.fromPublisher(upstreamProbe2) ~> zipLatest.in1
          zipLatest.out ~> Sink.fromSubscriber(downstreamProbe)
          ClosedShape
        })
        .run()

      val upstreamSubscription = upstreamProbe.expectSubscription()
      val upstreamSubscription2 = upstreamProbe2.expectSubscription()
      val downstreamSubscription = downstreamProbe.expectSubscription()

      upstreamSubscription.sendNext(10)
      upstreamSubscription2.sendNext(1)
      upstreamSubscription2.sendNext(2)

      downstreamSubscription.request(2)
      downstreamProbe.expectNext(11)
      downstreamProbe.expectNext(12)
      upstreamSubscription2.sendComplete()

      upstreamSubscription.sendNext(20)
      downstreamSubscription.request(1)
      downstreamProbe.expectNext(22)

      upstreamSubscription.sendNext(30)
      downstreamSubscription.request(1)
      downstreamProbe.expectNext(32)

      upstreamSubscription.sendComplete()

      downstreamProbe.expectComplete()
    }

    "work in the eager version" in {
      val upstreamProbe = TestPublisher.manualProbe[Int]()
      val downstreamProbe = TestSubscriber.manualProbe[Outputs]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zipLatest = b.add(ZipLatestWith((_: Int) + (_: Int), true))
          Source(1 to 2) ~> zipLatest.in0
          Source.fromPublisher(upstreamProbe) ~> zipLatest.in1
          zipLatest.out ~> Sink.fromSubscriber(downstreamProbe)
          ClosedShape
        })
        .run()

      val upstreamSubscription = upstreamProbe.expectSubscription()
      val downstreamSubscription = downstreamProbe.expectSubscription()

      upstreamSubscription.sendNext(10)
      downstreamSubscription.request(2)
      downstreamProbe.expectNext(11)
      downstreamProbe.expectNext(12)

      downstreamProbe.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 = setup(nonemptyPublisher(1 to 4), completedPublisher)
      subscriber2.expectSubscriptionAndComplete()
    }

    "work with one delayed completed and one nonempty publisher" in {
      val subscriber1 =
        setup(soonToCompletePublisher, nonemptyPublisher(1 to 4))
      subscriber1.expectSubscriptionAndComplete()

      val subscriber2 =
        setup(nonemptyPublisher(1 to 4), soonToCompletePublisher)
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

    "zipLatestWith a ETA expanded Person.apply (3 inputs)" in {
      val upstreamProbe = TestPublisher.manualProbe[Int]()
      val downstreamProbe = TestSubscriber.manualProbe[Person]()

      case class Person(name: String, surname: String, int: Int)

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(ZipLatestWith(Person.apply _))

          Source.single("Caplin") ~> zip.in0
          Source.single("Capybara") ~> zip.in1
          Source.fromPublisher(upstreamProbe).take(1) ~> zip.in2

          zip.out ~> Sink.fromSubscriber(downstreamProbe)

          ClosedShape
        })
        .run()

      val downstreamSubscription = downstreamProbe.expectSubscription()
      val upstreamSubscription = upstreamProbe.expectSubscription()

      downstreamSubscription.request(1)
      upstreamSubscription.sendNext(3)
      downstreamProbe.expectNext(Person("Caplin", "Capybara", 3))
      downstreamProbe.expectComplete()
    }

    "work with up to 22 inputs" in {
      val downstreamProbe = TestSubscriber.manualProbe[String]()
      val upstreamProbe = TestPublisher.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val sum22 = (
              v1: Int,
              v2: String,
              v3: Int,
              v4: String,
              v5: Int,
              v6: String,
              v7: Int,
              v8: String,
              v9: Int,
              v10: String,
              v11: Int,
              v12: String,
              v13: Int,
              v14: String,
              v15: Int,
              v16: String,
              v17: Int,
              v18: String,
              v19: Int,
              v20: String,
              v21: Int,
              v22: String) => s"$v1$v2$v3$v4$v5$v6$v7$v8$v9$v10$v11$v12$v13$v14$v15$v16$v17$v18$v19$v20$v21$v22"

          // odd input ports will be Int, even input ports will be String
          val zip = b.add(ZipLatestWith(sum22))

          Source.single(1) ~> zip.in0
          Source.single(2).map(_.toString) ~> zip.in1
          Source.single(3) ~> zip.in2
          Source.single(4).map(_.toString) ~> zip.in3
          Source.single(5) ~> zip.in4
          Source.single(6).map(_.toString) ~> zip.in5
          Source.single(7) ~> zip.in6
          Source.single(8).map(_.toString) ~> zip.in7
          Source.single(9) ~> zip.in8
          Source.single(10).map(_.toString) ~> zip.in9
          Source.single(11) ~> zip.in10
          Source.single(12).map(_.toString) ~> zip.in11
          Source.single(13) ~> zip.in12
          Source.single(14).map(_.toString) ~> zip.in13
          Source.single(15) ~> zip.in14
          Source.single(16).map(_.toString) ~> zip.in15
          Source.single(17) ~> zip.in16
          Source.single(18).map(_.toString) ~> zip.in17
          Source.single(19) ~> zip.in18
          Source.single(20).map(_.toString) ~> zip.in19
          Source.single(21) ~> zip.in20
          Source.fromPublisher(upstreamProbe).map(_.toString) ~> zip.in21

          zip.out ~> Sink.fromSubscriber(downstreamProbe)

          ClosedShape
        })
        .run()

      val downstreamSubscription = downstreamProbe.expectSubscription()
      val upstreamSubscription = upstreamProbe.expectSubscription()

      downstreamSubscription.request(1)
      upstreamSubscription.sendNext(22)
      upstreamSubscription.sendComplete()
      downstreamProbe.expectNext((1 to 22).mkString(""))
      downstreamProbe.expectComplete()

    }

  }

}
