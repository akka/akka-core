/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.ClosedShape
import akka.stream.OverflowStrategy
import akka.stream.testkit._

class GraphUnzipSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  "A unzip" must {
    import GraphDSL.Implicits._

    "unzip to two subscribers" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out1 ~> Flow[String].buffer(16, OverflowStrategy.backpressure) ~> Sink.fromSubscriber(c2)
          unzip.out0 ~> Flow[Int].buffer(16, OverflowStrategy.backpressure).map(_ * 2) ~> Sink.fromSubscriber(c1)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      sub2.request(2)
      c1.expectNext(1 * 2)
      c1.expectNoMessage(100.millis)
      c2.expectNext("a")
      c2.expectNext("b")
      c2.expectNoMessage(100.millis)
      sub1.request(3)
      c1.expectNext(2 * 2)
      c1.expectNext(3 * 2)
      c1.expectComplete()
      sub2.request(3)
      c2.expectNext("c")
      c2.expectComplete()
    }

    "produce to right downstream even though left downstream cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out0 ~> Sink.fromSubscriber(c1)
          unzip.out1 ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.cancel()
      sub2.request(3)
      c2.expectNext("a")
      c2.expectNext("b")
      c2.expectNext("c")
      c2.expectComplete()
    }

    "produce to left downstream even though right downstream cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out0 ~> Sink.fromSubscriber(c1)
          unzip.out1 ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub2.cancel()
      sub1.request(3)
      c1.expectNext(1)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
    }

    "not push twice when pull is followed by cancel before element has been pushed" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out0 ~> Sink.fromSubscriber(c1)
          unzip.out1 ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub2.request(3)
      sub1.request(3)
      sub2.cancel()
      c1.expectNext(1)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
    }

    "not lose elements when pull is followed by cancel before other sink has requested" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out0 ~> Sink.fromSubscriber(c1)
          unzip.out1 ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub2.request(3)
      sub2.cancel()
      sub1.request(3)
      c1.expectNext(1)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
    }

    "cancel upstream when downstreams cancel" in {
      val p1 = TestPublisher.manualProbe[(Int, String)]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[String]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val unzip = b.add(Unzip[Int, String]())
          Source.fromPublisher(p1.getPublisher) ~> unzip.in
          unzip.out0 ~> Sink.fromSubscriber(c1)
          unzip.out1 ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val p1Sub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(3)
      sub2.request(3)
      p1.expectRequest(p1Sub, 16)
      p1Sub.sendNext(1 -> "a")
      c1.expectNext(1)
      c2.expectNext("a")
      p1Sub.sendNext(2 -> "b")
      c1.expectNext(2)
      c2.expectNext("b")
      sub1.cancel()
      sub2.cancel()
      p1Sub.expectCancellation()
    }

    "work with zip" in {
      val c1 = TestSubscriber.manualProbe[(Int, String)]()
      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val zip = b.add(Zip[Int, String]())
          val unzip = b.add(Unzip[Int, String]())
          Source(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
          unzip.out0 ~> zip.in0
          unzip.out1 ~> zip.in1
          zip.out ~> Sink.fromSubscriber(c1)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      sub1.request(5)
      c1.expectNext(1 -> "a")
      c1.expectNext(2 -> "b")
      c1.expectNext(3 -> "c")
      c1.expectComplete()
    }

  }

}
