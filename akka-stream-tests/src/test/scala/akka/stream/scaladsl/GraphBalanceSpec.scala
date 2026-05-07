/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl._

class GraphBalanceSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  "A balance" must {
    import GraphDSL.Implicits._

    "balance between subscribers which signal demand" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](2))
          Source(List(1, 2, 3)) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()

      sub1.request(1)
      c1.expectNext(1)
      c1.expectNoMessage(100.millis)

      sub2.request(2)
      c2.expectNext(2)
      c2.expectNext(3)
      c1.expectComplete()
      c2.expectComplete()
    }

    "support waiting for demand from all downstream subscriptions" in {
      val s1 = TestSubscriber.manualProbe[Int]()
      val p2 = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.asPublisher[Int](false)) { implicit b => p2Sink =>
          val balance = b.add(Balance[Int](2, waitForAllDownstreams = true))
          Source(List(1, 2, 3)) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(s1)
          balance.out(1) ~> p2Sink
          ClosedShape
        })
        .run()

      val sub1 = s1.expectSubscription()
      sub1.request(1)
      s1.expectNoMessage(200.millis)

      val s2 = TestSubscriber.manualProbe[Int]()
      p2.subscribe(s2)
      val sub2 = s2.expectSubscription()

      // still no demand from s2
      s1.expectNoMessage(200.millis)

      sub2.request(2)
      s1.expectNext(1)
      s2.expectNext(2)
      s2.expectNext(3)
      s1.expectComplete()
      s2.expectComplete()
    }

    "support waiting for demand from all non-cancelled downstream subscriptions" in {
      val s1 = TestSubscriber.manualProbe[Int]()

      val (p2, p3) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.asPublisher[Int](false), Sink.asPublisher[Int](false))(Keep.both) {
          implicit b => (p2Sink, p3Sink) =>
            val balance = b.add(Balance[Int](3, waitForAllDownstreams = true))
            Source(List(1, 2, 3)) ~> balance.in
            balance.out(0) ~> Sink.fromSubscriber(s1)
            balance.out(1) ~> p2Sink
            balance.out(2) ~> p3Sink
            ClosedShape
        })
        .run()

      val sub1 = s1.expectSubscription()
      sub1.request(1)

      val s2 = TestSubscriber.manualProbe[Int]()
      p2.subscribe(s2)
      val sub2 = s2.expectSubscription()

      val s3 = TestSubscriber.manualProbe[Int]()
      p3.subscribe(s3)
      val sub3 = s3.expectSubscription()

      sub2.request(2)
      s1.expectNoMessage(200.millis)
      sub3.cancel()

      s1.expectNext(1)
      s2.expectNext(2)
      s2.expectNext(3)
      s1.expectComplete()
      s2.expectComplete()
    }

    "work with one-way merge" in {
      val result = Source
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](1))
          val source = b.add(Source(1 to 3))

          source ~> balance.in
          SourceShape(balance.out(0))
        })
        .runFold(Seq[Int]())(_ :+ _)

      Await.result(result, 3.seconds) should ===(Seq(1, 2, 3))
    }

    "work with 5-way balance" in {

      val sink = Sink.head[Seq[Int]]
      val (s1, s2, s3, s4, s5) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(sink, sink, sink, sink, sink)(Tuple5.apply) {
          implicit b => (f1, f2, f3, f4, f5) =>
            val balance = b.add(Balance[Int](5, waitForAllDownstreams = true))
            Source(0 to 14) ~> balance.in
            balance.out(0).grouped(15) ~> f1
            balance.out(1).grouped(15) ~> f2
            balance.out(2).grouped(15) ~> f3
            balance.out(3).grouped(15) ~> f4
            balance.out(4).grouped(15) ~> f5
            ClosedShape
        })
        .run()

      Set(s1, s2, s3, s4, s5).flatMap(Await.result(_, 3.seconds)) should be((0 to 14).toSet)
    }

    "balance between all three outputs" in {
      val numElementsForSink = 10000
      val outputs = Sink.fold[Int, Int](0)(_ + _)

      val results = RunnableGraph
        .fromGraph(GraphDSL.createGraph(outputs, outputs, outputs)(List(_, _, _)) { implicit b => (o1, o2, o3) =>
          val balance = b.add(Balance[Int](3, waitForAllDownstreams = true))
          Source.repeat(1).take(numElementsForSink * 3) ~> balance.in
          balance.out(0) ~> o1
          balance.out(1) ~> o2
          balance.out(2) ~> o3
          ClosedShape
        })
        .run()

      import system.dispatcher
      val sum = Future.sequence(results).map { res =>
        res should not contain 0
        res.sum
      }
      Await.result(sum, 3.seconds) should be(numElementsForSink * 3)
    }

    "fairly balance between three outputs" in {
      val probe = TestSink[Int]()
      val (p1, p2, p3) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(probe, probe, probe)(Tuple3.apply) { implicit b => (o1, o2, o3) =>
          val balance = b.add(Balance[Int](3))
          Source(1 to 7) ~> balance.in
          balance.out(0) ~> o1
          balance.out(1) ~> o2
          balance.out(2) ~> o3
          ClosedShape
        })
        .run()

      p1.requestNext(1)
      p2.requestNext(2)
      p3.requestNext(3)
      p2.requestNext(4)
      p1.requestNext(5)
      p3.requestNext(6)
      p1.requestNext(7)

      p1.expectComplete()
      p2.expectComplete()
      p3.expectComplete()
    }

    "produce to second even though first cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](2))
          Source(List(1, 2, 3)) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      sub1.cancel()
      val sub2 = c2.expectSubscription()
      sub2.request(3)
      c2.expectNext(1)
      c2.expectNext(2)
      c2.expectNext(3)
      c2.expectComplete()
    }

    "produce to first even though second cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](2))
          Source(List(1, 2, 3)) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
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

    "cancel upstream when all downstreams cancel if eagerCancel is false" in {
      val p1 = TestPublisher.manualProbe[Int]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](2))
          Source.fromPublisher(p1.getPublisher) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()

      sub1.request(1)
      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)
      c1.expectNext(1)

      sub2.request(1)
      bsub.sendNext(2)
      c2.expectNext(2)

      sub1.cancel()
      sub2.cancel()
      bsub.expectCancellation()
    }

    "cancel upstream when any downstream cancel if eagerCancel is true" in {
      val p1 = TestPublisher.manualProbe[Int]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(new Balance[Int](2, waitForAllDownstreams = false, eagerCancel = true))
          Source.fromPublisher(p1.getPublisher) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()

      sub1.request(1)
      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)
      c1.expectNext(1)

      sub2.request(1)
      bsub.sendNext(2)
      c2.expectNext(2)

      sub1.cancel()
      bsub.expectCancellation()
    }

    // Bug #20943
    "not push output twice" in {
      val p1 = TestPublisher.manualProbe[Int]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](2))
          Source.fromPublisher(p1.getPublisher) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()

      sub1.request(1)
      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)
      c1.expectNext(1)

      sub2.request(1)
      sub2.cancel()
      bsub.sendNext(2)

      sub1.cancel()
      bsub.expectCancellation()
    }

    // Bug #25387
    "not dequeue from empty outlet buffer" in {
      val p1 = TestPublisher.manualProbe[Int]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()
      val c3 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val balance = b.add(Balance[Int](3))
          Source.fromPublisher(p1.getPublisher) ~> balance.in
          balance.out(0) ~> Sink.fromSubscriber(c1)
          balance.out(1) ~> Sink.fromSubscriber(c2)
          balance.out(2) ~> Sink.fromSubscriber(c3)

          ClosedShape
        })
        .run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      val sub3 = c3.expectSubscription()

      sub1.request(1)
      sub1.cancel()
      sub2.request(1)
      sub2.cancel()

      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)

      sub3.request(1)
      c3.expectNext(1)

      sub3.cancel()

      bsub.expectCancellation()
    }
  }

}
