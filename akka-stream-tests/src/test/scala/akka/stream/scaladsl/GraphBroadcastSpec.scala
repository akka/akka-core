/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class GraphBroadcastSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  "A broadcast" must {
    import GraphDSL.Implicits._

    "broadcast to other subscriber" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val bcast = b.add(Broadcast[Int](2))
          Source(List(1, 2, 3)) ~> bcast.in
          bcast.out(0) ~> Flow[Int].buffer(16, OverflowStrategy.backpressure) ~> Sink.fromSubscriber(c1)
          bcast.out(1) ~> Flow[Int].buffer(16, OverflowStrategy.backpressure) ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      sub2.request(2)
      c1.expectNext(1)
      c1.expectNoMessage(100.millis)
      c2.expectNext(1)
      c2.expectNext(2)
      c2.expectNoMessage(100.millis)
      sub1.request(3)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
      sub2.request(3)
      c2.expectNext(3)
      c2.expectComplete()
    }

    "work with one-way broadcast" in {
      val result = Source
        .fromGraph(GraphDSL.create() { implicit b =>
          val broadcast = b.add(Broadcast[Int](1))
          val source = b.add(Source(1 to 3))

          source ~> broadcast.in

          SourceShape(broadcast.out(0))
        })
        .runFold(Seq[Int]())(_ :+ _)

      Await.result(result, 3.seconds) should ===(Seq(1, 2, 3))
    }

    "work with n-way broadcast" in {
      val headSink = Sink.head[Seq[Int]]

      import system.dispatcher
      val result = RunnableGraph
        .fromGraph(
          GraphDSL.createGraph(headSink, headSink, headSink, headSink, headSink)((fut1, fut2, fut3, fut4, fut5) =>
            Future.sequence(List(fut1, fut2, fut3, fut4, fut5))) { implicit b => (p1, p2, p3, p4, p5) =>
            val bcast = b.add(Broadcast[Int](5))
            Source(List(1, 2, 3)) ~> bcast.in
            bcast.out(0).grouped(5) ~> p1.in
            bcast.out(1).grouped(5) ~> p2.in
            bcast.out(2).grouped(5) ~> p3.in
            bcast.out(3).grouped(5) ~> p4.in
            bcast.out(4).grouped(5) ~> p5.in
            ClosedShape
          })
        .run()

      Await.result(result, 3.seconds) should be(List.fill(5)(List(1, 2, 3)))
    }

    "work with 22-way broadcast" in {
      type T = Seq[Int]
      type FT = Future[Seq[Int]]
      val headSink: Sink[T, FT] = Sink.head[T]

      import system.dispatcher
      val combine: (FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT, FT) => Future[
        Seq[Seq[Int]]] =
        (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22) =>
          Future.sequence(
            List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22))

      val result = RunnableGraph
        .fromGraph(GraphDSL.createGraph(
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink,
          headSink)(combine) {
          implicit b =>
            (p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22) =>
              val bcast = b.add(Broadcast[Int](22))
              Source(List(1, 2, 3)) ~> bcast.in
              bcast.out(0).grouped(5) ~> p1.in
              bcast.out(1).grouped(5) ~> p2.in
              bcast.out(2).grouped(5) ~> p3.in
              bcast.out(3).grouped(5) ~> p4.in
              bcast.out(4).grouped(5) ~> p5.in
              bcast.out(5).grouped(5) ~> p6.in
              bcast.out(6).grouped(5) ~> p7.in
              bcast.out(7).grouped(5) ~> p8.in
              bcast.out(8).grouped(5) ~> p9.in
              bcast.out(9).grouped(5) ~> p10.in
              bcast.out(10).grouped(5) ~> p11.in
              bcast.out(11).grouped(5) ~> p12.in
              bcast.out(12).grouped(5) ~> p13.in
              bcast.out(13).grouped(5) ~> p14.in
              bcast.out(14).grouped(5) ~> p15.in
              bcast.out(15).grouped(5) ~> p16.in
              bcast.out(16).grouped(5) ~> p17.in
              bcast.out(17).grouped(5) ~> p18.in
              bcast.out(18).grouped(5) ~> p19.in
              bcast.out(19).grouped(5) ~> p20.in
              bcast.out(20).grouped(5) ~> p21.in
              bcast.out(21).grouped(5) ~> p22.in
              ClosedShape
        })
        .run()

      Await.result(result, 3.seconds) should be(List.fill(22)(List(1, 2, 3)))
    }

    "produce to other even though downstream cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val bcast = b.add(Broadcast[Int](2))
          Source(List(1, 2, 3)) ~> bcast.in
          bcast.out(0) ~> Flow[Int] ~> Sink.fromSubscriber(c1)
          bcast.out(1) ~> Flow[Int] ~> Sink.fromSubscriber(c2)
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

    "produce to downstream even though other cancels" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val bcast = b.add(Broadcast[Int](2))
          Source(List(1, 2, 3)) ~> bcast.in
          bcast.out(0) ~> Flow[Int].named("identity-a") ~> Sink.fromSubscriber(c1)
          bcast.out(1) ~> Flow[Int].named("identity-b") ~> Sink.fromSubscriber(c2)
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

    "cancel upstream when downstreams cancel" in {
      val p1 = TestPublisher.manualProbe[Int]()
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      RunnableGraph
        .fromGraph(GraphDSL.create() { implicit b =>
          val bcast = b.add(Broadcast[Int](2))
          Source.fromPublisher(p1.getPublisher) ~> bcast.in
          bcast.out(0) ~> Flow[Int] ~> Sink.fromSubscriber(c1)
          bcast.out(1) ~> Flow[Int] ~> Sink.fromSubscriber(c2)
          ClosedShape
        })
        .run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(3)
      sub2.request(3)
      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)
      c1.expectNext(1)
      c2.expectNext(1)
      bsub.sendNext(2)
      c1.expectNext(2)
      c2.expectNext(2)
      sub1.cancel()
      sub2.cancel()
      bsub.expectCancellation()
    }

    "pass along early cancellation" in {
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()

      val sink = Sink.fromGraph(GraphDSL.create() { implicit b =>
        val bcast = b.add(Broadcast[Int](2))
        bcast.out(0) ~> Sink.fromSubscriber(c1)
        bcast.out(1) ~> Sink.fromSubscriber(c2)
        SinkShape(bcast.in)
      })

      val s = Source.asSubscriber[Int].to(sink).run()

      val up = TestPublisher.manualProbe[Int]()

      val downsub1 = c1.expectSubscription()
      val downsub2 = c2.expectSubscription()
      downsub1.cancel()
      downsub2.cancel()

      up.subscribe(s)
      val upsub = up.expectSubscription()
      upsub.expectCancellation()
    }

    "alsoTo must broadcast" in {
      val p, p2 = TestSink[Int]()(system)
      val (ps1, ps2) = Source(1 to 6).alsoToMat(p)(Keep.right).toMat(p2)(Keep.both).run()
      ps1.request(6)
      ps2.request(6)
      ps1.expectNext(1, 2, 3, 4, 5, 6)
      ps2.expectNext(1, 2, 3, 4, 5, 6)
      ps1.expectComplete()
      ps2.expectComplete()
    }

    "cancel if alsoTo side branch cancels" in {
      val in = TestSource[Int]()(system)
      val outSide = TestSink[Int]()(system)
      val (pIn, pSide) = in.alsoToMat(outSide)(Keep.both).toMat(Sink.ignore)(Keep.left).run()

      pSide.cancel()
      pIn.expectCancellation()
    }

    "cancel if alsoTo main branch cancels" in {
      val in = TestSource[Int]()(system)
      val outMain = TestSink[Int]()(system)
      val (pIn, pMain) = in.alsoToMat(Sink.ignore)(Keep.left).toMat(outMain)(Keep.both).run()

      pMain.cancel()
      pIn.expectCancellation()
    }
  }

}
