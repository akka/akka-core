/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream._
import akka.stream.testkit.StreamSpec
import akka.util.ByteString

@nowarn // tests deprecated APIs
class BidiFlowSpec extends StreamSpec {
  import Attributes._
  import GraphDSL.Implicits._

  val bidi = BidiFlow.fromFlows(
    Flow[Int].map(x => x.toLong + 2).withAttributes(name("top")),
    Flow[ByteString].map(_.decodeString("UTF-8")).withAttributes(name("bottom")))

  val inverse = BidiFlow.fromFlows(
    Flow[Long].map(x => x.toInt + 2).withAttributes(name("top")),
    Flow[String].map(ByteString(_)).withAttributes(name("bottom")))

  val bidiMat = BidiFlow.fromGraph(GraphDSL.createGraph(Sink.head[Int]) { implicit b => s =>
    Source.single(42) ~> s

    val top = b.add(Flow[Int].map(x => x.toLong + 2))
    val bottom = b.add(Flow[ByteString].map(_.decodeString("UTF-8")))
    BidiShape(top.in, top.out, bottom.in, bottom.out)
  })

  val str = "Hello World"
  val bytes = ByteString(str)

  "A BidiFlow" must {

    "work top/bottom in isolation" in {
      val (top, bottom) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.head[Long], Sink.head[String])(Keep.both) { implicit b => (st, sb) =>
          val s = b.add(bidi)

          Source.single(1) ~> s.in1; s.out1 ~> st
          sb <~ s.out2; s.in2 <~ Source.single(bytes)
          ClosedShape
        })
        .run()

      Await.result(top, 1.second) should ===(3L)
      Await.result(bottom, 1.second) should ===(str)
    }

    "work as a Flow that is open on the left" in {
      val f = bidi.join(Flow[Long].map(x => ByteString(s"Hello $x")))
      val result = Source(List(1, 2, 3)).via(f).limit(10).runWith(Sink.seq)
      Await.result(result, 1.second) should ===(Seq("Hello 3", "Hello 4", "Hello 5"))
    }

    "work as a Flow that is open on the right" in {
      val f = Flow[String].map(Integer.valueOf(_).toInt).join(bidi)
      val result = Source(List(ByteString("1"), ByteString("2"))).via(f).limit(10).runWith(Sink.seq)
      Await.result(result, 1.second) should ===(Seq(3L, 4L))
    }

    "work when atop its inverse" in {
      val f = bidi.atop(inverse).join(Flow[Int].map(_.toString))
      val result = Source(List(1, 2, 3)).via(f).limit(10).runWith(Sink.seq)
      Await.result(result, 1.second) should ===(Seq("5", "6", "7"))
    }

    "work when reversed" in {
      // just reversed from the case above; observe that Flow inverts itself automatically by being on the left side
      val f = Flow[Int].map(_.toString).join(inverse.reversed).join(bidi.reversed)
      val result = Source(List(1, 2, 3)).via(f).limit(10).runWith(Sink.seq)
      Await.result(result, 1.second) should ===(Seq("5", "6", "7"))
    }

    "materialize to its value" in {
      val f = RunnableGraph
        .fromGraph(GraphDSL.createGraph(bidiMat) { implicit b => bidi =>
          Flow[String].map(Integer.valueOf(_).toInt) <~> bidi <~> Flow[Long].map(x => ByteString(s"Hello $x"))
          ClosedShape
        })
        .run()
      Await.result(f, 1.second) should ===(42)
    }

    "combine materialization values" in {
      val left = Flow.fromGraph(GraphDSL.createGraph(Sink.head[Int]) { implicit b => sink =>
        val bcast = b.add(Broadcast[Int](2))
        val merge = b.add(Merge[Int](2))
        val flow = b.add(Flow[String].map(Integer.valueOf(_).toInt))
        bcast ~> sink
        Source.single(1) ~> bcast ~> merge
        flow ~> merge
        FlowShape(flow.in, merge.out)
      })
      val right = Flow.fromGraph(GraphDSL.createGraph(Sink.head[immutable.Seq[Long]]) { implicit b => sink =>
        val flow = b.add(Flow[Long].grouped(10))
        flow ~> sink
        FlowShape(flow.in, b.add(Source.single(ByteString("10"))).out)
      })
      val ((l, m), r) = left.joinMat(bidiMat)(Keep.both).joinMat(right)(Keep.both).run()
      Await.result(l, 1.second) should ===(1)
      Await.result(m, 1.second) should ===(42)
      Await.result(r, 1.second).toSet should ===(Set(3L, 12L))
    }

    "suitably override attribute handling methods" in {
      import Attributes._
      val b: BidiFlow[Int, Long, ByteString, String, NotUsed] = bidi.async.addAttributes(none).named("name")

      val name = b.traversalBuilder.attributes.getFirst[Name]
      name shouldEqual Some(Name("name"))
      val boundary = b.traversalBuilder.attributes.getFirst[AsyncBoundary.type]
      boundary shouldEqual Some(AsyncBoundary)
    }

    "short circuit identity in atop" in {
      val myBidi = BidiFlow.fromFlows(Flow[Long].map(_ + 1L), Flow[ByteString])
      val identity = BidiFlow.identity[Long, ByteString]

      // simple ones
      myBidi.atop(identity) should ===(myBidi)
      identity.atopMat(myBidi)(Keep.right) should ===(myBidi)

      // optimized but not the same instance (because myBidi mat value is dropped)
      identity.atop(myBidi) should !==(myBidi)
      myBidi.atopMat(identity)(Keep.right) should !==(myBidi)
    }

    "semi-shortcuted atop with identity should still work" in {
      // atop when the NotUsed matval is kept from identity has a smaller optimization, so verify they still work
      val myBidi =
        BidiFlow.fromFlows(Flow[Long].map(_ + 1L), Flow[Long].map(_ + 1L)).mapMaterializedValue(_ => "bidi-matval")
      val identity = BidiFlow.identity[Long, Long]

      def verify[M](atopBidi: BidiFlow[Long, Long, Long, Long, M], expectedMatVal: M): Unit = {
        val joinedFlow = atopBidi.joinMat(Flow[Long])(Keep.left)
        val (bidiMatVal, seqSinkMatValF) =
          Source(1L :: 2L :: Nil).viaMat(joinedFlow)(Keep.right).toMat(Sink.seq)(Keep.both).run()
        seqSinkMatValF.futureValue should ===(Seq(3L, 4L))
        bidiMatVal should ===(expectedMatVal)
      }

      // identity atop myBidi
      verify(identity.atopMat(myBidi)(Keep.left), NotUsed)
      verify(identity.atopMat(myBidi)(Keep.none), NotUsed)
      verify(identity.atopMat(myBidi)(Keep.right), "bidi-matval")
      // arbitrary matval combine
      verify(identity.atopMat(myBidi)((_, m) => m), "bidi-matval")

      // myBidi atop identity
      verify(myBidi.atopMat(identity)(Keep.left), "bidi-matval")
      verify(myBidi.atopMat(identity)(Keep.none), NotUsed)
      verify(myBidi.atopMat(identity)(Keep.right), NotUsed)
      verify(myBidi.atopMat(identity)((m, _) => m), "bidi-matval")
    }

  }

}
