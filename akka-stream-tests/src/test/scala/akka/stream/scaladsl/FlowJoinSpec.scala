/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable

import org.scalatest.time._

import akka.stream.FlowShape
import akka.stream.OverflowStrategy
import akka.stream.testkit._
import akka.stream.testkit.scaladsl._

class FlowJoinSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) {

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(200, Millis))

  "A Flow using join" must {
    "allow for cycles" in {
      val end = 47
      val (even, odd) = (0 to end).partition(_ % 2 == 0)
      val result = Set() ++ even ++ odd ++ odd.map(_ * 10)
      val source = Source(0 to end)
      val probe = TestSubscriber.manualProbe[Seq[Int]]()

      val flow1 = Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val merge = b.add(Merge[Int](2))
        val broadcast = b.add(Broadcast[Int](2))
        source ~> merge.in(0)
        merge.out ~> broadcast.in
        broadcast.out(0).grouped(1000) ~> Sink.fromSubscriber(probe)

        FlowShape(merge.in(1), broadcast.out(1))
      })

      val flow2 = Flow[Int]
        .filter(_ % 2 == 1)
        .map(_ * 10)
        .buffer((end + 1) / 2, OverflowStrategy.backpressure)
        .take((end + 1) / 2)

      flow1.join(flow2).run()

      val sub = probe.expectSubscription()
      sub.request(1)
      probe.expectNext().toSet should be(result)
      sub.cancel()
    }

    "allow for merge cycle" in {
      val source = Source.single("lonely traveler")

      val flow1 = Flow.fromGraph(GraphDSL.createGraph(Sink.head[String]) { implicit b => sink =>
        import GraphDSL.Implicits._
        val merge = b.add(Merge[String](2))
        val broadcast = b.add(Broadcast[String](2, eagerCancel = true))
        source ~> merge.in(0)
        merge.out ~> broadcast.in
        broadcast.out(0) ~> sink

        FlowShape(merge.in(1), broadcast.out(1))
      })

      whenReady(flow1.join(Flow[String]).run())(_ shouldBe "lonely traveler")
    }

    "allow for merge preferred cycle" in {
      val source = Source.single("lonely traveler")

      val flow1 = Flow.fromGraph(GraphDSL.createGraph(Sink.head[String]) { implicit b => sink =>
        import GraphDSL.Implicits._
        val merge = b.add(MergePreferred[String](1))
        val broadcast = b.add(Broadcast[String](2, eagerCancel = true))
        source ~> merge.preferred
        merge.out ~> broadcast.in
        broadcast.out(0) ~> sink

        FlowShape(merge.in(0), broadcast.out(1))
      })

      whenReady(flow1.join(Flow[String]).run())(_ shouldBe "lonely traveler")
    }

    "allow for zip cycle" in {
      val source = Source(immutable.Seq("traveler1", "traveler2"))

      val flow = Flow.fromGraph(GraphDSL.createGraph(TestSink[(String, String)]()) { implicit b => sink =>
        import GraphDSL.Implicits._
        val zip = b.add(Zip[String, String]())
        val broadcast = b.add(Broadcast[(String, String)](2))
        source ~> zip.in0
        zip.out ~> broadcast.in
        broadcast.out(0) ~> sink

        FlowShape(zip.in1, broadcast.out(1))
      })

      val feedback = Flow.fromGraph(GraphDSL.createGraph(Source.single("ignition")) { implicit b => ignition =>
        import GraphDSL.Implicits._
        val flow = b.add(Flow[(String, String)].map(_._1))
        val merge = b.add(Merge[String](2))

        ignition ~> merge.in(0)
        flow ~> merge.in(1)

        FlowShape(flow.in, merge.out)
      })

      val probe = flow.join(feedback).run()
      probe.requestNext(("traveler1", "ignition"))
      probe.requestNext(("traveler2", "traveler1"))
    }

    "allow for concat cycle" in {
      val flow = Flow.fromGraph(GraphDSL.createGraph(TestSource[String]()(system), Sink.head[String])(Keep.both) {
        implicit b => (source, sink) =>
          import GraphDSL.Implicits._
          val concat = b.add(Concat[String](2))
          val broadcast = b.add(Broadcast[String](2, eagerCancel = true))
          source ~> concat.in(0)
          concat.out ~> broadcast.in
          broadcast.out(0) ~> sink

          FlowShape(concat.in(1), broadcast.out(1))
      })

      val (probe, result) = flow.join(Flow[String]).run()
      probe.sendNext("lonely traveler")
      whenReady(result) { r =>
        r shouldBe "lonely traveler"
        probe.sendComplete()
      }
    }

    "allow for interleave cycle" in {
      val source = Source.single("lonely traveler")

      val flow1 = Flow.fromGraph(GraphDSL.createGraph(Sink.head[String]) { implicit b => sink =>
        import GraphDSL.Implicits._
        val merge = b.add(Interleave[String](2, 1))
        val broadcast = b.add(Broadcast[String](2, eagerCancel = true))
        source ~> merge.in(0)
        merge.out ~> broadcast.in
        broadcast.out(0) ~> sink

        FlowShape(merge.in(1), broadcast.out(1))
      })

      whenReady(flow1.join(Flow[String]).run())(_ shouldBe "lonely traveler")
    }
  }
}
