/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.nowarn
import scala.collection.immutable.Seq
import scala.concurrent.Future

import org.reactivestreams.Publisher

import akka.NotUsed
import akka.stream.testkit.StreamSpec

@nowarn // unused vars are used in shouldNot compile tests
class FlowCompileSpec extends StreamSpec {

  val intSeq = Source(Seq(1, 2, 3))
  val strSeq = Source(Seq("a", "b", "c"))

  import scala.concurrent.ExecutionContext.Implicits.global
  val intFut = Source.fromFuture(Future { 3 })

  "Flow" should {
    "not run" in {
      val open: Flow[Int, Int, _] = Flow[Int]
      "open.run()" shouldNot compile
    }
    "accept Iterable" in {
      val f: Source[Int, _] = intSeq.via(Flow[Int])
    }
    "accept Future" in {
      val f: Source[Int, _] = intFut.via(Flow[Int])
    }
    "append Flow" in {
      val open1: Flow[Int, String, _] = Flow[Int].map(_.toString)
      val open2: Flow[String, Int, _] = Flow[String].map(_.hashCode)
      val open3: Flow[Int, Int, _] = open1.via(open2)
      "open3.run()" shouldNot compile

      val closedSource: Source[Int, _] = intSeq.via(open3)
      val closedSink: Sink[Int, _] = open3.to(Sink.asPublisher[Int](false))
      "closedSink.run()" shouldNot compile

      closedSource.to(Sink.asPublisher[Int](false)).run()
      intSeq.to(closedSink).run()
    }
    "append Sink" in {
      val open: Flow[Int, String, _] = Flow[Int].map(_.toString)
      val closedSink: Sink[String, _] = Flow[String].map(_.hashCode).to(Sink.asPublisher[Int](false))
      val appended: Sink[Int, _] = open.to(closedSink)
      "appended.run()" shouldNot compile
      "appended.to(Sink.head[Int])" shouldNot compile
      intSeq.to(appended).run()
    }
    "be appended to Source" in {
      val open: Flow[Int, String, _] = Flow[Int].map(_.toString)
      val closedSource: Source[Int, _] = strSeq.via(Flow[String].map(_.hashCode))
      val closedSource2: Source[String, _] = closedSource.via(open)
      "strSeq.to(closedSource2)" shouldNot compile
      closedSource2.to(Sink.asPublisher[String](false)).run()
    }
  }

  "Sink" should {
    val openSink: Sink[Int, _] =
      Flow[Int].map(_.toString).to(Sink.asPublisher[String](false))
    "accept Source" in {
      intSeq.to(openSink)
    }
    "not accept Sink" in {
      "openSink.to(Sink.head[String])" shouldNot compile
    }
    "not run()" in {
      "openSink.run()" shouldNot compile
    }
  }

  "Source" should {
    val openSource: Source[String, _] =
      Source(Seq(1, 2, 3)).map(_.toString)
    "accept Sink" in {
      openSource.to(Sink.asPublisher[String](false))
    }
    "not be accepted by Source" in {
      "openSource.to(intSeq)" shouldNot compile
    }
  }

  "RunnableGraph" should {
    Sink.head[String]
    val closed: RunnableGraph[Publisher[String]] =
      Source(Seq(1, 2, 3)).map(_.toString).toMat(Sink.asPublisher[String](false))(Keep.right)
    "run" in {
      closed.run()
    }
    "not be accepted by Source" in {
      "intSeq.to(closed)" shouldNot compile
    }

    "not accept Sink" in {
      "closed.to(Sink.head[String])" shouldNot compile
    }
  }

  "FlowOps" should {
    "be extensible" in {
      val f: FlowOps[Int, NotUsed] { type Closed = Sink[Int, NotUsed] } = Flow[Int]
      val fm = f.map(identity)
      val f2: FlowOps[Int, NotUsed] = fm
      val s: Sink[Int, NotUsed] = fm.to(Sink.ignore)
    }

    "be extensible (with MaterializedValue)" in {
      val f: FlowOpsMat[Int, NotUsed] { type ClosedMat[+M] = Sink[Int, M] } = Flow[Int]
      val fm = f.map(identity).concatMat(Source.empty)(Keep.both)
      // this asserts only the FlowOpsMat part of the signature, but fm also carries the
      // CloseMat type without which `.to(sink)` does not work
      val f2: FlowOpsMat[Int, (NotUsed, NotUsed)] = fm
      val s: Sink[Int, (NotUsed, NotUsed)] = fm.to(Sink.ignore)
    }
  }
}
