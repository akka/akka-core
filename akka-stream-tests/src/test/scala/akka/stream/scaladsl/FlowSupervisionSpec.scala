/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.Supervision
import akka.stream.impl.ReactiveStreamsCompliance
import akka.stream.testkit._

class FlowSupervisionSpec extends StreamSpec {
  import ActorAttributes.supervisionStrategy

  val exc = new RuntimeException("simulated exc") with NoStackTrace

  val failingMap = Flow[Int].map(n => if (n == 3) throw exc else n)

  def run(f: Flow[Int, Int, NotUsed]): immutable.Seq[Int] =
    Await.result(Source((1 to 5).toSeq ++ (1 to 5)).via(f).limit(1000).runWith(Sink.seq), 3.seconds)

  "Stream supervision" must {

    "stop and complete stream with failure by default" in {
      intercept[RuntimeException] {
        run(failingMap)
      } should be(exc)
    }

    "support resume " in {
      val result = run(failingMap.withAttributes(supervisionStrategy(Supervision.resumingDecider)))
      result should be(List(1, 2, 4, 5, 1, 2, 4, 5))
    }

    "support restart " in {
      val result = run(failingMap.withAttributes(supervisionStrategy(Supervision.restartingDecider)))
      result should be(List(1, 2, 4, 5, 1, 2, 4, 5))
    }

    "complete stream with NPE failure when null is emitted" in {
      intercept[NullPointerException] {
        Await.result(Source(List("a", "b")).map(_ => null).limit(1000).runWith(Sink.seq), 3.seconds)
      }.getMessage should be(ReactiveStreamsCompliance.ElementMustNotBeNullMsg)
    }

    "resume stream when null is emitted" in {
      val nullMap = Flow[String]
        .map(elem => if (elem == "b") null else elem)
        .withAttributes(supervisionStrategy(Supervision.resumingDecider))
      val result = Await.result(Source(List("a", "b", "c")).via(nullMap).limit(1000).runWith(Sink.seq), 3.seconds)
      result should be(List("a", "c"))
    }

  }
}
