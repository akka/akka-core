/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom.{ current => random }

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.stream.ActorAttributes._
import akka.stream.Attributes
import akka.stream.Supervision._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class FlowFilterSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "A Filter" must {

    "filter" in {
      def script =
        Script(TestConfig.RandomTestRange.map { _ =>
          val x = random.nextInt(); Seq(x) -> (if ((x & 1) == 0) Seq(x) else Seq())
        }: _*)
      TestConfig.RandomTestRange.foreach(_ => runScript(script)(_.filter(_ % 2 == 0)))
    }

    "not blow up with high request counts" in {
      val probe = TestSubscriber.manualProbe[Int]()
      Source(List.fill(1000)(0) ::: List(1))
        .filter(_ != 0)
        .toMat(Sink.fromSubscriber(probe))(Keep.right)
        .withAttributes(Attributes.inputBuffer(1, 1))
        .run()

      val subscription = probe.expectSubscription()
      for (_ <- 1 to 10000) {
        subscription.request(Int.MaxValue)
      }

      probe.expectNext(1)
      probe.expectComplete()
    }

    "continue if error" in {
      val TE = new Exception("TEST") with NoStackTrace {
        override def toString = "TE"
      }

      Source(1 to 3)
        .filter((x: Int) => if (x == 2) throw TE else true)
        .withAttributes(supervisionStrategy(resumingDecider))
        .runWith(TestSink[Int]())
        .request(3)
        .expectNext(1, 3)
        .expectComplete()
    }

    "filter out elements without demand" in {
      val (inProbe, outProbe) =
        TestSource[Int]()
          .filter(_ > 1000)
          .toMat(TestSink[Int]())(Keep.both)
          .addAttributes(Attributes.inputBuffer(1, 1))
          .run()

      outProbe.ensureSubscription()
      // none of those should fail even without demand
      inProbe.sendNext(1).sendNext(2).sendNext(3).sendNext(4).sendNext(5).sendNext(1001).sendNext(1002)

      // now the buffer should be full (1 internal buffer, 1 async buffer at source probe)

      inProbe.expectNoMessage(100.millis).pending shouldBe 0L

      inProbe.sendComplete() // to test later that completion is buffered as well

      outProbe.requestNext(1001)
      outProbe.requestNext(1002)
      outProbe.expectComplete()
    }
    "complete without demand if remaining elements are filtered out" in {
      Source(1 to 1000).filter(_ > 1000).runWith(TestSink[Int]()).ensureSubscription().expectComplete()
    }
  }

  "A FilterNot" must {
    "filter based on inverted predicate" in {
      def script =
        Script(TestConfig.RandomTestRange.map { _ =>
          val x = random.nextInt()
          Seq(x) -> (if ((x & 1) == 1) Seq(x) else Seq())
        }: _*)
      TestConfig.RandomTestRange.foreach(_ => runScript(script)(_.filterNot(_ % 2 == 0)))
    }
  }

}
