/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl._

import scala.util.Random
import scala.math._
import scala.concurrent.duration._
import scala.collection.immutable
import akka.testkit.{ AkkaSpec, TestLatch }

import scala.annotation.nowarn
import scala.concurrent.Await

class RateTransformationDocSpec extends AkkaSpec {

  "conflate should summarize" in {
    //#conflate-summarize
    val statsFlow = Flow[Double].conflateWithSeed(immutable.Seq(_))(_ :+ _).map { s =>
      val μ = s.sum / s.size
      val se = s.map(x => pow(x - μ, 2))
      val σ = sqrt(se.sum / se.size)
      (σ, μ, s.size)
    }
    //#conflate-summarize

    val fut =
      Source
        .fromIterator(() => Iterator.continually(Random.nextGaussian()))
        .via(statsFlow)
        .grouped(10)
        .runWith(Sink.head)

    fut.futureValue
  }

  "conflate should sample" in {
    //#conflate-sample
    val p = 0.01
    val sampleFlow = Flow[Double]
      .conflateWithSeed(immutable.Seq(_)) {
        case (acc, elem) if Random.nextDouble() < p => acc :+ elem
        case (acc, _)                               => acc
      }
      .mapConcat(identity)
    //#conflate-sample

    val fut = Source(1 to 1000).map(_.toDouble).via(sampleFlow).runWith(Sink.fold(Seq.empty[Double])(_ :+ _))

    fut.futureValue
  }

  "extrapolate should repeat last" in {
    //#extrapolate-last
    val lastFlow = Flow[Double].extrapolate(Iterator.continually(_))
    //#extrapolate-last

    val (probe, fut) = TestSource[Double]().via(lastFlow).grouped(10).toMat(Sink.head)(Keep.both).run()

    probe.sendNext(1.0)
    val extrapolated = fut.futureValue
    extrapolated.size shouldBe 10
    extrapolated.sum shouldBe 10
  }

  "extrapolate should send seed first" in {
    //#extrapolate-seed
    val initial = 2.0
    val seedFlow = Flow[Double].extrapolate(Iterator.continually(_), Some(initial))
    //#extrapolate-seed

    val fut = TestSource[Double]().via(seedFlow).grouped(10).runWith(Sink.head)

    val extrapolated = fut.futureValue
    extrapolated.size shouldBe 10
    extrapolated.sum shouldBe 10 * initial
  }

  "extrapolate should track drift" in {
    @nowarn("msg=never used") // sample snippet
    //#extrapolate-drift
    val driftFlow = Flow[Double].map(_ -> 0).extrapolate[(Double, Int)] { case (i, _) => Iterator.from(1).map(i -> _) }
    //#extrapolate-drift
    val latch = TestLatch(2)
    val realDriftFlow = Flow[Double].map(d => { latch.countDown(); d -> 0; }).extrapolate[(Double, Int)] {
      case (d, _) => latch.countDown(); Iterator.from(1).map(d -> _)
    }

    val (pub, sub) = TestSource[Double]().via(realDriftFlow).toMat(TestSink[(Double, Int)]())(Keep.both).run()

    sub.request(1)
    pub.sendNext(1.0)
    sub.expectNext((1.0, 0))

    sub.requestNext((1.0, 1))
    sub.requestNext((1.0, 2))

    pub.sendNext(2.0)
    Await.ready(latch, 1.second)
    sub.requestNext((2.0, 0))
  }

  "expand should track drift" in {
    @nowarn("msg=never used") // sample snippet
    //#expand-drift
    val driftFlow = Flow[Double].expand(i => Iterator.from(0).map(i -> _))
    //#expand-drift
    val latch = TestLatch(2)
    val realDriftFlow = Flow[Double].expand(d => { latch.countDown(); Iterator.from(0).map(d -> _) })

    val (pub, sub) = TestSource[Double]().via(realDriftFlow).toMat(TestSink[(Double, Int)]())(Keep.both).run()

    sub.request(1)
    pub.sendNext(1.0)
    sub.expectNext((1.0, 0))

    sub.requestNext((1.0, 1))
    sub.requestNext((1.0, 2))

    pub.sendNext(2.0)
    Await.ready(latch, 1.second)
    sub.requestNext((2.0, 0))
  }

}
