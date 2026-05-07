/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import akka.stream.ClosedShape
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestPublisher.Probe
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

object GraphZipLatestSpec {
  val someString = "someString"
  val someInt = 1
}

class GraphZipLatestSpec extends StreamSpec with ScalaCheckPropertyChecks with ScalaFutures {
  import GraphZipLatestSpec._

  "ZipLatest" must {
    "only emit when at least one pair is available" in {
      val (probe, bools, ints) = testGraph[Boolean, Int]

      probe.request(1)

      // an element pushed on one of the sources
      bools.sendNext(true)

      // does not emit yet
      probe.expectNoMessage(0 seconds)

      // an element pushed on the other source
      ints.sendNext(1)

      // emits a single pair
      probe.expectNext((true, 1))

      probe.cancel()
    }

    "emits as soon as one source is available" in {
      val (probe, bools, ints) = testGraph[Boolean, Int]

      probe.request(3)

      // a first element pushed on either source
      bools.sendNext(true)
      ints.sendNext(1)

      // then 2 elements pushed only on one source
      ints.sendNext(1)
      ints.sendNext(1)

      // 3 elements are emitted
      probe.expectNext((true, 1))
      probe.expectNext((true, 1))
      probe.expectNext((true, 1))

      probe.cancel()
    }

    "does not emit the same pair upon two pulls with value types" in {
      val (probe, bools, ints) = testGraph[Boolean, Int]

      probe.request(1)

      // one element pushed on each source
      bools.sendNext(true)
      ints.sendNext(1)

      // emits a single pair
      probe.expectNext((true, 1))

      // another request does not emit a duplicate
      probe.request(1)
      probe.expectNoMessage(0 seconds)

      bools.sendComplete()
      probe.expectComplete()
    }

    "does not emit the same pair upon two pulls with reference types" in {
      val a = someString
      val b = Some(someInt)
      val (probe, as, bs) = testGraph[String, Option[Int]]

      probe.request(1)

      // one element pushed on each source
      as.sendNext(a)
      bs.sendNext(b)

      // emits a single pair
      probe.expectNext((a, b))

      // another request does not emit a duplicate
      probe.request(1)
      probe.expectNoMessage(0 seconds)

      as.sendComplete()
      probe.expectComplete()
    }

    "does not de-duplicate instances based on value" in {
      /*
        S1 -> A1 A2 A3   --\
                            > -- ZipLatest
        S2 -> B1      B2 --/
       */

      val a1 = someString
      val a2 = someString
      val a3 = someString
      val b1 = someInt
      val b2 = someInt
      val (probe, as, bs) = testGraph[String, Int]

      // O -> (A1, B1), (A2, B1), (A3, B1), (A3, B2)
      probe.request(4)

      as.sendNext(a1)
      bs.sendNext(b1)
      probe.expectNext((a1, b1))

      as.sendNext(a2)
      probe.expectNext((a2, b1))

      as.sendNext(a3)
      probe.expectNext((a3, b1))

      bs.sendNext(b2)
      probe.expectNext((a3, b2))

      probe.cancel()
    }

    val first = (t: (Probe[Boolean], Probe[Int])) => t._1
    val second = (t: (Probe[Boolean], Probe[Int])) => t._2

    "complete when either source completes" in {
      forAll(Gen.oneOf(first, second)) { select =>
        val (probe, bools, ints) = testGraph[Boolean, Int]
        select((bools, ints)).sendComplete()
        probe.expectSubscriptionAndComplete()
      }
    }

    "complete when either source completes and requesting element" in {
      forAll(Gen.oneOf(first, second)) { select =>
        val (probe, bools, ints) = testGraph[Boolean, Int]
        select((bools, ints)).sendComplete()
        probe.request(1)
        probe.expectComplete()
      }
    }

    "complete when either source completes with some pending element" in {
      forAll(Gen.oneOf(first, second)) { select =>
        val (probe, bools, ints) = testGraph[Boolean, Int]

        // one element pushed on each source
        bools.sendNext(true)
        ints.sendNext(1)

        // either source completes
        select((bools, ints)).sendComplete()

        // should emit first element then complete
        probe.requestNext((true, 1))
        probe.expectComplete()
      }
    }

    // Reproducing #26711
    "complete when one quickly completes without any elements" in {
      // not sure quite why, but Source.empty does not trigger this, while Source(List()) does - race condition somehow
      val immediatelyCompleting = Source.single(3).zipLatest(Source(List[Int]()))

      immediatelyCompleting.runWith(Sink.seq).futureValue should ===(Seq[(Int, Int)]())
    }

    "complete when one source completes and the other continues pushing" in {

      val (probe, bools, ints) = testGraph[Boolean, Int]

      // one element pushed on each source
      bools.sendNext(true)
      ints.sendNext(1)

      // either source completes
      bools.sendComplete()
      ints.sendNext(10)
      ints.sendNext(10)

      // should emit first element then complete
      probe.requestNext((true, 1))
      probe.expectComplete()
    }

    "complete if no pending demand" in {
      forAll(Gen.oneOf(first, second)) { select =>
        val (probe, bools, ints) = testGraph[Boolean, Int]

        probe.request(1)

        // one element pushed on each source and tuple emitted
        bools.sendNext(true)
        ints.sendNext(1)
        probe.expectNext((true, 1))

        // either source completes
        select((bools, ints)).sendComplete()

        // should complete
        probe.expectComplete()
      }
    }

    "fail when either source has error" in {
      forAll(Gen.oneOf(first, second)) { select =>
        val (probe, bools, ints) = testGraph[Boolean, Int]
        val error = new RuntimeException

        select((bools, ints)).sendError(error)
        probe.expectSubscriptionAndError(error)
      }
    }

    "emit even if pair is the same" in {
      val (probe, bools, ints) = testGraph[Boolean, Int]

      probe.request(2)

      // one element pushed on each source
      bools.sendNext(true)
      ints.sendNext(1)
      // once again the same element on one source
      ints.sendNext(1)

      // followed by complete
      bools.sendComplete()
      ints.sendComplete()

      // emits two equal pairs
      probe.expectNext((true, 1))
      probe.expectNext((true, 1))

      // then completes
      probe.expectComplete()
    }

    "emit combined elements in proper order" in {
      val (probe, firstDigits, secondDigits) = testGraph[Int, Int]

      // numbers up to 99 in tuples
      val allNumbers = for {
        firstDigit <- 0 to 9
        secondDigit <- 0 to 9
      } yield (firstDigit, secondDigit)

      allNumbers.groupBy(_._1).toList.sortBy(_._1).foreach {
        case (firstDigit, pairs) => {
          firstDigits.sendNext(firstDigit)
          pairs.map { case (_, digits) => digits }.foreach { secondDigit =>
            secondDigits.sendNext(secondDigit)
            probe.request(1)
            probe.expectNext((firstDigit, secondDigit))
          }
        }
      }

      probe.cancel()
    }
  }

  private def testGraph[A, B]: (TestSubscriber.Probe[(A, B)], TestPublisher.Probe[A], TestPublisher.Probe[B]) =
    RunnableGraph
      .fromGraph(GraphDSL.createGraph(TestSink[(A, B)](), TestSource[A](), TestSource[B]())(Tuple3.apply) {
        implicit b => (ts, as, bs) =>
          import GraphDSL.Implicits._
          val zipLatest = b.add(new ZipLatest[A, B]())
          as ~> zipLatest.in0
          bs ~> zipLatest.in1
          zipLatest.out ~> ts
          ClosedShape
      })
      .run()

}
