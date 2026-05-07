/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom.{ current => random }

import scala.collection.immutable
import scala.concurrent.duration._

import akka.stream.ThrottleMode
import akka.stream.testkit._
import akka.testkit.TimingTest
import akka.util.ConstantFun

class FlowGroupedWithinSpec extends StreamSpec with ScriptedTest {

  "A GroupedWithin" must {

    "group elements within the duration" taggedAs TimingTest in {
      val input = Iterator.from(1)
      val p = TestPublisher.manualProbe[Int]()
      val c = TestSubscriber.manualProbe[immutable.Seq[Int]]()
      Source.fromPublisher(p).groupedWithin(1000, 1.second).to(Sink.fromSubscriber(c)).run()
      val pSub = p.expectSubscription()
      val cSub = c.expectSubscription()
      cSub.request(100)
      val demand1 = pSub.expectRequest().toInt
      (1 to demand1).foreach { _ =>
        pSub.sendNext(input.next())
      }
      val demand2 = pSub.expectRequest().toInt
      (1 to demand2).foreach { _ =>
        pSub.sendNext(input.next())
      }
      val demand3 = pSub.expectRequest().toInt
      c.expectNext((1 to (demand1 + demand2).toInt).toVector)
      (1 to demand3).foreach { _ =>
        pSub.sendNext(input.next())
      }
      c.expectNoMessage(300.millis)
      c.expectNext(((demand1 + demand2 + 1).toInt to (demand1 + demand2 + demand3).toInt).toVector)
      c.expectNoMessage(300.millis)
      pSub.expectRequest()
      val last = input.next()
      pSub.sendNext(last)
      pSub.sendComplete()
      c.expectNext(List(last))
      c.expectComplete()
      c.expectNoMessage(200.millis)
    }

    "deliver buffered elements onComplete before the timeout" taggedAs TimingTest in {
      val c = TestSubscriber.manualProbe[immutable.Seq[Int]]()
      Source(1 to 3).groupedWithin(1000, 10.second).to(Sink.fromSubscriber(c)).run()
      val cSub = c.expectSubscription()
      cSub.request(100)
      c.expectNext((1 to 3).toList)
      c.expectComplete()
      c.expectNoMessage(200.millis)
    }

    "buffer groups until requested from downstream" taggedAs TimingTest in {
      val input = Iterator.from(1)
      val p = TestPublisher.manualProbe[Int]()
      val c = TestSubscriber.manualProbe[immutable.Seq[Int]]()
      Source.fromPublisher(p).groupedWithin(1000, 1.second).to(Sink.fromSubscriber(c)).run()
      val pSub = p.expectSubscription()
      val cSub = c.expectSubscription()
      cSub.request(1)
      val demand1 = pSub.expectRequest().toInt
      (1 to demand1).foreach { _ =>
        pSub.sendNext(input.next())
      }
      c.expectNext((1 to demand1).toVector)
      val demand2 = pSub.expectRequest().toInt
      (1 to demand2).foreach { _ =>
        pSub.sendNext(input.next())
      }
      c.expectNoMessage(300.millis)
      cSub.request(1)
      c.expectNext(((demand1 + 1) to (demand1 + demand2)).toVector)
      pSub.sendComplete()
      c.expectComplete()
      c.expectNoMessage(100.millis)
    }

    "drop empty groups" taggedAs TimingTest in {
      val p = TestPublisher.manualProbe[Int]()
      val c = TestSubscriber.manualProbe[immutable.Seq[Int]]()
      Source.fromPublisher(p).groupedWithin(1000, 500.millis).to(Sink.fromSubscriber(c)).run()
      val pSub = p.expectSubscription()
      val cSub = c.expectSubscription()
      cSub.request(2)
      pSub.expectRequest()
      c.expectNoMessage(600.millis)
      pSub.sendNext(1)
      pSub.sendNext(2)
      c.expectNext(List(1, 2))
      // nothing more requested
      c.expectNoMessage(1100.millis)
      cSub.request(3)
      c.expectNoMessage(600.millis)
      pSub.sendComplete()
      c.expectComplete()
    }

    "not emit empty group when finished while not being pushed" taggedAs TimingTest in {
      val p = TestPublisher.manualProbe[Int]()
      val c = TestSubscriber.manualProbe[immutable.Seq[Int]]()
      Source.fromPublisher(p).groupedWithin(1000, 50.millis).to(Sink.fromSubscriber(c)).run()
      val pSub = p.expectSubscription()
      val cSub = c.expectSubscription()
      cSub.request(1)
      pSub.expectRequest()
      pSub.sendComplete()
      c.expectComplete()
    }

    "reset time window when max elements reached" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()
      Source.fromPublisher(upstream).groupedWithin(3, 2.second).to(Sink.fromSubscriber(downstream)).run()

      downstream.request(2)
      downstream.expectNoMessage(1000.millis)

      (1 to 4).foreach(upstream.sendNext)
      downstream.within(1000.millis) {
        downstream.expectNext((1 to 3).toVector)
      }

      downstream.expectNoMessage(1500.millis)

      downstream.within(1000.millis) {
        downstream.expectNext(List(4))
      }

      upstream.sendComplete()
      downstream.expectComplete()
      downstream.expectNoMessage(100.millis)
    }

    "reset time window when exact max elements reached" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[Int]()
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()
      Source.fromPublisher(upstream).groupedWithin(3, 1.second).to(Sink.fromSubscriber(downstream)).run()

      downstream.request(2)

      (1 to 3).foreach(upstream.sendNext)
      downstream.within(1000.millis) {
        downstream.expectNext((1 to 3).toVector)
      }

      upstream.sendComplete()
      downstream.expectComplete()
    }

    "group evenly" taggedAs TimingTest in {
      def script =
        Script(TestConfig.RandomTestRange.map { _ =>
          val x, y, z = random.nextInt(); Seq(x, y, z) -> Seq(immutable.Seq(x, y, z))
        }: _*)
      TestConfig.RandomTestRange.foreach(_ => runScript(script)(_.groupedWithin(3, 10.minutes)))
    }

    "group with rest" taggedAs TimingTest in {
      def script =
        Script((TestConfig.RandomTestRange.map { _ =>
          val x, y, z = random.nextInt(); Seq(x, y, z) -> Seq(immutable.Seq(x, y, z))
        }
        :+ { val x = random.nextInt(); Seq(x) -> Seq(immutable.Seq(x)) }): _*)
      TestConfig.RandomTestRange.foreach(_ => runScript(script)(_.groupedWithin(3, 10.minutes)))
    }

    "group with small groups with backpressure" taggedAs TimingTest in {
      Source(1 to 10)
        .groupedWithin(1, 1.day)
        .throttle(1, 110.millis, 0, ThrottleMode.Shaping)
        .runWith(Sink.seq)
        .futureValue should ===((1 to 10).map(List(_)))
    }

  }

  "A GroupedWeightedWithin" must {
    "handle elements larger than the limit" taggedAs TimingTest in {
      val downstream = TestSubscriber.probe[immutable.Seq[Int]]()
      Source(List(1, 2, 3, 101, 4, 5, 6))
        .groupedWeightedWithin(100, 100.millis)(_.toLong)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.request(1)
      downstream.expectNext((1 to 3).toVector)
      downstream.request(1)
      downstream.expectNext(Vector(101))
      downstream.request(1)
      downstream.expectNext((4 to 6).toVector)
      downstream.expectComplete()
    }

    "not drop a pending last element on upstream finish" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[Long]()
      val downstream = TestSubscriber.probe[immutable.Seq[Long]]()
      Source
        .fromPublisher(upstream)
        .groupedWeightedWithin(5, 50.millis)(identity)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.ensureSubscription()
      downstream.expectNoMessage(100.millis)
      upstream.sendNext(1)
      upstream.sendNext(2)
      upstream.sendNext(3)
      upstream.sendComplete()
      downstream.request(1)
      downstream.expectNext(Vector(1, 2): immutable.Seq[Long])
      downstream.expectNoMessage(100.millis)
      downstream.request(1)
      downstream.expectNext(Vector(3): immutable.Seq[Long])
      downstream.expectComplete()
    }

    "append zero weighted elements to a full group before timeout received, if downstream hasn't pulled yet" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[immutable.Seq[String]]()
      Source
        .fromPublisher(upstream)
        .groupedWeightedWithin(5, 50.millis)(_.length.toLong)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.ensureSubscription()
      upstream.sendNext("333")
      upstream.sendNext("22")
      upstream.sendNext("")
      upstream.sendNext("")
      upstream.sendNext("")
      downstream.request(1)
      downstream.expectNext(Vector("333", "22", "", "", ""): immutable.Seq[String])
      upstream.sendNext("")
      upstream.sendNext("")
      upstream.sendComplete()
      downstream.request(1)
      downstream.expectNext(Vector("", ""): immutable.Seq[String])
      downstream.expectComplete()
    }

    "not emit an empty group if first element is heavier than maxWeight" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[Long]()
      val downstream = TestSubscriber.probe[immutable.Seq[Long]]()
      Source
        .fromPublisher(upstream)
        .groupedWeightedWithin(10, 50.millis)(identity)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.ensureSubscription()
      downstream.request(1)
      upstream.sendNext(11)
      downstream.expectNext(Vector(11): immutable.Seq[Long])
      upstream.sendComplete()
      downstream.expectComplete()
    }

    "handle zero cost function to get only timed based grouping without limit" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[String]()
      val downstream = TestSubscriber.probe[immutable.Seq[String]]()
      Source
        .fromPublisher(upstream)
        .groupedWeightedWithin(1, 100.millis)(ConstantFun.zeroLong)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.ensureSubscription()
      downstream.request(1)
      upstream.sendNext("333")
      upstream.sendNext("22")
      upstream.sendNext("333")
      upstream.sendNext("22")
      downstream.expectNoMessage(50.millis)
      downstream.expectNext(Vector("333", "22", "333", "22"): immutable.Seq[String])
      upstream.sendComplete()
      downstream.expectComplete()
    }

    "group by max weight and max number of elements reached" taggedAs TimingTest in {
      val upstream = TestPublisher.probe[Long]()
      val downstream = TestSubscriber.probe[immutable.Seq[Long]]()
      Source
        .fromPublisher(upstream)
        .groupedWeightedWithin(10, 3, 30.seconds)(identity)
        .to(Sink.fromSubscriber(downstream))
        .run()

      downstream.ensureSubscription()
      upstream.sendNext(1)
      upstream.sendNext(2)
      upstream.sendNext(3)
      upstream.sendNext(4)
      upstream.sendNext(5)
      upstream.sendNext(6)
      upstream.sendNext(11)
      upstream.sendNext(7)
      upstream.sendNext(2)
      upstream.sendComplete()
      downstream.request(1)
      // split because of maxNumber: 3 element
      downstream.expectNext(Vector(1, 2, 3): immutable.Seq[Long])
      downstream.request(1)
      // split because of maxWeight: 9=4+5, one more element did not fit
      downstream.expectNext(Vector(4, 5): immutable.Seq[Long])
      downstream.request(1)
      // split because of maxWeight: 6, one more element did not fit
      downstream.expectNext(Vector(6): immutable.Seq[Long])
      downstream.request(1)
      // split because of maxWeight: 11
      downstream.expectNext(Vector(11): immutable.Seq[Long])
      downstream.request(1)
      // no split
      downstream.expectNext(Vector(7, 2): immutable.Seq[Long])
      downstream.expectComplete()
    }
  }
}
