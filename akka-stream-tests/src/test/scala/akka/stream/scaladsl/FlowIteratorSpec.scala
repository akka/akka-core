/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream.testkit._

class FlowIteratorSpec extends AbstractFlowIteratorSpec {
  override def testName = "A Flow based on an iterator producing function"
  override def createSource(elements: Int): Source[Int, NotUsed] =
    Source.fromIterator(() => (1 to elements).iterator)
}

class FlowIterableSpec extends AbstractFlowIteratorSpec {
  override def testName = "A Flow based on an iterable"
  override def createSource(elements: Int): Source[Int, NotUsed] =
    Source(1 to elements)

  "produce onError when iterator throws" in {
    val iterable = new immutable.Iterable[Int] {
      override def iterator: Iterator[Int] =
        (1 to 3).iterator.map(x => if (x == 2) throw new IllegalStateException("not two") else x)
    }
    val p = Source(iterable).runWith(Sink.asPublisher(false))
    val c = TestSubscriber.manualProbe[Int]()
    p.subscribe(c)
    val sub = c.expectSubscription()
    sub.request(1)
    c.expectNext(1)
    c.expectNoMessage(100.millis)
    sub.request(2)
    c.expectError().getMessage should be("not two")
    sub.request(2)
    c.expectNoMessage(100.millis)
  }

  "produce onError when Source construction throws" in {
    val iterable = new immutable.Iterable[Int] {
      override def iterator: Iterator[Int] = throw new IllegalStateException("no good iterator")
    }
    val p = Source(iterable).runWith(Sink.asPublisher(false))
    val c = TestSubscriber.manualProbe[Int]()
    p.subscribe(c)
    c.expectSubscriptionAndError().getMessage should be("no good iterator")
    c.expectNoMessage(100.millis)
  }

  "produce onError when hasNext throws" in {
    val iterable = new immutable.Iterable[Int] {
      override def iterator: Iterator[Int] = new Iterator[Int] {
        override def hasNext: Boolean = throw new IllegalStateException("no next")
        override def next(): Int = -1
      }
    }
    val p = Source(iterable).runWith(Sink.asPublisher(false))
    val c = TestSubscriber.manualProbe[Int]()
    p.subscribe(c)
    c.expectSubscriptionAndError().getMessage should be("no next")
    c.expectNoMessage(100.millis)
  }
}

abstract class AbstractFlowIteratorSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
""") {

  def testName: String

  def createSource(elements: Int): Source[Int, NotUsed]

  testName must {
    "produce elements" in {
      val p = createSource(3).runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[Int]()
      p.subscribe(c)
      val sub = c.expectSubscription()
      sub.request(1)
      c.expectNext(1)
      c.expectNoMessage(100.millis)
      sub.request(3)
      c.expectNext(2)
      c.expectNext(3)
      c.expectComplete()
    }

    "complete empty" in {
      val p = createSource(0).runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[Int]()
      p.subscribe(c)
      c.expectSubscriptionAndComplete()
      c.expectNoMessage(100.millis)
    }

    "produce elements with multiple subscribers" in {
      val p = createSource(3).runWith(Sink.asPublisher(true))
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()
      p.subscribe(c1)
      p.subscribe(c2)
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      sub2.request(2)
      c1.expectNext(1)
      c2.expectNext(1)
      c2.expectNext(2)
      c1.expectNoMessage(100.millis)
      c2.expectNoMessage(100.millis)
      sub1.request(2)
      sub2.request(2)
      c1.expectNext(2)
      c1.expectNext(3)
      c2.expectNext(3)
      c1.expectComplete()
      c2.expectComplete()
    }

    "produce elements to later subscriber" in {
      val p = createSource(3).runWith(Sink.asPublisher(true))
      val c1 = TestSubscriber.manualProbe[Int]()
      val c2 = TestSubscriber.manualProbe[Int]()
      p.subscribe(c1)

      val sub1 = c1.expectSubscription()
      sub1.request(1)
      c1.expectNext(1)
      c1.expectNoMessage(100.millis)
      p.subscribe(c2)
      val sub2 = c2.expectSubscription()
      sub2.request(3)
      // element 1 is already gone
      c2.expectNext(2)
      c2.expectNext(3)
      c2.expectComplete()
      sub1.request(3)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
    }

    "produce elements with one transformation step" in {
      val p = createSource(3).map(_ * 2).runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[Int]()
      p.subscribe(c)
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectNext(2)
      c.expectNext(4)
      c.expectNext(6)
      c.expectComplete()
    }

    "produce elements with two transformation steps" in {
      val p = createSource(4).filter(_ % 2 == 0).map(_ * 2).runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[Int]()
      p.subscribe(c)
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectNext(4)
      c.expectNext(8)
      c.expectComplete()
    }

    "not produce after cancel" in {
      val p = createSource(3).runWith(Sink.asPublisher(false))
      val c = TestSubscriber.manualProbe[Int]()
      p.subscribe(c)
      val sub = c.expectSubscription()
      sub.request(1)
      c.expectNext(1)
      sub.cancel()
      sub.request(2)
      c.expectNoMessage(100.millis)
    }

  }
}
