/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.Attributes.Attribute
import akka.stream.testkit.{ StreamSpec, TestSubscriber }
import akka.stream.testkit.TestSubscriber.ManualProbe

class GraphMergePrioritizedNSpec extends StreamSpec {

  "merge prioritized" must {

    "stream data from all sources" in {
      val source1 = Source.fromIterator(() => (1 to 3).iterator)
      val source2 = Source.fromIterator(() => (4 to 6).iterator)
      val source3 = Source.fromIterator(() => (7 to 9).iterator)

      val sourcesAndPriorities = List((source1, 6), (source2, 3), (source3, 1))
      val probe = TestSubscriber.manualProbe[Int]()
      threeSourceMerge(sourcesAndPriorities, probe).run()

      val subscription = probe.expectSubscription()

      var collected = Vector.empty[Int]
      for (_ <- 1 to 9) {
        subscription.request(1)
        collected :+= probe.expectNext()
      }

      collected.toSet should be(Set(1, 2, 3, 4, 5, 6, 7, 8, 9))
      probe.expectComplete()
    }

    "stream data with priority" in {
      val elementCount = 20000
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator(() => Iterator.continually(2).take(elementCount))
      val source3 = Source.fromIterator(() => Iterator.continually(3).take(elementCount))

      val sourcesAndPriorities = List((source1, 6), (source2, 3), (source3, 1));

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(sourcesAndPriorities, probe).run()

      val subscription = probe.expectSubscription()

      val builder = Vector.newBuilder[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        builder += probe.expectNext()
      }
      val collected = builder.result()

      val ones = collected.count(_ == 1).toDouble
      val twos = collected.count(_ == 2).toDouble
      val threes = collected.count(_ == 3).toDouble

      (ones / twos) should ===(2d +- 1)
      (ones / threes) should ===(6d +- 1)
      (twos / threes) should ===(3d +- 1)
    }

    "stream data when only one source produces" in {
      val elementCount = 10
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator[Int](() => Iterator.empty)
      val source3 = Source.fromIterator[Int](() => Iterator.empty)

      val sourcesAndPriorities = List((source1, 6), (source2, 3), (source3, 1))

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(sourcesAndPriorities, probe).run()

      val subscription = probe.expectSubscription()

      var collected = Vector.empty[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        collected :+= probe.expectNext()
      }

      val ones = collected.count(_ == 1)
      val twos = collected.count(_ == 2)
      val threes = collected.count(_ == 3)

      ones shouldEqual elementCount
      twos shouldEqual 0
      threes shouldEqual 0
    }

    "stream data with priority when only two sources produce" in {
      val elementCount = 20000
      val source1 = Source.fromIterator(() => Iterator.continually(1).take(elementCount))
      val source2 = Source.fromIterator(() => Iterator.continually(2).take(elementCount))
      val source3 = Source.fromIterator[Int](() => Iterator.empty)

      val sourcesAndPriorities = List((source1, 6), (source2, 3), (source3, 1))

      val probe = TestSubscriber.manualProbe[Int]()

      threeSourceMerge(sourcesAndPriorities, probe).run()

      val subscription = probe.expectSubscription()

      val builder = Vector.newBuilder[Int]
      for (_ <- 1 to elementCount) {
        subscription.request(1)
        builder += probe.expectNext()
      }
      val collected = builder.result()

      val ones = collected.count(_ == 1).toDouble
      val twos = collected.count(_ == 2).toDouble
      val threes = collected.count(_ == 3)

      threes shouldEqual 0
      (ones / twos) should ===(2d +- 1)
    }
  }

  private def threeSourceMerge[T](
      sourceAndPriorities: immutable.Seq[(Source[T, NotUsed], Int)],
      probe: ManualProbe[T]) = {

    Source
      .mergePrioritizedN(sourceAndPriorities, eagerComplete = false)
      .initialDelay(50.millis)
      .to(Sink.fromSubscriber(probe))
  }

  "get Priority from graph" in {
    val elementCount = 10
    case class MyPriority(priority: Int) extends Attribute

    val myAttributes1 = Attributes(MyPriority(6))
    val myAttributes2 = Attributes(MyPriority(3))
    val myAttributes3 = Attributes(MyPriority(1))

    val defaultPriority = MyPriority(-1)

    val source1: Source[Int, NotUsed] =
      Source.fromIterator(() => Iterator.continually(1).take(elementCount)).addAttributes(myAttributes1)
    val source2 = Source.fromIterator[Int](() => Iterator.empty).addAttributes(myAttributes2)
    val source3: Source[Int, NotUsed] = Source.fromIterator[Int](() => Iterator.empty).addAttributes(myAttributes3)

    val sourcesAndPriorities = List(
      (source1, source1.getAttributes.get[MyPriority](defaultPriority).priority),
      (source2, source2.getAttributes.get[MyPriority](defaultPriority).priority),
      (source3, source3.getAttributes.get[MyPriority](defaultPriority).priority))

    val probe = TestSubscriber.manualProbe[Int]()

    threeSourceMerge(sourcesAndPriorities, probe).run()

    val subscription = probe.expectSubscription()

    var collected = Vector.empty[Int]
    for (_ <- 1 to elementCount) {
      subscription.request(1)
      collected :+= probe.expectNext()
    }

    val ones = collected.count(_ == 1)
    val twos = collected.count(_ == 2)
    val threes = collected.count(_ == 3)

    ones shouldEqual elementCount
    twos shouldEqual 0
    threes shouldEqual 0
  }
}
