/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SegmentedRecencyListSpec extends AnyWordSpec with Matchers {

  private def check(recency: SegmentedRecencyList[String], expectedSegments: List[List[String]]): Unit = {
    recency.size shouldBe expectedSegments.map(_.size).sum
    expectedSegments.zipWithIndex.foreach {
      case (expectedSegment, level) =>
        expectedSegment.forall(recency.contains)
        recency.leastToMostRecentOf(level).toList shouldBe expectedSegment
        recency.sizeOf(level) shouldBe expectedSegment.size
    }
  }

  "SegmentedRecencyList" must {

    "track recency of elements across 2 segments" in {
      val recency = SegmentedRecencyList.empty[String](limits = List(2, 3))

      check(recency, Nil)

      recency.update("a").update("b").update("c").update("d").update("e").update("f")
      check(recency, List(List("a", "b", "c", "d", "e", "f"), Nil))

      recency.removeLeastRecentOverLimit() shouldBe List("a")
      check(recency, List(List("b", "c", "d", "e", "f"), Nil))

      recency.update("a").update("b").update("c").update("d")
      check(recency, List(List("e", "f", "a"), List("b", "c", "d")))

      recency.removeLeastRecentOverLimit() shouldBe List("e")
      check(recency, List(List("f", "a"), List("b", "c", "d")))

      recency.update("a")
      check(recency, List(List("f", "b"), List("c", "d", "a")))

      recency.removeLeastRecentOverLimit() shouldBe Nil
      check(recency, List(List("f", "b"), List("c", "d", "a")))

      recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
      check(recency, List(List("a", "e", "b", "g"), List("c", "d", "f")))

      recency.removeLeastRecentOverLimit() shouldBe List("a", "e")
      check(recency, List(List("b", "g"), List("c", "d", "f")))
    }

    "track recency of elements across 4 segments" in {
      val recency = SegmentedRecencyList.empty[String](limits = List(4, 3, 2, 1))

      check(recency, Nil)

      recency.update("a").update("b").update("c").update("d").update("e").update("f")
      check(recency, List(List("a", "b", "c", "d", "e", "f"), Nil, Nil, Nil))

      recency.removeLeastRecentOverLimit() shouldBe Nil
      check(recency, List(List("a", "b", "c", "d", "e", "f"), Nil, Nil, Nil))

      recency.update("g").update("h").update("i").update("j").update("k").update("l")
      check(recency, List(List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"), Nil, Nil, Nil))

      recency.removeLeastRecentOverLimit() shouldBe List("a", "b")
      check(recency, List(List("c", "d", "e", "f", "g", "h", "i", "j", "k", "l"), Nil, Nil, Nil))

      recency.update("b").update("d").update("f").update("h").update("j").update("l")
      check(recency, List(List("c", "e", "g", "i", "k", "b", "d", "f"), List("h", "j", "l"), Nil, Nil))

      recency.update("h").update("i").update("j").update("k").update("l").update("m")
      check(recency, List(List("c", "e", "g", "b", "d", "f", "m"), List("i", "k", "h"), List("j", "l"), Nil))

      recency.removeLeastRecentOverLimit() shouldBe List("c", "e")
      check(recency, List(List("g", "b", "d", "f", "m"), List("i", "k", "h"), List("j", "l"), Nil))

      recency.update("j").update("k").update("l").update("m").update("n").update("o").update("p")
      check(recency, List(List("g", "b", "d", "f", "n", "o", "p"), List("i", "h", "m"), List("k", "j"), List("l")))

      recency.removeLeastRecentOverLimit() shouldBe List("g", "b", "d")
      check(recency, List(List("f", "n", "o", "p"), List("i", "h", "m"), List("k", "j"), List("l")))
    }

    "allow protected limits to be updated" in {
      val recency = SegmentedRecencyList.empty[String](limits = List(2, 8))

      check(recency, Nil)

      recency.update("a").update("b").update("c").update("d").update("e").update("f")
      recency.update("g").update("h").update("i").update("j").update("k").update("l")
      check(recency, List(List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"), Nil))

      recency.update("a").update("b").update("c").update("d").update("e").update("f")
      recency.update("g").update("h").update("i").update("j").update("k").update("l")
      check(recency, List(List("a", "b", "c", "d"), List("e", "f", "g", "h", "i", "j", "k", "l")))

      recency.removeLeastRecentOverLimit() shouldBe List("a", "b")
      check(recency, List(List("c", "d"), List("e", "f", "g", "h", "i", "j", "k", "l")))

      recency.updateLimits(newLimits = List(1, 4))

      recency.removeLeastRecentOverLimit() shouldBe List("c", "d", "e", "f", "g")
      check(recency, List(List("h"), List("i", "j", "k", "l")))
    }

    "remove overall least recent elements" in {
      val clock = new TestClock
      val recency = new SegmentedRecencyList[String](initialLimits = List(5, 5), OptionVal.Some(clock))

      check(recency, Nil)

      clock.tick() // time = 1
      recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
      check(recency, List(List("a", "b", "c", "d", "e", "f", "g"), Nil))

      clock.tick() // time = 2
      recency.update("a").update("b").update("c").update("d").update("e").update("f").update("g")
      check(recency, List(List("a", "b"), List("c", "d", "e", "f", "g")))

      clock.tick() // time = 3
      recency.update("h").update("i").update("j").update("k").update("j").update("k")
      check(recency, List(List("a", "b", "h", "i", "c", "d"), List("e", "f", "g", "j", "k")))

      clock.tick() // time = 4
      recency.update("l").update("m").update("n").update("n").update("o")
      check(recency, List(List("a", "b", "h", "i", "c", "d", "l", "m", "e", "o"), List("f", "g", "j", "k", "n")))

      clock.tick() // time = 5
      recency.removeOverallLeastRecentOutside(2.seconds) shouldBe List("a", "b", "c", "d", "e", "f", "g")
      check(recency, List(List("h", "i", "l", "m", "o"), List("j", "k", "n")))
    }
  }
}
