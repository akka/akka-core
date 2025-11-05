/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FrequencyListSpec extends AnyWordSpec with Matchers {

  private def check(frequencyList: FrequencyList[String], expectedLeastToMostFrequent: List[String]): Unit = {
    expectedLeastToMostFrequent.forall(frequencyList.contains)
    frequencyList.size shouldBe expectedLeastToMostFrequent.size
    frequencyList.leastToMostFrequent.toList shouldBe expectedLeastToMostFrequent
    frequencyList.mostToLeastFrequent.toList shouldBe expectedLeastToMostFrequent.reverse
  }

  private def checkRecency(frequencyList: FrequencyList[String], expectedLeastToMostRecent: List[String]): Unit = {
    expectedLeastToMostRecent.forall(frequencyList.contains)
    frequencyList.size shouldBe expectedLeastToMostRecent.size
    frequencyList.overallLeastToMostRecent.toList shouldBe expectedLeastToMostRecent
    frequencyList.overallMostToLeastRecent.toList shouldBe expectedLeastToMostRecent.reverse
  }

  "FrequencyList" must {

    "track frequency of elements" in {
      val frequency = FrequencyList.empty[String]()

      check(frequency, Nil)

      frequency.update("a")
      check(frequency, List( /* 1: */ "a"))

      frequency.update("b").update("c")
      check(frequency, List( /* 1: */ "a", "b", "c"))

      frequency.update("a").update("c")
      check(frequency, List( /* 1: */ "b", /* 2: */ "a", "c"))

      frequency.update("d").update("e").update("f").update("g")
      check(frequency, List( /* 1: */ "b", "d", "e", "f", "g", /* 2: */ "a", "c"))

      frequency.update("c").update("f")
      check(frequency, List( /* 1: */ "b", "d", "e", "g", /* 2: */ "a", "f", /* 3: */ "c"))

      frequency.remove("d").remove("g").remove("b").remove("f")
      check(frequency, List( /* 1: */ "e", /* 2: */ "a", /* 3: */ "c"))

      frequency.update("e").update("h").update("i")
      check(frequency, List( /* 1: */ "h", "i", /* 2: */ "a", "e", /* 3: */ "c"))

      frequency.removeLeastFrequent(3) shouldBe List("h", "i", "a")
      check(frequency, List( /* 2: */ "e", /* 3: */ "c"))

      frequency.update("j").update("k").update("l").update("m")
      check(frequency, List( /* 1: */ "j", "k", "l", "m", /* 2: */ "e", /* 3: */ "c"))

      frequency.removeLeastFrequent(skip = OptionVal.Some("j")) shouldBe List("k")
      check(frequency, List( /* 1: */ "j", "l", "m", /* 2: */ "e", /* 3: */ "c"))

      frequency.removeLeastFrequent(2, skip = OptionVal.Some("l")) shouldBe List("j", "m")
      check(frequency, List( /* 1: */ "l", /* 2: */ "e", /* 3: */ "c"))

      frequency.update("n").update("o").update("p").update("e").update("o").update("l")
      check(frequency, List( /* 1: */ "n", "p", /* 2: */ "o", "l", /* 3: */ "c", "e"))

      frequency.removeMostFrequent(3) shouldBe List("e", "c", "l")
      check(frequency, List( /* 1: */ "n", "p", /* 2: */ "o"))

      frequency.update("q").update("r").update("p").update("o").update("n")
      check(frequency, List( /* 1: */ "q", "r", /* 2: */ "p", "n", /* 3: */ "o"))

      frequency.removeMostFrequent(skip = OptionVal.Some("o")) shouldBe List("n")
      check(frequency, List( /* 1: */ "q", "r", /* 2: */ "p", /* 3: */ "o"))

      frequency.removeMostFrequent(2, skip = OptionVal.Some("p")) shouldBe List("o", "r")
      check(frequency, List( /* 1: */ "q", /* 2: */ "p"))
    }

    "track overall recency of elements when enabled" in {
      val clock = new TestClock
      val frequency = new FrequencyList[String](dynamicAging = false, OptionVal.Some(clock))

      check(frequency, Nil)

      clock.tick() // time = 1
      frequency.update("a")
      check(frequency, List( /* 1: */ "a"))
      checkRecency(frequency, List("a"))

      clock.tick() // time = 2
      frequency.update("b").update("c")
      check(frequency, List( /* 1: */ "a", "b", "c"))
      checkRecency(frequency, List("a", "b", "c"))

      clock.tick() // time = 3
      frequency.update("a").update("c")
      check(frequency, List( /* 1: */ "b", /* 2: */ "a", "c"))
      checkRecency(frequency, List("b", "a", "c"))

      clock.tick() // time = 4
      frequency.update("d").update("e").update("f")
      check(frequency, List( /* 1: */ "b", "d", "e", "f", /* 2: */ "a", "c"))
      checkRecency(frequency, List("b", "a", "c", "d", "e", "f"))

      clock.tick() // time = 5
      frequency.update("c").update("f")
      check(frequency, List( /* 1: */ "b", "d", "e", /* 2: */ "a", "f", /* 3: */ "c"))
      checkRecency(frequency, List("b", "a", "d", "e", "c", "f"))

      clock.tick() // time = 6
      frequency.remove("d").remove("b").remove("f")
      check(frequency, List( /* 1: */ "e", /* 2: */ "a", /* 3: */ "c"))
      checkRecency(frequency, List("a", "e", "c"))

      clock.tick() // time = 7
      frequency.update("e").update("h").update("i")
      check(frequency, List( /* 1: */ "h", "i", /* 2: */ "a", "e", /* 3: */ "c"))
      checkRecency(frequency, List("a", "c", "e", "h", "i"))

      clock.tick() // time = 8
      frequency.removeOverallLeastRecent() shouldBe List("a")
      check(frequency, List( /* 1: */ "h", "i", /* 2: */ "e", /* 3: */ "c"))
      checkRecency(frequency, List("c", "e", "h", "i"))

      clock.tick() // time = 9
      frequency.update("i").update("j").update("k")
      check(frequency, List( /* 1: */ "h", "j", "k", /* 2: */ "e", "i", /* 3: */ "c"))
      checkRecency(frequency, List("c", "e", "h", "i", "j", "k"))

      clock.tick() // time = 10
      frequency.removeOverallMostRecent() shouldBe List("k")
      check(frequency, List( /* 1: */ "h", "j", /* 2: */ "e", "i", /* 3: */ "c"))
      checkRecency(frequency, List("c", "e", "h", "i", "j"))

      clock.tick() // time = 11
      frequency.removeOverallLeastRecentOutside(3.seconds) shouldBe List("c", "e", "h")
      check(frequency, List( /* 1: */ "j", /* 2: */ "i"))
      checkRecency(frequency, List("i", "j"))

      clock.tick() // time = 12
      frequency.update("l").update("m")
      check(frequency, List( /* 1: */ "j", "l", "m", /* 2: */ "i"))
      checkRecency(frequency, List("i", "j", "l", "m"))

      clock.tick() // time = 13
      frequency.removeOverallMostRecentWithin(3.seconds) shouldBe List("m", "l")
      check(frequency, List( /* 1: */ "j", /* 2: */ "i"))
      checkRecency(frequency, List("i", "j"))

      clock.tick() // time = 14
      frequency.update("n").update("o").update("n")
      check(frequency, List( /* 1: */ "j", "o", /* 2: */ "i", "n"))
      checkRecency(frequency, List("i", "j", "o", "n"))
    }

    "support dynamic aging with least frequently used eviction" in {
      val regular = FrequencyList.empty[String]()
      val aging = FrequencyList.empty[String](dynamicAging = true)

      check(regular, Nil)
      check(aging, Nil)

      for (_ <- 1 to 10) regular.update("a").update("b").update("c")
      check(regular, List( /*10*/ "a", "b", "c"))

      for (_ <- 1 to 10) aging.update("a").update("b").update("c")
      check(aging, List( /*10+0*/ "a", "b", "c"))

      // age = 0

      regular.update("x").update("y").update("z")
      check(regular, List( /*1*/ "x", "y", "z", /*10*/ "a", "b", "c"))

      aging.update("x").update("y").update("z")
      check(aging, List( /*1+0*/ "x", "y", "z", /*10+0*/ "a", "b", "c"))

      regular.removeLeastFrequent() shouldBe List("x")
      check(regular, List( /*1*/ "y", "z", /*10*/ "a", "b", "c"))

      aging.removeLeastFrequent() shouldBe List("x")
      check(aging, List( /*1+0*/ "y", "z", /*10+0*/ "a", "b", "c"))

      // age = 1 (from last removal of "x")

      regular.update("x").update("y").update("z").update("z")
      check(regular, List( /*1*/ "x", /*2*/ "y", /*3*/ "z", /*10*/ "a", "b", "c"))

      aging.update("x").update("y").update("z").update("z")
      check(aging, List( /*1+1*/ "x", /*2+1*/ "y", /*3+1*/ "z", /*10+0*/ "a", "b", "c"))

      regular.removeLeastFrequent(2) shouldBe List("x", "y")
      check(regular, List( /*3*/ "z", /*10*/ "a", "b", "c"))

      aging.removeLeastFrequent(2) shouldBe List("x", "y")
      check(aging, List( /*3+1*/ "z", /*10+0*/ "a", "b", "c"))

      // age = 3 (from last removal of "y")

      regular.update("x").update("y").update("z")
      check(regular, List( /*1*/ "x", "y", /*4*/ "z", /*10*/ "a", "b", "c"))

      aging.update("x").update("y").update("z")
      check(aging, List( /*1+3*/ "x", "y", /*4+3*/ "z", /*10+0*/ "a", "b", "c"))

      regular.removeLeastFrequent(3) shouldBe List("x", "y", "z")
      check(regular, List( /*10*/ "a", "b", "c"))

      aging.removeLeastFrequent(3) shouldBe List("x", "y", "z")
      check(aging, List( /*10+0*/ "a", "b", "c"))

      // age = 7 (from last removal of "z")

      regular.update("x").update("y").update("y").update("z").update("z").update("z")
      check(regular, List( /*1*/ "x", /*2*/ "y", /*3*/ "z", /*10*/ "a", "b", "c"))

      aging.update("x").update("y").update("y").update("z").update("z").update("z")
      check(aging, List( /*1+7*/ "x", /*2+7*/ "y", /*10+0*/ "a", "b", "c", /*3+7*/ "z"))

      regular.removeLeastFrequent(2) shouldBe List("x", "y")
      check(regular, List( /*3*/ "z", /*10*/ "a", "b", "c"))

      aging.removeLeastFrequent(2) shouldBe List("x", "y")
      check(aging, List( /*10+0*/ "a", "b", "c", /*3+7*/ "z"))

      // age = 9 (from last removal of "y")

      regular.update("x").update("y").update("z")
      check(regular, List( /*1*/ "x", "y", /*4*/ "z", /*10*/ "a", "b", "c"))

      aging.update("x").update("y").update("z")
      check(aging, List( /*10+0*/ "a", "b", "c", /*1+9*/ "x", "y", /*4+9*/ "z"))

      regular.removeLeastFrequent(3) shouldBe List("x", "y", "z")
      check(regular, List( /*10*/ "a", "b", "c"))

      aging.removeLeastFrequent(3) shouldBe List("a", "b", "c")
      check(aging, List( /*1+9*/ "x", "y", /*4+9*/ "z"))

      // age = 10 (from last removal of "c")
    }

  }
}
