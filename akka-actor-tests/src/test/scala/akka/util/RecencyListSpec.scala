/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RecencyListSpec extends AnyWordSpec with Matchers {

  private def check(recencyList: RecencyList[String], expectedLeastToMostRecent: List[String]): Unit = {
    expectedLeastToMostRecent.forall(recencyList.contains)
    recencyList.size shouldBe expectedLeastToMostRecent.size
    recencyList.leastToMostRecent.toList shouldBe expectedLeastToMostRecent
    recencyList.mostToLeastRecent.toList shouldBe expectedLeastToMostRecent.reverse
  }

  "RecencyList" must {

    "track recency of elements" in {
      val clock = new TestClock
      val recency = RecencyList[String](clock)

      check(recency, Nil)

      clock.tick() // time = 1
      recency.update("a")
      check(recency, List("a"))

      clock.tick() // time = 2
      recency.update("b").update("c")
      check(recency, List("a", "b", "c"))

      clock.tick() // time = 3
      recency.update("a").update("c")
      check(recency, List("b", "a", "c"))

      clock.tick() // time = 4
      recency.update("d").update("e").update("f").update("g")
      check(recency, List("b", "a", "c", "d", "e", "f", "g"))

      clock.tick() // time = 5
      recency.update("c").update("f")
      check(recency, List("b", "a", "d", "e", "g", "c", "f"))

      clock.tick() // time = 6
      recency.remove("d").remove("g").remove("b").remove("f")
      check(recency, List("a", "e", "c"))

      clock.tick() // time = 7
      recency.update("e").update("h").update("i").update("j")
      check(recency, List("a", "c", "e", "h", "i", "j"))

      clock.tick() // time = 8
      recency.removeLeastRecent(3) shouldBe List("a", "c", "e")
      check(recency, List("h", "i", "j"))

      clock.tick() // time = 9
      recency.update("k").update("l").update("m").update("i")
      check(recency, List("h", "j", "k", "l", "m", "i"))

      clock.tick() // time = 10
      recency.removeMostRecent(3) shouldBe List("i", "m", "l")
      check(recency, List("h", "j", "k"))

      clock.tick() // time = 11
      recency.update("n").update("o")
      check(recency, List("h", "j", "k", "n", "o"))

      clock.tick() // time = 12
      recency.removeLeastRecentOutside(3.seconds) shouldBe List("h", "j")
      check(recency, List("k", "n", "o"))

      clock.tick() // time = 13
      recency.update("p").update("q").update("k").update("r")
      check(recency, List("n", "o", "p", "q", "k", "r"))

      clock.tick() // time = 14
      recency.removeMostRecentWithin(3.seconds) shouldBe List("r", "k", "q", "p")
      check(recency, List("n", "o"))

      clock.tick() // time = 15
      recency.update("s").update("t").update("u").update("v").update("w").update("x").update("y").update("z")
      check(recency, List("n", "o", "s", "t", "u", "v", "w", "x", "y", "z"))

      clock.tick() // time = 16
      recency.removeLeastRecent(3, skip = 3) shouldBe List("t", "u", "v")
      check(recency, List("n", "o", "s", "w", "x", "y", "z"))

      clock.tick() // time = 17
      recency.removeMostRecent(3, skip = 2) shouldBe List("x", "w", "s")
      check(recency, List("n", "o", "y", "z"))

      clock.tick() // time = 18
      recency.removeLeastRecent(10) shouldBe List("n", "o", "y", "z")
      check(recency, Nil)

      clock.tick() // time = 19
      recency.update("a").update("b").update("c")
      check(recency, List("a", "b", "c"))

      clock.tick() // time = 20
      recency.removeMostRecent(10) shouldBe List("c", "b", "a")
      check(recency, Nil)
    }

  }
}
