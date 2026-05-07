/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class HeavyHittersSpec extends AnyWordSpecLike with Matchers {

  "TopHeavyHitters" must {
    "should work" in {
      val hitters = new TopHeavyHitters[String](4)
      hitters.update("A", 10) shouldBe true
      hitters.iterator.toSet should ===(Set("A"))

      hitters.update("B", 20) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B"))

      hitters.update("C", 1) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B", "C"))

      hitters.update("D", 100) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B", "D", "C"))

      hitters.update("E", 200) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B", "D", "E"))

      hitters.update("BB", 22) shouldBe true
      hitters.iterator.toSet should ===(Set("B", "BB", "D", "E"))

      hitters.update("a", 1) shouldBe false
      hitters.iterator.toSet should ===(Set("B", "BB", "D", "E"))
    }

    "correctly replace a hitter" in {
      val hitters = new TopHeavyHitters[String](4)
      hitters.update("A", 10) shouldBe true
      hitters.iterator.toSet should ===(Set("A"))

      hitters.update("A", 12) shouldBe false
      hitters.update("A", 22) shouldBe false
      hitters.iterator.toSet should ===(Set("A"))
    }

    "correctly drop least heavy hitter when more than N are inserted" in {
      val hitters = new TopHeavyHitters[String](4)

      hitters.update("A", 1) shouldBe true
      hitters.iterator.toSet should ===(Set("A"))

      hitters.update("B", 22) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B"))

      hitters.update("C", 33) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B", "C"))
      hitters.lowestHitterWeight should ===(0)

      // first item which forces dropping least heavy hitter
      hitters.update("D", 100) shouldBe true
      hitters.iterator.toSet should ===(Set("A", "B", "C", "D"))

      // second item which forces dropping least heavy hitter
      hitters.update("X", 999) shouldBe true
      hitters.iterator.toSet should ===(Set("X", "B", "C", "D"))
    }

    "replace the right item even when hashCodes collide" in {
      case class MockHashCode(override val toString: String, override val hashCode: Int)
      val hitters = new TopHeavyHitters[MockHashCode](2)

      val a1 = MockHashCode("A", 1)
      val b1 = MockHashCode("B", 1)

      hitters.update(a1, 1)
      hitters.iterator.toSet should ===(Set(a1))
      hitters.lowestHitterWeight should ===(0)

      hitters.update(b1, 2)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(1)

      hitters.update(a1, 10)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(2)

      hitters.update(b1, 100)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(10)
    }

    "replace the right item even when hashCodes collide (and equal to zero)" in {
      case class MockHashCode(override val toString: String, override val hashCode: Int)
      val hitters = new TopHeavyHitters[MockHashCode](2)

      val a1 = MockHashCode("A", 0)
      val b1 = MockHashCode("B", 0)

      hitters.update(a1, 1)
      hitters.iterator.toSet should ===(Set(a1))
      hitters.lowestHitterWeight should ===(0)

      hitters.update(b1, 2)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(1)

      hitters.update(a1, 10)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(2)

      hitters.update(b1, 100)
      hitters.iterator.toSet should ===(Set(a1, b1))
      hitters.lowestHitterWeight should ===(10)
    }

    "behave when something drops from being a hitter and comes back" in {
      val hitters = new TopHeavyHitters[String](2)
      hitters.update("A", 1) should ===(true)
      hitters.update("B", 2) should ===(true)
      hitters.update("C", 3) should ===(true) // A was dropped now
      hitters.update("A", 10) should ===(true) // this is technically unexpected, we have already compressed A...
    }

    "allow updating entries that have lower weight than the least known weight if there is capacity anyway" in {
      val hitters = new TopHeavyHitters[String](2)
      hitters.update("A", 100) should ===(true)
      hitters.update("B", 1) should ===(true)
    }

    "discard zero weight entries" in {
      val hitters = new TopHeavyHitters[String](2)
      hitters.update("A", 0) should ===(false)
      hitters.update("B", 1) should ===(true)
      hitters.update("A", 0) should ===(false)
    }

    "maintain lowest hitter weight" in {
      val hitters = new TopHeavyHitters[String](2)
      hitters.update("A", 1)
      hitters.lowestHitterWeight should ===(0)
      hitters.update("B", 2)
      hitters.lowestHitterWeight should ===(1)
      hitters.update("A", 2)
      hitters.lowestHitterWeight should ===(2)
      hitters.update("A", 3)
      hitters.lowestHitterWeight should ===(2)
      hitters.update("B", 4)
      hitters.lowestHitterWeight should ===(3)
    }

    "kick out smallest hitter if full" in {
      val hitters = new TopHeavyHitters[String](2)
      hitters.update("A", 1)
      hitters.lowestHitterWeight should ===(0)
      hitters.update("B", 2)
      hitters.lowestHitterWeight should ===(1)
      hitters.update("C", 3)
      hitters.lowestHitterWeight should ===(2)
      hitters.update("B", 4)
      hitters.lowestHitterWeight should ===(3)
    }

    "be disabled with max=0" in {
      val hitters = new TopHeavyHitters[String](0)
      hitters.update("A", 10) shouldBe true
      hitters.iterator.toSet should ===(Set.empty)

      hitters.update("B", 5) shouldBe false
      hitters.update("C", 15) shouldBe true
      hitters.iterator.toSet should ===(Set.empty)
    }

  }
}
