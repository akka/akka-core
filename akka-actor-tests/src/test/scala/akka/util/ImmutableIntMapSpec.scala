/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.util.Random

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ImmutableIntMapSpec extends AnyWordSpec with Matchers {

  "ImmutableIntMap" must {

    "have no entries when empty" in {
      val empty = ImmutableIntMap.empty
      empty.size should be(0)
      empty.keysIterator.toList should be(Nil)
    }

    "add and get entries" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10)
      m1.keysIterator.toList should be(List(10))
      m1.keysIterator.map(m1.get).toList should be(List(10))

      val m2 = m1.updated(20, 20)
      m2.keysIterator.toList should be(List(10, 20))
      m2.keysIterator.map(m2.get).toList should be(List(10, 20))

      val m3 = m1.updated(5, 5)
      m3.keysIterator.toList should be(List(5, 10))
      m3.keysIterator.map(m3.get).toList should be(List(5, 10))

      val m4 = m2.updated(5, 5)
      m4.keysIterator.toList should be(List(5, 10, 20))
      m4.keysIterator.map(m4.get).toList should be(List(5, 10, 20))

      val m5 = m4.updated(15, 15)
      m5.keysIterator.toList should be(List(5, 10, 15, 20))
      m5.keysIterator.map(m5.get).toList should be(List(5, 10, 15, 20))
    }

    "replace entries" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10).updated(10, 11)
      m1.keysIterator.map(m1.get).toList should be(List(11))

      val m2 = m1.updated(20, 20).updated(30, 30).updated(20, 21).updated(30, 31)
      m2.keysIterator.map(m2.get).toList should be(List(11, 21, 31))
    }

    "update if absent" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10).updated(20, 11)
      m1.updateIfAbsent(10, 15) should be(ImmutableIntMap.empty.updated(10, 10).updated(20, 11))
      m1.updateIfAbsent(30, 12) should be(ImmutableIntMap.empty.updated(10, 10).updated(20, 11).updated(30, 12))
    }

    "have toString" in {
      ImmutableIntMap.empty.toString should be("ImmutableIntMap()")
      ImmutableIntMap.empty.updated(10, 10).toString should be("ImmutableIntMap(10 -> 10)")
      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).toString should be("ImmutableIntMap(10 -> 10, 20 -> 20)")
    }

    "have equals and hashCode" in {
      ImmutableIntMap.empty.updated(10, 10) should be(ImmutableIntMap.empty.updated(10, 10))
      ImmutableIntMap.empty.updated(10, 10).hashCode should be(ImmutableIntMap.empty.updated(10, 10).hashCode)

      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30) should be(
        ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30))
      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30).hashCode should be(
        ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30).hashCode)

      ImmutableIntMap.empty.updated(10, 10).updated(20, 20) should not be ImmutableIntMap.empty.updated(10, 10)

      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30) should not be
      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 31)

      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30) should not be
      ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(31, 30)

      ImmutableIntMap.empty should be(ImmutableIntMap.empty)
      ImmutableIntMap.empty.hashCode should be(ImmutableIntMap.empty.hashCode)
    }

    "remove entries" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30)

      val m2 = m1.remove(10)
      m2.keysIterator.map(m2.get).toList should be(List(20, 30))

      val m3 = m1.remove(20)
      m3.keysIterator.map(m3.get).toList should be(List(10, 30))

      val m4 = m1.remove(30)
      m4.keysIterator.map(m4.get).toList should be(List(10, 20))

      m1.remove(5) should be(m1)

      m1.remove(10).remove(20).remove(30) should be(ImmutableIntMap.empty)
    }

    "get None when entry doesn't exist" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30)
      m1.get(5) should be(Int.MinValue)
      m1.get(15) should be(Int.MinValue)
      m1.get(25) should be(Int.MinValue)
      m1.get(35) should be(Int.MinValue)
    }

    "contain keys" in {
      val m1 = ImmutableIntMap.empty.updated(10, 10).updated(20, 20).updated(30, 30)
      m1.contains(10) should be(true)
      m1.contains(20) should be(true)
      m1.contains(30) should be(true)
      m1.contains(5) should be(false)
      m1.contains(25) should be(false)
    }

    "have correct behavior for random operations" in {
      val seed = System.nanoTime()
      val rnd = new Random(seed)

      var longMap = ImmutableIntMap.empty
      var reference = Map.empty[Long, Int]

      def verify(): Unit = {
        val m = longMap.keysIterator.map(key => key -> longMap.get(key)).toMap

        m should be(reference)
      }

      (1 to 1000).foreach { i =>
        withClue(s"seed=$seed, iteration=$i") {
          val key = rnd.nextInt(100)
          val value = rnd.nextPrintableChar()
          rnd.nextInt(3) match {
            case 0 | 1 =>
              longMap = longMap.updated(key, value)
              reference = reference.updated(key, value)
            case 2 =>
              longMap = longMap.remove(key)
              reference = reference - key
          }
          verify()
        }
      }
    }

  }
}
