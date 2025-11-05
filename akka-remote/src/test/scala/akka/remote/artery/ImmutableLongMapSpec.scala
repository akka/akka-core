/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.util.Random

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.util.OptionVal

class ImmutableLongMapSpec extends AnyWordSpec with Matchers {

  "ImmutableLongMap" must {

    "have no entries when empty" in {
      val empty = ImmutableLongMap.empty[String]
      empty.size should be(0)
      empty.keysIterator.toList should be(Nil)
    }

    "add and get entries" in {
      val m1 = ImmutableLongMap.empty[String].updated(10L, "10")
      m1.keysIterator.toList should be(List(10L))
      m1.keysIterator.map(m1.get).toList should be(List(OptionVal("10")))

      val m2 = m1.updated(20L, "20")
      m2.keysIterator.toList should be(List(10L, 20L))
      m2.keysIterator.map(m2.get).toList should be(List(OptionVal("10"), OptionVal("20")))

      val m3 = m1.updated(5L, "5")
      m3.keysIterator.toList should be(List(5L, 10L))
      m3.keysIterator.map(m3.get).toList should be(List(OptionVal("5"), OptionVal("10")))

      val m4 = m2.updated(5L, "5")
      m4.keysIterator.toList should be(List(5L, 10L, 20L))
      m4.keysIterator.map(m4.get).toList should be(List(OptionVal("5"), OptionVal("10"), OptionVal("20")))

      val m5 = m4.updated(15L, "15")
      m5.keysIterator.toList should be(List(5L, 10L, 15L, 20L))
      m5.keysIterator.map(m5.get).toList should be(
        List(OptionVal("5"), OptionVal("10"), OptionVal("15"), OptionVal("20")))
    }

    "replace entries" in {
      val m1 = ImmutableLongMap.empty[String].updated(10L, "10a").updated(10, "10b")
      m1.keysIterator.map(m1.get).toList should be(List(OptionVal("10b")))

      val m2 = m1.updated(20L, "20a").updated(30L, "30a").updated(20L, "20b").updated(30L, "30b")
      m2.keysIterator.map(m2.get).toList should be(List(OptionVal("10b"), OptionVal("20b"), OptionVal("30b")))
    }

    "have toString" in {
      ImmutableLongMap.empty[String].toString should be("ImmutableLongMap()")
      ImmutableLongMap.empty[String].updated(10L, "a").toString should be("ImmutableLongMap(10 -> a)")
      ImmutableLongMap.empty[String].updated(10L, "a").updated(20, "b").toString should be(
        "ImmutableLongMap(10 -> a, 20 -> b)")
    }

    "have equals and hashCode" in {
      ImmutableLongMap.empty[String].updated(10L, "10") should be(ImmutableLongMap.empty[String].updated(10L, "10"))
      ImmutableLongMap.empty[String].updated(10L, "10").hashCode should be(
        ImmutableLongMap.empty[String].updated(10L, "10").hashCode)

      ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20").updated(30, "30") should be(
        ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20").updated(30, "30"))
      ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20").updated(30, "30").hashCode should be(
        ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20").updated(30, "30").hashCode)

      ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20") should not be (ImmutableLongMap
        .empty[String]
        .updated(10L, "10"))

      ImmutableLongMap
        .empty[String]
        .updated(10L, "10")
        .updated(20, "20")
        .updated(30, "30") should not be (ImmutableLongMap
        .empty[String]
        .updated(10L, "10")
        .updated(20, "20b")
        .updated(30, "30"))

      ImmutableLongMap
        .empty[String]
        .updated(10L, "10")
        .updated(20, "20")
        .updated(30, "30") should not be (ImmutableLongMap
        .empty[String]
        .updated(10L, "10")
        .updated(20, "20b")
        .updated(31, "30"))

      ImmutableLongMap.empty[String] should be(ImmutableLongMap.empty[String])
      ImmutableLongMap.empty[String].hashCode should be(ImmutableLongMap.empty[String].hashCode)
    }

    "remove entries" in {
      val m1 = ImmutableLongMap.empty[String].updated(10L, "10").updated(20, "20").updated(30, "30")

      val m2 = m1.remove(10L)
      m2.keysIterator.map(m2.get).toList should be(List(OptionVal("20"), OptionVal("30")))

      val m3 = m1.remove(20L)
      m3.keysIterator.map(m3.get).toList should be(List(OptionVal("10"), OptionVal("30")))

      val m4 = m1.remove(30L)
      m4.keysIterator.map(m4.get).toList should be(List(OptionVal("10"), OptionVal("20")))

      m1.remove(5L) should be(m1)

      m1.remove(10L).remove(20L).remove(30L) should be(ImmutableLongMap.empty[String])
    }

    "get None when entry doesn't exist" in {
      val m1 = ImmutableLongMap.empty[String].updated(10L, "10").updated(20L, "20").updated(30L, "30")
      m1.get(5L) should be(OptionVal.None)
      m1.get(15L) should be(OptionVal.None)
      m1.get(25L) should be(OptionVal.None)
      m1.get(35L) should be(OptionVal.None)
    }

    "contain keys" in {
      val m1 = ImmutableLongMap.empty[String].updated(10L, "10").updated(20L, "20").updated(30L, "30")
      m1.contains(10L) should be(true)
      m1.contains(20L) should be(true)
      m1.contains(30L) should be(true)
      m1.contains(5L) should be(false)
      m1.contains(25L) should be(false)
    }

    "have correct behavior for random operations" in {
      val seed = System.nanoTime()
      val rnd = new Random(seed)

      var longMap = ImmutableLongMap.empty[String]
      var reference = Map.empty[Long, String]

      def verify(): Unit = {
        val m = longMap.keysIterator.map(key => key -> longMap.get(key).get).toMap

        m should be(reference)
      }

      (1 to 1000).foreach { i =>
        withClue(s"seed=$seed, iteration=$i") {
          val key = rnd.nextInt(100)
          val value = String.valueOf(rnd.nextPrintableChar())
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
