/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class PNCounterMapSpec extends AnyWordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A PNCounterMap" must {

    "be able to increment and decrement entries with implicit SelfUniqueAddress" in {
      implicit val node = SelfUniqueAddress(node1)
      PNCounterMap().incrementBy("a", 2).incrementBy("b", 1).incrementBy("b", 2).decrementBy("a", 1).entries should be(
        Map("a" -> 1, "b" -> 3))
    }

    "be able to increment and decrement entries" in {
      val m = PNCounterMap().increment(node1, "a", 2).increment(node1, "b", 3).decrement(node2, "a", 1)
      m.entries should be(Map("a" -> 1, "b" -> 3))
    }

    "be able to have its entries correctly merged with another ORMap with other entries" in {
      val m1 = PNCounterMap().increment(node1, "a", 1).increment(node1, "b", 3).increment(node1, "c", 2)
      val m2 = PNCounterMap().increment(node2, "c", 5)

      // merge both ways
      val expected = Map("a" -> 1, "b" -> 3, "c" -> 7)
      m1.merge(m2).entries should be(expected)
      m2.merge(m1).entries should be(expected)
    }

    "be able to remove entry" in {
      val m1 = PNCounterMap().increment(node1, "a", 1).increment(node1, "b", 3).increment(node1, "c", 2)
      val m2 = PNCounterMap().increment(node2, "c", 5)

      val merged1 = m1.merge(m2)

      val m3 = merged1.remove(node1, "b")
      merged1.merge(m3).entries should be(Map("a" -> 1, "c" -> 7))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.increment(node2, "b", 10)
      m3.merge(m4).entries should be(Map("a" -> 1, "b" -> 13, "c" -> 7))
    }

    "be able to work with deltas" in {
      val m1 = PNCounterMap().increment(node1, "a", 1).increment(node1, "b", 3).increment(node1, "c", 2)
      val m2 = PNCounterMap().increment(node2, "c", 5)

      val expected = Map("a" -> 1, "b" -> 3, "c" -> 7)
      PNCounterMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get).entries should be(expected)
      PNCounterMap().mergeDelta(m2.delta.get).mergeDelta(m1.delta.get).entries should be(expected)

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      merged1.mergeDelta(m3.delta.get).entries should be(Map("a" -> 1, "c" -> 7))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.resetDelta.increment(node2, "b", 10)
      m3.mergeDelta(m4.delta.get).entries should be(Map("a" -> 1, "b" -> 13, "c" -> 7))
    }

    "have unapply extractor" in {
      val m1 = PNCounterMap.empty.increment(node1, "a", 1).increment(node2, "b", 2)
      val entries1 = m1 match {
        case PNCounterMap(entries1) => entries1
        case _                      => throw new RuntimeException()
      }
      val entries2: Map[String, BigInt] = entries1
      entries2 should be(Map("a" -> 1L, "b" -> 2L))

      Changed(PNCounterMapKey[String]("key"))(m1) match {
        case c @ Changed(PNCounterMapKey("key")) =>
          val entries3 = c.dataValue match {
            case PNCounterMap(entries3) => entries3
            case _                      => throw new RuntimeException()
          }
          val entries4: Map[String, BigInt] = entries3
          entries4 should be(Map("a" -> 1L, "b" -> 2L))
        case _ =>
          fail("Did not match")
      }
    }

  }
}
