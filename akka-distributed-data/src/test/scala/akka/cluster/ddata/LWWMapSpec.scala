/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

import scala.annotation.nowarn

@nowarn("cat=lint-infer-any")
class LWWMapSpec extends AnyWordSpec with Matchers {
  import LWWRegister.defaultClock

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A LWWMap" must {

    "be able to set entries" in {
      val m = LWWMap.empty[String, Int].put(node1, "a", 1, defaultClock[Int]).put(node2, "b", 2, defaultClock[Int])
      m.entries should be(Map("a" -> 1, "b" -> 2))
    }

    "be able to have its entries correctly merged with another LWWMap with other entries" in {
      val m1 = LWWMap.empty[String, Int].put(node1, "a", 1, defaultClock[Int]).put(node1, "b", 2, defaultClock[Int])
      val m2 = LWWMap.empty[String, Int].put(node2, "c", 3, defaultClock[Int])

      // merge both ways
      val expected = Map("a" -> 1, "b" -> 2, "c" -> 3)
      m1.merge(m2).entries should be(expected)
      m2.merge(m1).entries should be(expected)
    }

    "be able to remove entry" in {
      val m1 = LWWMap.empty[String, Int].put(node1, "a", 1, defaultClock[Int]).put(node1, "b", 2, defaultClock[Int])
      val m2 = LWWMap.empty[String, Int].put(node2, "c", 3, defaultClock[Int])

      val merged1 = m1.merge(m2)

      val m3 = merged1.remove(node1, "b")
      merged1.merge(m3).entries should be(Map("a" -> 1, "c" -> 3))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.put(node2, "b", 22, defaultClock[Int])
      m3.merge(m4).entries should be(Map("a" -> 1, "b" -> 22, "c" -> 3))
    }

    "be able to work with deltas" in {
      val m1 = LWWMap.empty[String, Int].put(node1, "a", 1, defaultClock[Int]).put(node1, "b", 2, defaultClock[Int])
      val m2 = LWWMap.empty[String, Int].put(node2, "c", 3, defaultClock[Int])

      val expected = Map("a" -> 1, "b" -> 2, "c" -> 3)
      m1.merge(m2).entries should be(expected)
      m2.merge(m1).entries should be(expected)

      LWWMap.empty.mergeDelta(m1.delta.get).mergeDelta(m2.delta.get).entries should be(expected)
      LWWMap.empty.mergeDelta(m2.delta.get).mergeDelta(m1.delta.get).entries should be(expected)

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      merged1.mergeDelta(m3.delta.get).entries should be(Map("a" -> 1, "c" -> 3))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.resetDelta.put(node2, "b", 22, defaultClock[Int])
      m3.mergeDelta(m4.delta.get).entries should be(Map("a" -> 1, "b" -> 22, "c" -> 3))
    }

    "have unapply extractor" in {
      val m1 = LWWMap.empty[String, Long].put(node1, "a", 1L, defaultClock[Long])
      val entries1 = m1 match {
        case LWWMap(entries1) => entries1
        case _                => fail()
      }
      val entries2: Map[String, Long] = entries1
      entries2 should be(Map("a" -> 1L))

      Changed(LWWMapKey[String, Long]("key"))(m1) match {
        case c @ Changed(LWWMapKey("key")) =>
          val entries3 = c.dataValue match {
            case LWWMap(entries3) => entries3
            case _                => fail()
          }
          val entries4: Map[String, Long] = entries3
          entries4 should be(Map("a" -> 1L))
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

  }
}
