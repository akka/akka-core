/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.cluster.ddata.Replicator.Changed

class GSetSpec extends AnyWordSpec with Matchers {

  val user1 = """{"username":"john","password":"coltrane"}"""
  val user2 = """{"username":"sonny","password":"rollins"}"""
  val user3 = """{"username":"charlie","password":"parker"}"""
  val user4 = """{"username":"charles","password":"mingus"}"""

  "A GSet" must {

    "be able to add user" in {
      val c1 = GSet.empty[String]

      val c2 = c1 + user1
      val c3 = c2 + user2

      val c4 = c3 + user4
      val c5 = c4 + user3

      c5.elements should contain(user1)
      c5.elements should contain(user2)
      c5.elements should contain(user3)
      c5.elements should contain(user4)
    }

    "be able to have its user set correctly merged with another GSet with unique user sets" in {
      // set 1
      val c11 = GSet.empty[String]

      val c12 = c11 + user1
      val c13 = c12 + user2

      c13.elements should ===(Set(user1, user2))

      // set 2
      val c21 = GSet.empty[String]

      val c22 = c21 + user3
      val c23 = c22 + user4

      c23.elements should ===(Set(user3, user4))

      // merge both ways
      val merged1 = c13.merge(c23)
      merged1.elements should ===(Set(user1, user2, user3, user4))

      val merged2 = c23.merge(c13)
      merged2.elements should ===(Set(user1, user2, user3, user4))
    }

    "be able to work with deltas" in {
      // set 1
      val c11 = GSet.empty[String]

      val c12 = c11 + user1
      val c13 = c12 + user2

      c12.delta.get.elements should ===(Set(user1))
      c13.delta.get.elements should ===(Set(user1, user2))

      // deltas build state
      (c12.mergeDelta(c13.delta.get)) should ===(c13)

      // own deltas are idempotent
      (c13.mergeDelta(c13.delta.get)) should ===(c13)

      // set 2
      val c21 = GSet.empty[String]

      val c22 = c21 + user3
      val c23 = c22.resetDelta + user4

      c22.delta.get.elements should ===(Set(user3))
      c23.delta.get.elements should ===(Set(user4))

      c23.elements should ===(Set(user3, user4))

      val c33 = c13.merge(c23)

      // merge both ways
      val merged1 = GSet
        .empty[String]
        .mergeDelta(c12.delta.get)
        .mergeDelta(c13.delta.get)
        .mergeDelta(c22.delta.get)
        .mergeDelta(c23.delta.get)
      merged1.elements should ===(Set(user1, user2, user3, user4))

      val merged2 = GSet.empty[String].mergeDelta(c23.delta.get).mergeDelta(c13.delta.get).mergeDelta(c22.delta.get)
      merged2.elements should ===(Set(user1, user2, user3, user4))

      merged1 should ===(c33)
      merged2 should ===(c33)
    }

    "be able to have its user set correctly merged with another GSet with overlapping user sets" in {
      // set 1
      val c10 = GSet.empty[String]

      val c11 = c10 + user1
      val c12 = c11 + user2
      val c13 = c12 + user3

      c13.elements should contain(user1)
      c13.elements should contain(user2)
      c13.elements should contain(user3)

      // set 2
      val c20 = GSet.empty[String]

      val c21 = c20 + user2
      val c22 = c21 + user3
      val c23 = c22 + user4

      c23.elements should contain(user2)
      c23.elements should contain(user3)
      c23.elements should contain(user4)

      // merge both ways
      val merged1 = c13.merge(c23)
      merged1.elements should contain(user1)
      merged1.elements should contain(user2)
      merged1.elements should contain(user3)
      merged1.elements should contain(user4)

      val merged2 = c23.merge(c13)
      merged2.elements should contain(user1)
      merged2.elements should contain(user2)
      merged2.elements should contain(user3)
      merged2.elements should contain(user4)
    }

    "have unapply extractor" in {
      val s1 = GSet.empty + "a" + "b"
      val _: GSet[String] = s1
      val GSet(elements1) = s1
      val elements2: Set[String] = elements1
      elements2 should be(Set("a", "b"))

      Changed(GSetKey[String]("key"))(s1) match {
        case c @ Changed(GSetKey("key")) =>
          val GSet(elements3) = c.dataValue
          val elements4: Set[String] = elements3
          elements4 should be(Set("a", "b"))
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

  }
}
