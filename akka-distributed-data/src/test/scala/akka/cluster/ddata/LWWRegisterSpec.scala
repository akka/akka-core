/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class LWWRegisterSpec extends AnyWordSpec with Matchers {
  import LWWRegister.defaultClock

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A LWWRegister" must {
    "use latest of successive assignments" in {
      val r = (1 to 100).foldLeft(LWWRegister(node1, 0, defaultClock[Int])) {
        case (r, n) =>
          r.value should be(n - 1)
          r.withValue(node1, n, defaultClock[Int])
      }
      r.value should be(100)
    }

    "merge by picking max timestamp" in {
      val clock = new LWWRegister.Clock[String] {
        val i = Iterator.from(100)
        override def apply(current: Long, value: String): Long = i.next()
      }
      val r1 = LWWRegister(node1, "A", clock)
      r1.timestamp should be(100)
      val r2 = r1.withValue(node2, "B", clock)
      r2.timestamp should be(101)
      val m1 = r1.merge(r2)
      m1.value should be("B")
      m1.timestamp should be(101)
      val m2 = r2.merge(r1)
      m2.value should be("B")
      m2.timestamp should be(101)
    }

    "merge by picking least address when same timestamp" in {
      val clock = new LWWRegister.Clock[String] {
        override def apply(current: Long, value: String): Long = 100
      }
      val r1 = LWWRegister(node1, "A", clock)
      val r2 = LWWRegister(node2, "B", clock)
      val m1 = r1.merge(r2)
      m1.value should be("A")
      val m2 = r2.merge(r1)
      m2.value should be("A")
    }

    "use monotonically increasing defaultClock" in {
      implicit val node = SelfUniqueAddress(node1)

      (1 to 100).foldLeft(LWWRegister.create(0)) {
        case (r, n) =>
          r.value should be(n - 1)
          val r2 = r.withValueOf(n)
          r2.timestamp should be > r.timestamp
          r2
      }
    }

    "have unapply extractor" in {
      val r1 = LWWRegister(node1, "a", defaultClock[String])
      val value1 = r1 match {
        case LWWRegister(value1) => value1
        case _                   => fail()
      }
      val value2: String = value1
      value2 should be("a")

      Changed(LWWRegisterKey[String]("key"))(r1) match {
        case c @ Changed(LWWRegisterKey("key")) =>
          val value3 = c.dataValue match {
            case LWWRegister(value3) => value3
            case _                   => fail()
          }
          val value4: String = value3
          value4 should be("a")
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

    "can be used as first-write-wins-register" in {
      import LWWRegister.reverseClock
      val r = (1 to 100).foldLeft(LWWRegister(node1, 0, reverseClock[Int])) {
        case (r, n) =>
          r.value should be(0)
          val newRegister = r.merge(r.withValue(node1, n, reverseClock[Int]))
          newRegister should be(r)
          newRegister
      }
      r.value should be(0)
    }
  }
}
