/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class PNCounterSpec extends AnyWordSpec with Matchers {
  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A PNCounter" must {

    "be able to increment each node's record by one" in {
      val c1 = PNCounter()

      val c2 = c1.increment(node1)
      val c3 = c2.increment(node1)

      val c4 = c3.increment(node2)
      val c5 = c4.increment(node2)
      val c6 = c5.resetDelta.increment(node2)

      c6.increments.state(node1) should be(2)
      c6.increments.state(node2) should be(3)

      c2.delta.get.value.toLong should be(1)
      c2.delta.get.increments.state(node1) should be(1)
      c3.delta.get.value should be(2)
      c3.delta.get.increments.state(node1) should be(2)

      c6.delta.get.value should be(3)
      c6.delta.get.increments.state(node2) should be(3)
    }

    "be able to decrement each node's record by one" in {
      val c1 = PNCounter()

      val c2 = c1.decrement(node1)
      val c3 = c2.decrement(node1)

      val c4 = c3.decrement(node2)
      val c5 = c4.decrement(node2)
      val c6 = c5.resetDelta.decrement(node2)

      c6.decrements.state(node1) should be(2)
      c6.decrements.state(node2) should be(3)

      c3.delta.get.value should be(-2)
      c3.delta.get.decrements.state(node1) should be(2)

      c6.delta.get.value should be(-3)
      c6.delta.get.decrements.state(node2) should be(3)
    }

    "be able to increment each node's record by arbitrary delta" in {
      val c1 = PNCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.increment(node1, 4)

      val c4 = c3.increment(node2, 2)
      val c5 = c4.increment(node2, 7)
      val c6 = c5.increment(node2)

      c6.increments.state(node1) should be(7)
      c6.increments.state(node2) should be(10)
    }

    "be able to increment each node's record by arbitrary BigInt delta" in {
      val c1 = PNCounter()

      val c2 = c1.increment(node1, BigInt(3))
      val c3 = c2.increment(node1, BigInt(4))

      val c4 = c3.increment(node2, BigInt(2))
      val c5 = c4.increment(node2, BigInt(7))
      val c6 = c5.increment(node2)

      c6.increments.state(node1) should be(7)
      c6.increments.state(node2) should be(10)
    }

    "be able to decrement each node's record by arbitrary delta" in {
      val c1 = PNCounter()

      val c2 = c1.decrement(node1, 3)
      val c3 = c2.decrement(node1, 4)

      val c4 = c3.decrement(node2, 2)
      val c5 = c4.decrement(node2, 7)
      val c6 = c5.decrement(node2)

      c6.decrements.state(node1) should be(7)
      c6.decrements.state(node2) should be(10)
    }

    "be able to increment and decrement each node's record by arbitrary delta" in {
      val c1 = PNCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.decrement(node1, 2)

      val c4 = c3.increment(node2, 5)
      val c5 = c4.decrement(node2, 2)
      val c6 = c5.increment(node2)

      c6.increments.value should be(9)
      c6.decrements.value should be(4)
    }

    "be able to summarize the history to the correct aggregated value of increments and decrements" in {
      val c1 = PNCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.decrement(node1, 2)

      val c4 = c3.increment(node2, 5)
      val c5 = c4.decrement(node2, 2)
      val c6 = c5.increment(node2)

      c6.increments.value should be(9)
      c6.decrements.value should be(4)

      c6.value should be(5)
    }

    "be able to have its history correctly merged with another GCounter" in {
      // counter 1
      val c11 = PNCounter()
      val c12 = c11.increment(node1, 3)
      val c13 = c12.decrement(node1, 2)
      val c14 = c13.increment(node2, 5)
      val c15 = c14.decrement(node2, 2)
      val c16 = c15.increment(node2)

      c16.increments.value should be(9)
      c16.decrements.value should be(4)
      c16.value should be(5)

      // counter 1
      val c21 = PNCounter()
      val c22 = c21.increment(node1, 2)
      val c23 = c22.decrement(node1, 3)
      val c24 = c23.increment(node2, 3)
      val c25 = c24.decrement(node2, 2)
      val c26 = c25.increment(node2)

      c26.increments.value should be(6)
      c26.decrements.value should be(5)
      c26.value should be(1)

      // merge both ways
      val merged1 = c16.merge(c26)
      merged1.increments.value should be(9)
      merged1.decrements.value should be(5)
      merged1.value should be(4)

      val merged2 = c26.merge(c16)
      merged2.increments.value should be(9)
      merged2.decrements.value should be(5)
      merged2.value should be(4)
    }

    "have support for pruning" in {
      val c1 = PNCounter()
      val c2 = c1.increment(node1)
      val c3 = c2.decrement(node2)
      c2.modifiedByNodes should ===(Set(node1))
      c2.needPruningFrom(node1) should be(true)
      c2.needPruningFrom(node2) should be(false)
      c3.modifiedByNodes should ===(Set(node1, node2))
      c3.needPruningFrom(node1) should be(true)
      c3.needPruningFrom(node2) should be(true)

      val c4 = c3.prune(node1, node2)
      c4.modifiedByNodes should ===(Set(node2))
      c4.needPruningFrom(node2) should be(true)
      c4.needPruningFrom(node1) should be(false)

      val c5 = c4.increment(node1).pruningCleanup(node1)
      c5.modifiedByNodes should ===(Set(node2))
      c5.needPruningFrom(node1) should be(false)
    }

    "have unapply extractor" in {
      val c1 = PNCounter.empty.increment(node1).increment(node1).decrement(node2)
      val value1 = c1.value
      val value2: BigInt = value1
      value2 should be(1L)

      Changed(PNCounterKey("key"))(c1) match {
        case c @ Changed(PNCounterKey("key")) =>
          val value3 = c.dataValue.value
          val value4: BigInt = value3
          value4 should be(1L)
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

  }
}
