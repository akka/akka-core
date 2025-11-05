/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class GCounterSpec extends AnyWordSpec with Matchers {
  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)

  "A GCounter" must {

    "be able to increment each node's record by one" in {
      val c1 = GCounter()

      val c2 = c1.increment(node1)
      val c3 = c2.increment(node1)

      val c4 = c3.increment(node2)
      val c5 = c4.increment(node2)
      val c6 = c5.resetDelta.increment(node2)

      c6.state(node1) should be(2)
      c6.state(node2) should be(3)

      c2.delta.get.state(node1) should be(1)
      c1.mergeDelta(c2.delta.get) should be(c2)
      c3.delta.get.state(node1) should be(2)
      c2.mergeDelta(c3.delta.get) should be(c3)
      c6.delta.get.state(node2) should be(3)
      c5.mergeDelta(c6.delta.get) should be(c6)
    }

    "be able to increment each node's record by arbitrary delta" in {
      val c1 = GCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.increment(node1, 4)

      val c4 = c3.increment(node2, 2)
      val c5 = c4.increment(node2, 7)
      val c6 = c5.increment(node2)

      c6.state(node1) should be(7)
      c6.state(node2) should be(10)
    }

    "be able to summarize the history to the correct aggregated value" in {
      val c1 = GCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.increment(node1, 4)

      val c4 = c3.increment(node2, 2)
      val c5 = c4.increment(node2, 7)
      val c6 = c5.increment(node2)

      c6.state(node1) should be(7)
      c6.state(node2) should be(10)

      c6.value should be(17)
    }

    "be able to have its history correctly merged with another GCounter 1" in {
      // counter 1
      val c11 = GCounter()
      val c12 = c11.increment(node1, 3)
      val c13 = c12.increment(node1, 4)
      val c14 = c13.increment(node2, 2)
      val c15 = c14.increment(node2, 7)
      val c16 = c15.increment(node2)

      c16.state(node1) should be(7)
      c16.state(node2) should be(10)
      c16.value should be(17)

      // counter 2
      val c21 = GCounter()
      val c22 = c21.increment(node1, 2)
      val c23 = c22.increment(node1, 2)
      val c24 = c23.increment(node2, 3)
      val c25 = c24.increment(node2, 2)
      val c26 = c25.increment(node2)

      c26.state(node1) should be(4)
      c26.state(node2) should be(6)
      c26.value should be(10)

      // merge both ways
      val merged1 = c16.merge(c26)
      merged1.state(node1) should be(7)
      merged1.state(node2) should be(10)
      merged1.value should be(17)
      merged1.delta should ===(None)

      val merged2 = c26.merge(c16)
      merged2.state(node1) should be(7)
      merged2.state(node2) should be(10)
      merged2.value should be(17)
      merged2.delta should ===(None)
    }

    "be able to have its history correctly merged with another GCounter 2" in {
      // counter 1
      val c11 = GCounter()
      val c12 = c11.increment(node1, 2)
      val c13 = c12.increment(node1, 2)
      val c14 = c13.increment(node2, 2)
      val c15 = c14.increment(node2, 7)
      val c16 = c15.increment(node2)

      c16.state(node1) should be(4)
      c16.state(node2) should be(10)
      c16.value should be(14)

      // counter 1
      val c21 = GCounter()
      val c22 = c21.increment(node1, 3)
      val c23 = c22.increment(node1, 4)
      val c24 = c23.increment(node2, 3)
      val c25 = c24.increment(node2, 2)
      val c26 = c25.increment(node2)

      c26.state(node1) should be(7)
      c26.state(node2) should be(6)
      c26.value should be(13)

      // merge both ways
      val merged1 = c16.merge(c26)
      merged1.state(node1) should be(7)
      merged1.state(node2) should be(10)
      merged1.value should be(17)

      val merged2 = c26.merge(c16)
      merged2.state(node1) should be(7)
      merged2.state(node2) should be(10)
      merged2.value should be(17)
    }

    "be able to have its history correctly merged with another GCounter 3" in {
      val c1 = GCounter()

      val c2 = c1.increment(node1, 3)
      val c3 = c2.increment(node1, 4)

      // new empty counter for node2
      val c4 = GCounter()
      val c5 = c4.increment(node2, 5)
      val c6 = c5.increment(node2)

      val c7 = c6.merge(c3)

      c7.state(node1) should be(7)
      c7.state(node2) should be(6)

      c7.value should be(13)
    }

    "have support for pruning" in {
      val c1 = GCounter()
      val c2 = c1.increment(node1)
      val c3 = c2.increment(node2)
      c2.modifiedByNodes should ===(Set(node1))
      c2.needPruningFrom(node1) should be(true)
      c2.needPruningFrom(node2) should be(false)
      c3.modifiedByNodes should ===(Set(node1, node2))
      c3.needPruningFrom(node1) should be(true)
      c3.needPruningFrom(node2) should be(true)
      c3.value should be(2)

      val c4 = c3.prune(node1, node2)
      c4.modifiedByNodes should ===(Set(node2))
      c4.needPruningFrom(node2) should be(true)
      c4.needPruningFrom(node1) should be(false)
      c4.value should be(2)

      val c5 = c4.increment(node1).pruningCleanup(node1)
      c5.modifiedByNodes should ===(Set(node2))
      c5.needPruningFrom(node1) should be(false)
      c4.value should be(2)
    }

    "have unapply extractor" in {
      val c1 = GCounter.empty.increment(node1).increment(node2)
      val value1 = c1 match {
        case GCounter(value1) => value1
        case _                => fail()
      }
      val value2: BigInt = value1
      value2 should be(2L)

      Changed(GCounterKey("key"))(c1) match {
        case c @ Changed(GCounterKey("key")) =>
          val value3 = c.dataValue match {
            case GCounter(value3) => value3
            case _                => fail()
          }
          val value4: BigInt = value3
          value4 should be(2L)
        case _ =>
          fail("Failed to update")
      }
    }

  }
}
