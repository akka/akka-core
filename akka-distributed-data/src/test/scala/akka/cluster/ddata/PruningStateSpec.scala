/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress

class PruningStateSpec extends AnyWordSpec with Matchers {
  import PruningState._

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)
  val node4 = UniqueAddress(node1.address.copy(port = Some(2554)), 4L)

  "Pruning state" must {

    "merge state correctly" in {
      val p1 = PruningInitialized(node1, Set.empty)
      val p2 = PruningPerformed(System.currentTimeMillis() + 3600 * 1000)
      p1.merge(p2) should be(p2)
      p2.merge(p1) should be(p2)

      val p3 = p2.copy(p2.obsoleteTime - 1)
      p2.merge(p3) should be(p2) // keep greatest obsoleteTime
      p3.merge(p2) should be(p2)

    }

    "merge owner correctly" in {
      val p1 = PruningInitialized(node1, Set.empty)
      val p2 = PruningInitialized(node2, Set.empty)
      val expected = PruningInitialized(node1, Set.empty)
      p1.merge(p2) should be(expected)
      p2.merge(p1) should be(expected)
    }

    "merge seen correctly" in {
      val p1 = PruningInitialized(node1, Set(node2.address))
      val p2 = PruningInitialized(node1, Set(node4.address))
      val expected = PruningInitialized(node1, Set(node2.address, node4.address))
      p1.merge(p2) should be(expected)
      p2.merge(p1) should be(expected)
    }

  }
}
