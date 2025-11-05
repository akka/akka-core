/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Internal.DataEnvelope

class DataEnvelopeSpec extends AnyWordSpec with Matchers {
  import PruningState._

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)
  val node4 = UniqueAddress(node1.address.copy(port = Some(2554)), 4L)
  val obsoleteTimeInFuture = System.currentTimeMillis() + 3600 * 1000
  val oldObsoleteTime = System.currentTimeMillis() - 3600 * 1000

  "DataEnvelope" must {

    "handle pruning transitions" in {
      val g1 = GCounter.empty.increment(node1, 1)
      val d1 = DataEnvelope(g1)

      val d2 = d1.initRemovedNodePruning(node1, node2)
      d2.pruning(node1).isInstanceOf[PruningInitialized] should ===(true)
      d2.pruning(node1).asInstanceOf[PruningInitialized].owner should ===(node2)

      val d3 = d2.addSeen(node3.address)
      d3.pruning(node1).asInstanceOf[PruningInitialized].seen should ===(Set(node3.address))

      val d4 = d3.prune(node1, PruningPerformed(obsoleteTimeInFuture))
      d4.data.asInstanceOf[GCounter].modifiedByNodes should ===(Set(node2))
    }

    "merge correctly" in {
      val g1 = GCounter.empty.increment(node1, 1)
      val d1 = DataEnvelope(g1)
      val g2 = GCounter.empty.increment(node2, 2)
      val d2 = DataEnvelope(g2)

      val d3 = d1.merge(d2)
      d3.data.asInstanceOf[GCounter].value should ===(3)
      d3.data.asInstanceOf[GCounter].modifiedByNodes should ===(Set(node1, node2))
      val d4 = d3.initRemovedNodePruning(node1, node2)
      val d5 = d4.prune(node1, PruningPerformed(obsoleteTimeInFuture))
      d5.data.asInstanceOf[GCounter].modifiedByNodes should ===(Set(node2))

      // late update from node1
      val g11 = g1.increment(node1, 10)
      val d6 = d5.merge(DataEnvelope(g11))
      d6.data.asInstanceOf[GCounter].value should ===(3)
      d6.data.asInstanceOf[GCounter].modifiedByNodes should ===(Set(node2))

      // remove obsolete
      val d7 = d5.copy(pruning = d5.pruning.updated(node1, PruningPerformed(oldObsoleteTime)))
      val d8 = d5.copy(pruning = Map.empty)
      d8.merge(d7).pruning should ===(Map.empty)
      d7.merge(d8).pruning should ===(Map.empty)

      d5.merge(d7).pruning(node1) should ===(PruningPerformed(obsoleteTimeInFuture))
      d7.merge(d5).pruning(node1) should ===(PruningPerformed(obsoleteTimeInFuture))
    }

  }
}
