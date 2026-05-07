/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.Replicator.Internal.Delta
import akka.cluster.ddata.Replicator.Internal.DeltaPropagation
import akka.cluster.ddata.Replicator.Internal.DeltaPropagation.NoDeltaPlaceholder

object DeltaPropagationSelectorSpec {
  class TestSelector(val selfUniqueAddress: UniqueAddress, override val allNodes: Vector[UniqueAddress])
      extends DeltaPropagationSelector {
    override val gossipIntervalDivisor = 5
    override def createDeltaPropagation(deltas: Map[KeyId, (ReplicatedData, Long, Long)]): DeltaPropagation =
      DeltaPropagation(selfUniqueAddress, false, deltas.map {
        case (key, (d, fromSeqNr, toSeqNr)) => (key, Delta(DataEnvelope(d), fromSeqNr, toSeqNr))
      })
    override def maxDeltaSize: Int = 10
  }

  val deltaA = GSet.empty[String] + "a"
  val deltaB = GSet.empty[String] + "b"
  val deltaC = GSet.empty[String] + "c"
}

class DeltaPropagationSelectorSpec extends AnyWordSpec with Matchers with TypeCheckedTripleEquals {
  import DeltaPropagationSelectorSpec._
  val selfUniqueAddress = UniqueAddress(Address("akka", "Sys", "localhost", 4999), 17L)
  val nodes = (2500 until 2600).map(n => UniqueAddress(Address("akka", "Sys", "localhost", n), 17L)).toVector

  "DeltaPropagationSelector" must {
    "collect none when no nodes" in {
      val selector = new TestSelector(selfUniqueAddress, Vector.empty)
      selector.update("A", deltaA)
      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(false)
    }

    "collect 1 when one node" in {
      val selector = new TestSelector(selfUniqueAddress, nodes.take(1))
      selector.update("A", deltaA)
      selector.update("B", deltaB)
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(true)
      selector.hasDeltaEntries("B") should ===(true)
      val expected =
        DeltaPropagation(
          selfUniqueAddress,
          false,
          Map("A" -> Delta(DataEnvelope(deltaA), 1L, 1L), "B" -> Delta(DataEnvelope(deltaB), 1L, 1L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected))
      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(false)
      selector.hasDeltaEntries("B") should ===(false)
    }

    "collect 2+1 when three nodes" in {
      val selector = new TestSelector(selfUniqueAddress, nodes.take(3))
      selector.update("A", deltaA)
      selector.update("B", deltaB)
      val expected =
        DeltaPropagation(
          selfUniqueAddress,
          false,
          Map("A" -> Delta(DataEnvelope(deltaA), 1L, 1L), "B" -> Delta(DataEnvelope(deltaB), 1L, 1L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected, nodes(1) -> expected))
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(true)
      selector.hasDeltaEntries("B") should ===(true)
      selector.collectPropagations() should ===(Map(nodes(2) -> expected))
      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(false)
      selector.hasDeltaEntries("B") should ===(false)
    }

    "keep track of deltas per node" in {
      val selector = new TestSelector(selfUniqueAddress, nodes.take(3))
      selector.update("A", deltaA)
      selector.update("B", deltaB)
      val expected1 =
        DeltaPropagation(
          selfUniqueAddress,
          false,
          Map("A" -> Delta(DataEnvelope(deltaA), 1L, 1L), "B" -> Delta(DataEnvelope(deltaB), 1L, 1L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected1, nodes(1) -> expected1))
      // new update before previous was propagated to all nodes
      selector.update("C", deltaC)
      val expected2 = DeltaPropagation(
        selfUniqueAddress,
        false,
        Map(
          "A" -> Delta(DataEnvelope(deltaA), 1L, 1L),
          "B" -> Delta(DataEnvelope(deltaB), 1L, 1L),
          "C" -> Delta(DataEnvelope(deltaC), 1L, 1L)))
      val expected3 =
        DeltaPropagation(selfUniqueAddress, false, Map("C" -> Delta(DataEnvelope(deltaC), 1L, 1L)))
      selector.collectPropagations() should ===(Map(nodes(2) -> expected2, nodes(0) -> expected3))
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("A") should ===(false)
      selector.hasDeltaEntries("B") should ===(false)
      selector.hasDeltaEntries("C") should ===(true)
      selector.collectPropagations() should ===(Map(nodes(1) -> expected3))
      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
      selector.cleanupDeltaEntries()
      selector.hasDeltaEntries("C") should ===(false)
    }

    "bump version for each update" in {
      val delta1 = GSet.empty[String] + "a1"
      val delta2 = GSet.empty[String] + "a2"
      val delta3 = GSet.empty[String] + "a3"
      val selector = new TestSelector(selfUniqueAddress, nodes.take(1))
      selector.update("A", delta1)
      selector.currentVersion("A") should ===(1L)
      selector.update("A", delta2)
      selector.currentVersion("A") should ===(2L)
      val expected1 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta1.merge(delta2)), 1L, 2L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected1))
      selector.update("A", delta3)
      selector.currentVersion("A") should ===(3L)
      val expected2 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta3), 3L, 3L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected2))
      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
    }

    "merge deltas" in {
      val delta1 = GSet.empty[String] + "a1"
      val delta2 = GSet.empty[String] + "a2"
      val delta3 = GSet.empty[String] + "a3"
      val selector = new TestSelector(selfUniqueAddress, nodes.take(3)) {
        override def nodesSliceSize(allNodesSize: Int): Int = 1
      }
      selector.update("A", delta1)
      val expected1 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta1), 1L, 1L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected1))

      selector.update("A", delta2)
      val expected2 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta1.merge(delta2)), 1L, 2L)))
      selector.collectPropagations() should ===(Map(nodes(1) -> expected2))

      selector.update("A", delta3)
      val expected3 = DeltaPropagation(
        selfUniqueAddress,
        false,
        Map("A" -> Delta(DataEnvelope(delta1.merge(delta2).merge(delta3)), 1L, 3L)))
      selector.collectPropagations() should ===(Map(nodes(2) -> expected3))

      val expected4 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta2.merge(delta3)), 2L, 3L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected4))

      val expected5 =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(delta3), 3L, 3L)))
      selector.collectPropagations() should ===(Map(nodes(1) -> expected5))

      selector.collectPropagations() should ===(Map.empty[UniqueAddress, DeltaPropagation])
    }

    "discard too large deltas" in {
      val selector = new TestSelector(selfUniqueAddress, nodes.take(3)) {
        override def nodesSliceSize(allNodesSize: Int): Int = 1
      }
      var data = PNCounterMap.empty[String]
      (1 to 1000).foreach { n =>
        val d = data.resetDelta.increment(selfUniqueAddress, (n % 2).toString, 1)
        selector.update("A", d.delta.get)
        data = d
      }
      val expected =
        DeltaPropagation(selfUniqueAddress, false, Map("A" -> Delta(DataEnvelope(NoDeltaPlaceholder), 1L, 1000L)))
      selector.collectPropagations() should ===(Map(nodes(0) -> expected))
    }

    "calculate right slice size" in {
      val selector = new TestSelector(selfUniqueAddress, nodes)
      selector.nodesSliceSize(0) should ===(0)
      selector.nodesSliceSize(1) should ===(1)
      (2 to 9).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(2)
        }
      }
      (10 to 14).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(3)
        }
      }
      (15 to 19).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(4)
        }
      }
      (20 to 24).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(5)
        }
      }
      (25 to 29).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(6)
        }
      }
      (30 to 34).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(7)
        }
      }
      (35 to 39).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(8)
        }
      }
      (40 to 44).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(9)
        }
      }
      (45 to 200).foreach { n =>
        withClue(s"n=$n") {
          selector.nodesSliceSize(n) should ===(10)
        }
      }
    }
  }
}
