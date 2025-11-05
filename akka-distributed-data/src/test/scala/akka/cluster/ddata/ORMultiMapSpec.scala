/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class ORMultiMapSpec extends AnyWordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A ORMultiMap" must {

    "be able to add entries" in {
      val m = ORMultiMap().addBinding(node1, "a", "A").addBinding(node1, "b", "B")
      m.entries should be(Map("a" -> Set("A"), "b" -> Set("B")))

      val m2 = m.addBinding(node1, "a", "C")
      m2.entries should be(Map("a" -> Set("A", "C"), "b" -> Set("B")))
    }

    "be able to remove entry" in {
      val m = ORMultiMap().addBinding(node1, "a", "A").addBinding(node1, "b", "B").removeBinding(node1, "a", "A")
      m.entries should be(Map("b" -> Set("B")))
    }

    "be able to replace an entry" in {
      val m = ORMultiMap().addBinding(node1, "a", "A").replaceBinding(node1, "a", "A", "B")
      m.entries should be(Map("a" -> Set("B")))
    }

    "not handle concurrent updates to the same set" in {
      val m = ORMultiMap().addBinding(node1, "a", "A")

      val m1 = m.removeBinding(node1, "a", "A")

      val m2 = m.addBinding(node2, "a", "B")

      val merged1 = m1.merge(m2)
      val merged2 = m2.merge(m1)

      // more to document that the concurrent removal from the set may be lost
      // than asserting anything
      merged1.entries should be(Map("a" -> Set("A", "B")))
      merged2.entries should be(Map("a" -> Set("A", "B")))
    }

    "be able to have its entries correctly merged with another ORMultiMap with other entries" in {
      val m1 = ORMultiMap().addBinding(node1, "a", "A").addBinding(node1, "b", "B")
      val m2 = ORMultiMap().addBinding(node2, "c", "C")

      // merge both ways

      val expectedMerge = Map("a" -> Set("A"), "b" -> Set("B"), "c" -> Set("C"))

      val merged1 = m1.merge(m2)
      merged1.entries should be(expectedMerge)

      val merged2 = m2.merge(m1)
      merged2.entries should be(expectedMerge)
    }

    "be able to have its entries correctly merged with another ORMultiMap with overlapping entries" in {
      val m1 = ORMultiMap()
        .addBinding(node1, "a", "A1")
        .addBinding(node1, "b", "B1")
        .removeBinding(node1, "a", "A1")
        .addBinding(node1, "d", "D1")
      val m2 = ORMultiMap()
        .addBinding(node2, "c", "C2")
        .addBinding(node2, "a", "A2")
        .addBinding(node2, "b", "B2")
        .removeBinding(node2, "b", "B2")
        .addBinding(node2, "d", "D2")

      // merge both ways

      val expectedMerged = Map("a" -> Set("A2"), "b" -> Set("B1"), "c" -> Set("C2"), "d" -> Set("D1", "D2"))

      val merged1 = m1.merge(m2)
      merged1.entries should be(expectedMerged)

      val merged2 = m2.merge(m1)
      merged2.entries should be(expectedMerged)

      val merged3 = m1.mergeDelta(m2.delta.get)
      merged3.entries should be(expectedMerged)

      val merged4 = m2.mergeDelta(m1.delta.get)
      merged4.entries should be(expectedMerged)
    }
  }

  "be able to have its entries correctly merged with another ORMultiMap with overlapping entries 2" in {
    val m1 = ORMultiMap().addBinding(node1, "b", "B1")
    val m2 = ORMultiMap().addBinding(node2, "b", "B2").remove(node2, "b")

    // merge both ways

    val expectedMerged = Map("b" -> Set("B1"))

    val merged1 = m1.merge(m2)
    merged1.entries should be(expectedMerged)

    val merged2 = m2.merge(m1)
    merged2.entries should be(expectedMerged)

    val merged3 = m1.mergeDelta(m2.delta.get)
    merged3.entries should be(expectedMerged)

    val merged4 = m2.mergeDelta(m1.delta.get)
    merged4.entries should be(expectedMerged)
  }

  "not have anomalies for remove+updated scenario and deltas" in {
    val m2a = ORMultiMap.empty[String, String].addBinding(node1, "q", "Q").removeBinding(node1, "q", "Q")
    val m1 = ORMultiMap
      .empty[String, String]
      .addBinding(node1, "z", "Z")
      .addBinding(node2, "x", "X")
      .removeBinding(node1, "z", "Z")

    val m2 = m2a.resetDelta.removeBinding(node2, "a", "A")

    val merged1 = m1.merge(m2)

    merged1.contains("a") should be(false)

    val merged2 = m1.mergeDelta(m2.delta.get)

    merged2.contains("a") should be(false)
  }

  "be able to get all bindings for an entry and then reduce them upon putting them back" in {
    val m = ORMultiMap().addBinding(node1, "a", "A1").addBinding(node1, "a", "A2").addBinding(node1, "b", "B1")
    val a = m.get("a").get

    a should be(Set("A1", "A2"))

    val m2 = m.put(node1, "a", a - "A1")

    val expectedMerged = Map("a" -> Set("A2"), "b" -> Set("B1"))

    m2.entries should be(expectedMerged)
  }

  "return the value for an existing key and the default for a non-existing one when using getOrElse" in {
    val m = ORMultiMap().addBinding(node1, "a", "A")
    m.getOrElse("a", Set("B")) shouldBe Set("A")
    m.getOrElse("b", Set("B")) shouldBe Set("B")
  }

  "remove all bindings for a given key" in {
    val m = ORMultiMap().addBinding(node1, "a", "A1").addBinding(node1, "a", "A2").addBinding(node1, "b", "B1")
    val m2 = m.remove(node1, "a")
    m2.entries should be(Map("b" -> Set("B1")))
  }

  "not have usual anomalies for remove+addBinding scenario and delta-deltas" in {
    val m1 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m2 = ORMultiMap.emptyWithValueDeltas[String, String].put(node2, "c", Set("C"))

    val merged1 = m1.merge(m2)

    val m3 = merged1.resetDelta.remove(node1, "b")
    val m4 = m3.resetDelta.addBinding(node1, "b", "B2")

    val merged2 = m3.merge(m4)

    merged2.entries("a") should be(Set("A"))
    merged2.entries("b") should be(Set("B2"))
    merged2.entries("c") should be(Set("C"))

    val merged3 = m3.mergeDelta(m4.delta.get)

    merged3.entries("a") should be(Set("A"))
    merged3.entries("b") should be(Set("B2"))
    merged3.entries("c") should be(Set("C"))

    val merged4 = merged1.merge(m3).merge(m4)

    merged4.entries("a") should be(Set("A"))
    merged4.entries("b") should be(Set("B2"))
    merged4.entries("c") should be(Set("C"))

    val merged5 = merged1.mergeDelta(m3.delta.get).mergeDelta(m4.delta.get)

    merged5.entries("a") should be(Set("A"))
    merged5.entries("b") should be(Set("B2"))
    merged5.entries("c") should be(Set("C"))

    val merged6 = merged1.mergeDelta(m3.delta.get.merge(m4.delta.get))

    merged6.entries("a") should be(Set("A"))
    merged6.entries("b") should be(Set("B2"))
    merged6.entries("c") should be(Set("C"))
  }

  "not have usual anomalies for remove+addBinding scenario and delta-deltas 2" in {
    // the new delta-delta ORMultiMap is free from this anomaly
    val m1 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m2 = ORMultiMap.emptyWithValueDeltas[String, String].put(node2, "c", Set("C"))

    // m1 - node1 gets the update from m2
    val merged1 = m1.merge(m2)
    // m2 - node2 gets the update from m1
    val merged2 = m2.merge(m1)

    // no race condition
    val m3 = merged1.resetDelta.remove(node1, "b")
    // let's imagine that m3 (node1) update gets propagated here (full state or delta - doesn't matter)
    // and is in flight, but in the meantime, an element is being added somewhere else (m4 - node2)
    // and the update is propagated before the update from node1 is merged
    val m4 = merged2.resetDelta.addBinding(node2, "b", "B2")
    // and later merged on node1
    val merged3 = m3.merge(m4)
    // and the other way round...
    val merged4 = m4.merge(m3)

    // result -  the element "B" is kept on both sides...
    merged3.entries("a") should be(Set("A"))
    merged3.entries("b") should be(Set("B2"))
    merged3.entries("c") should be(Set("C"))

    merged4.entries("a") should be(Set("A"))
    merged4.entries("b") should be(Set("B2"))
    merged4.entries("c") should be(Set("C"))

    // but if the timing was slightly different, so that the update from node1
    // would get merged just before update on node2:
    val merged5 = m2.merge(m3).resetDelta.addBinding(node2, "b", "B2")
    // the update propagated ... and merged on node1:
    val merged6 = m3.merge(merged5)

    // then the outcome would be the same...
    merged5.entries("a") should be(Set("A"))
    merged5.entries("b") should be(Set("B2"))
    merged5.entries("c") should be(Set("C"))

    merged6.entries("a") should be(Set("A"))
    merged6.entries("b") should be(Set("B2"))
    merged6.entries("c") should be(Set("C"))
  }

  "work with delta-coalescing scenario 1" in {
    val m1 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m2 = m1.resetDelta.put(node2, "b", Set("B2")).addBinding(node2, "b", "B3")

    val merged1 = m1.merge(m2)

    merged1.entries("a") should be(Set("A"))
    merged1.entries("b") should be(Set("B2", "B3"))

    val merged2 = m1.mergeDelta(m2.delta.get)

    merged2.entries("a") should be(Set("A"))
    merged2.entries("b") should be(Set("B2", "B3"))

    val m3 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m4 = m3.resetDelta.put(node2, "b", Set("B2")).put(node2, "b", Set("B3"))

    val merged3 = m3.merge(m4)

    merged3.entries("a") should be(Set("A"))
    merged3.entries("b") should be(Set("B3"))

    val merged4 = m3.mergeDelta(m4.delta.get)

    merged4.entries("a") should be(Set("A"))
    merged4.entries("b") should be(Set("B3"))

    val m5 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m6 = m5.resetDelta.put(node2, "b", Set("B2")).addBinding(node2, "b", "B3").addBinding(node2, "b", "B4")

    val merged5 = m5.merge(m6)

    merged5.entries("a") should be(Set("A"))
    merged5.entries("b") should be(Set("B2", "B3", "B4"))

    val merged6 = m5.mergeDelta(m6.delta.get)

    merged6.entries("a") should be(Set("A"))
    merged6.entries("b") should be(Set("B2", "B3", "B4"))

    val m7 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m8 = m7.resetDelta.put(node2, "d", Set("D")).addBinding(node2, "b", "B3").put(node2, "b", Set("B4"))

    val merged7 = m7.merge(m8)

    merged7.entries("a") should be(Set("A"))
    merged7.entries("b") should be(Set("B4"))
    merged7.entries("d") should be(Set("D"))

    val merged8 = m7.mergeDelta(m8.delta.get)

    merged8.entries("a") should be(Set("A"))
    merged8.entries("b") should be(Set("B4"))
    merged8.entries("d") should be(Set("D"))

    val m9 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m10 = m9.resetDelta.addBinding(node2, "b", "B3").addBinding(node2, "b", "B4")

    val merged9 = m9.merge(m10)

    merged9.entries("a") should be(Set("A"))
    merged9.entries("b") should be(Set("B", "B3", "B4"))

    val merged10 = m9.mergeDelta(m10.delta.get)

    merged10.entries("a") should be(Set("A"))
    merged10.entries("b") should be(Set("B", "B3", "B4"))

    val m11 = ORMultiMap
      .emptyWithValueDeltas[String, String]
      .put(node1, "a", Set("A"))
      .put(node1, "b", Set("B", "B1"))
      .remove(node1, "b")
    val m12 = m11.resetDelta.addBinding(node2, "b", "B2").addBinding(node2, "b", "B3")

    val merged11 = m11.merge(m12)

    merged11.entries("a") should be(Set("A"))
    merged11.entries("b") should be(Set("B2", "B3"))

    val merged12 = m11.mergeDelta(m12.delta.get)

    merged12.entries("a") should be(Set("A"))
    merged12.entries("b") should be(Set("B2", "B3"))

    val m13 = ORMultiMap
      .emptyWithValueDeltas[String, String]
      .put(node1, "a", Set("A"))
      .put(node1, "b", Set("B", "B1"))
      .remove(node1, "b")
    val m14 = m13.resetDelta.addBinding(node2, "b", "B2").put(node2, "b", Set("B3"))

    val merged13 = m13.merge(m14)

    merged13.entries("a") should be(Set("A"))
    merged13.entries("b") should be(Set("B3"))

    val merged14 = m13.mergeDelta(m14.delta.get)

    merged14.entries("a") should be(Set("A"))
    merged14.entries("b") should be(Set("B3"))

    val m15 = ORMultiMap
      .emptyWithValueDeltas[String, String]
      .put(node1, "a", Set("A"))
      .put(node1, "b", Set("B", "B1"))
      .put(node1, "c", Set("C"))
    val m16 = m15.resetDelta.addBinding(node2, "b", "B2").addBinding(node2, "c", "C1")

    val merged15 = m15.merge(m16)

    merged15.entries("a") should be(Set("A"))
    merged15.entries("b") should be(Set("B", "B1", "B2"))
    merged15.entries("c") should be(Set("C", "C1"))

    val merged16 = m15.mergeDelta(m16.delta.get)

    merged16.entries("a") should be(Set("A"))
    merged16.entries("b") should be(Set("B", "B1", "B2"))
    merged16.entries("c") should be(Set("C", "C1"))

    // somewhat artificial setup
    val m17 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B", "B1"))
    val m18 = m17.resetDelta.addBinding(node2, "b", "B2")
    val m19 = ORMultiMap.emptyWithValueDeltas[String, String].resetDelta.put(node2, "b", Set("B3"))

    val merged17 = m17.merge(m18).merge(m19)

    merged17.entries("a") should be(Set("A"))
    merged17.entries("b") should be(Set("B", "B1", "B3"))

    val merged18 = m17.mergeDelta(m18.delta.get.merge(m19.delta.get))

    merged18.entries("a") should be(Set("A"))
    merged18.entries("b") should be(Set("B", "B1", "B3"))
  }

  "work with delta-coalescing scenario 2" in {
    val m1 = ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m2 = m1.resetDelta.put(node2, "b", Set("B2")).addBinding(node2, "b", "B3")

    val merged1 = m1.merge(m2)

    merged1.entries("a") should be(Set("A"))
    merged1.entries("b") should be(Set("B2", "B3"))

    val merged2 = m1.mergeDelta(m2.delta.get)

    merged2.entries("a") should be(Set("A"))
    merged2.entries("b") should be(Set("B2", "B3"))

    val m3 = ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m4 = m3.resetDelta.put(node2, "b", Set("B2")).put(node2, "b", Set("B3"))

    val merged3 = m3.merge(m4)

    merged3.entries("a") should be(Set("A"))
    merged3.entries("b") should be(Set("B3"))

    val merged4 = m3.mergeDelta(m4.delta.get)

    merged4.entries("a") should be(Set("A"))
    merged4.entries("b") should be(Set("B3"))

    val m5 = ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m6 = m5.resetDelta.put(node2, "b", Set("B2")).addBinding(node2, "b", "B3").addBinding(node2, "b", "B4")

    val merged5 = m5.merge(m6)

    merged5.entries("a") should be(Set("A"))
    merged5.entries("b") should be(Set("B2", "B3", "B4"))

    val merged6 = m5.mergeDelta(m6.delta.get)

    merged6.entries("a") should be(Set("A"))
    merged6.entries("b") should be(Set("B2", "B3", "B4"))

    val m7 = ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m8 = m7.resetDelta.put(node2, "d", Set("D")).addBinding(node2, "b", "B3").put(node2, "b", Set("B4"))

    val merged7 = m7.merge(m8)

    merged7.entries("a") should be(Set("A"))
    merged7.entries("b") should be(Set("B4"))
    merged7.entries("d") should be(Set("D"))

    val merged8 = m7.mergeDelta(m8.delta.get)

    merged8.entries("a") should be(Set("A"))
    merged8.entries("b") should be(Set("B4"))
    merged8.entries("d") should be(Set("D"))

    val m9 = ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B"))
    val m10 = m9.resetDelta.addBinding(node2, "b", "B3").addBinding(node2, "b", "B4")

    val merged9 = m9.merge(m10)

    merged9.entries("a") should be(Set("A"))
    merged9.entries("b") should be(Set("B", "B3", "B4"))

    val merged10 = m9.mergeDelta(m10.delta.get)

    merged10.entries("a") should be(Set("A"))
    merged10.entries("b") should be(Set("B", "B3", "B4"))

    val m11 =
      ORMultiMap.empty[String, String].put(node1, "a", Set("A")).put(node1, "b", Set("B", "B1")).remove(node1, "b")
    val m12 = ORMultiMap.empty[String, String].addBinding(node2, "b", "B2").addBinding(node2, "b", "B3")

    val merged11 = m11.merge(m12)

    merged11.entries("a") should be(Set("A"))
    merged11.entries("b") should be(Set("B2", "B3"))

    val merged12 = m11.mergeDelta(m12.delta.get)

    merged12.entries("a") should be(Set("A"))
    merged12.entries("b") should be(Set("B2", "B3"))
  }

  "work with tombstones for ORMultiMap.withValueDeltas and its delta-delta operations" in {
    // ORMultiMap.withValueDeltas has the following (public) interface:
    // put - place (or replace) a value in a destructive way - no tombstone is created
    //       this can be seen in the relevant delta: PutDeltaOp(AddDeltaOp(ORSet(a)),(a,ORSet()),ORMultiMapWithValueDeltasTag)
    // remove - to avoid anomalies that ORMultiMap has, value for the key being removed is being cleared
    //          before key removal, this can be seen in the following deltas created by the remove op (depending on situation):
    //          DeltaGroup(Vector(PutDeltaOp(AddDeltaOp(ORSet(a)),(a,ORSet()),ORMultiMapWithValueDeltasTag), RemoveKeyDeltaOp(RemoveDeltaOp(ORSet(a)),a,ORMultiMapWithValueDeltasTag)))
    //          DeltaGroup(Vector(UpdateDeltaOp(AddDeltaOp(ORSet(c)),Map(c -> FullStateDeltaOp(ORSet())),ORMultiMapWithValueDeltasTag), RemoveKeyDeltaOp(RemoveDeltaOp(ORSet(c)),c,ORMultiMapWithValueDeltasTag)))
    //          after applying the remove operation the tombstone for the given map looks as follows: Map(a -> ORSet()) (or Map(c -> ORSet()) )

    val m1 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A"))
    val m2 = m1.resetDelta.remove(node1, "a")

    val m3 = m1.mergeDelta(m2.delta.get)
    val m4 = m1.merge(m2)

    m3.underlying.values
      .contains("a") should be(false) // tombstone for 'a' has been optimized away at the end of the mergeDelta
    m4.underlying.values
      .contains("a") should be(false) // tombstone for 'a' has been optimized away at the end of the merge

    val m5 = ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A1"))
    m3.mergeDelta(m5.delta.get).entries("a") should ===(Set("A1"))
    m4.mergeDelta(m5.delta.get).entries("a") should ===(Set("A1"))
    m4.merge(m5).entries("a") should ===(Set("A1"))

    // addBinding - add a binding for a certain value - no tombstone is created
    //              this operation works through "updated" call of the underlying ORMap, that is not exposed
    //              in the ORMultiMap interface
    //              the side-effect of addBinding is that it can lead to anomalies with the standard "ORMultiMap"

    // removeBinding - remove binding for a certain value, and if there are no more remaining elements, remove
    //                 the now superfluous key, please note that for .withValueDeltas variant tombstone will be created

    val um1 = ORMultiMap.emptyWithValueDeltas[String, String].addBinding(node1, "a", "A")
    val um2 = um1.resetDelta.removeBinding(node1, "a", "A")

    val um3 = um1.mergeDelta(um2.delta.get)
    val um4 = um1.merge(um2)

    um3.underlying.values
      .contains("a") should be(false) // tombstone for 'a' has been optimized away at the end of the mergeDelta
    um4.underlying.values
      .contains("a") should be(false) // tombstone for 'a' has been optimized away at the end of the merge

    val um5 = ORMultiMap.emptyWithValueDeltas[String, String].addBinding(node1, "a", "A1")
    um3.mergeDelta(um5.delta.get).entries("a") should ===(Set("A1"))
    um4.mergeDelta(um5.delta.get).entries("a") should ===(Set("A1"))
    um4.merge(um5).entries("a") should ===(Set("A1"))

    // replaceBinding - that would first addBinding for new binding and then removeBinding for old binding
    //                  so no tombstone would be created

    // so the only option to create a tombstone with non-zero (!= Set() ) contents would be to call removeKey (not remove!)
    // for the underlying ORMap (or have a removeKeyOp delta that does exactly that)
    // but this is not possible in applications, as both remove and removeKey operations are API of internal ORMap
    // and are not externally exposed in the ORMultiMap, and deltas are causal, so removeKeyOp delta cannot arise
    // without previous delta containing 'clear' or 'put' operation setting the tombstone at Set()
    // the example shown below cannot happen in practice

    val tm1 = new ORMultiMap(
      ORMultiMap.emptyWithValueDeltas[String, String].addBinding(node1, "a", "A").underlying.removeKey(node1, "a"),
      true)
    tm1.underlying.values("a").elements should ===(Set("A")) // tombstone
    tm1.addBinding(node1, "a", "A1").entries("a") should be(Set("A", "A1"))
    val tm2 =
      ORMultiMap.emptyWithValueDeltas[String, String].put(node1, "a", Set("A")).resetDelta.addBinding(node1, "a", "A1")
    tm1.mergeDelta(tm2.delta.get).entries("a") should be(Set("A", "A1"))
    tm1.merge(tm2).entries("a") should be(Set("A", "A1"))
    val tm3 = new ORMultiMap(
      ORMultiMap.emptyWithValueDeltas[String, String].addBinding(node1, "a", "A").underlying.remove(node1, "a"),
      true)
    tm3.underlying.contains("a") should ===(false) // no tombstone, because remove not removeKey
    tm3
      .mergeDelta(tm2.delta.get)
      .entries should ===(Map.empty[String, String]) // no tombstone - update delta could not be applied
    tm3.merge(tm2).entries should ===(Map.empty[String, String])

    // The only valid value for tombstone created by means of either API call or application of delta propagation is Set()
    // which is then garbage collected at every `merge` and `mergeDelta` operation.
    // Hence in the case of valid API usage and normal operation of delta propagation no tombstones will be permanently created.
  }

  "have unapply extractor" in {
    val m1 = ORMultiMap.empty.put(node1, "a", Set(1L, 2L)).put(node2, "b", Set(3L))
    val _: ORMultiMap[String, Long] = m1
    val entries1 = m1 match {
      case ORMultiMap(entries1) => entries1
      case _                    => fail()
    }
    val entries2: Map[String, Set[Long]] = entries1
    entries2 should be(Map("a" -> Set(1L, 2L), "b" -> Set(3L)))

    Changed(ORMultiMapKey[String, Long]("key"))(m1) match {
      case c @ Changed(ORMultiMapKey("key")) =>
        val entries3 = c.dataValue match {
          case ORMultiMap(entries3: Map[String, Set[Long]]) => entries3
          case _                                            => fail()
        }
        val entries4: Map[String, Set[Long]] = entries3
        entries4 should be(Map("a" -> Set(1L, 2L), "b" -> Set(3L)))
      case changed =>
        fail(s"Failed to match [$changed]")
    }
  }
}
