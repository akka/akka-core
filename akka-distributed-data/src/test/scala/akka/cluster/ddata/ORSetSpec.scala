/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.collection.immutable.TreeMap

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Changed

class ORSetSpec extends AnyWordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)

  val nodeA = UniqueAddress(Address("akka", "Sys", "a", 2552), 1L)
  val nodeB = UniqueAddress(nodeA.address.copy(host = Some("b")), 2L)
  val nodeC = UniqueAddress(nodeA.address.copy(host = Some("c")), 3L)
  val nodeD = UniqueAddress(nodeA.address.copy(host = Some("d")), 4L)
  val nodeE = UniqueAddress(nodeA.address.copy(host = Some("e")), 5L)
  val nodeF = UniqueAddress(nodeA.address.copy(host = Some("f")), 6L)
  val nodeG = UniqueAddress(nodeA.address.copy(host = Some("g")), 7L)
  val nodeH = UniqueAddress(nodeA.address.copy(host = Some("h")), 8L)

  val user1 = """{"username":"john","password":"coltrane"}"""
  val user2 = """{"username":"sonny","password":"rollins"}"""
  val user3 = """{"username":"charlie","password":"parker"}"""
  val user4 = """{"username":"charles","password":"mingus"}"""

  "A ORSet" must {

    "be able to add user" in {
      val c1 = ORSet()

      val c2 = c1.add(node1, user1)
      val c3 = c2.add(node1, user2)

      val c4 = c3.add(node1, user4)
      val c5 = c4.add(node1, user3)

      c5.elements should contain(user1)
      c5.elements should contain(user2)
      c5.elements should contain(user3)
      c5.elements should contain(user4)
    }

    "be able to remove added user" in {
      val c1 = ORSet()

      val c2 = c1.add(node1, user1)
      val c3 = c2.add(node1, user2)

      val c4 = c3.remove(node1, user2)
      val c5 = c4.remove(node1, user1)

      c5.elements should not contain (user1)
      c5.elements should not contain (user2)

      val c6 = c3.merge(c5)
      c6.elements should not contain (user1)
      c6.elements should not contain (user2)

      val c7 = c5.merge(c3)
      c7.elements should not contain (user1)
      c7.elements should not contain (user2)
    }

    "be able to add removed" in {
      val c1 = ORSet()
      val c2 = c1.remove(node1, user1)
      val c3 = c2.add(node1, user1)
      c3.elements should contain(user1)
      val c4 = c3.remove(node1, user1)
      c4.elements should not contain (user1)
      val c5 = c4.add(node1, user1)
      c5.elements should contain(user1)
    }

    "be able to remove and add several times" in {
      val c1 = ORSet()

      val c2 = c1.add(node1, user1)
      val c3 = c2.add(node1, user2)
      val c4 = c3.remove(node1, user1)
      c4.elements should not contain (user1)
      c4.elements should contain(user2)

      val c5 = c4.add(node1, user1)
      val c6 = c5.add(node1, user2)
      c6.elements should contain(user1)
      c6.elements should contain(user2)

      val c7 = c6.remove(node1, user1)
      val c8 = c7.add(node1, user2)
      val c9 = c8.remove(node1, user1)
      c9.elements should not contain (user1)
      c9.elements should contain(user2)
    }

    "be able to have its user set correctly merged with another ORSet with unique user sets" in {
      // set 1
      val c1 = ORSet().add(node1, user1).add(node1, user2)
      c1.elements should contain(user1)
      c1.elements should contain(user2)

      // set 2
      val c2 = ORSet().add(node2, user3).add(node2, user4).remove(node2, user3)

      c2.elements should not contain (user3)
      c2.elements should contain(user4)

      // merge both ways
      val merged1 = c1.merge(c2)
      merged1.elements should contain(user1)
      merged1.elements should contain(user2)
      merged1.elements should not contain (user3)
      merged1.elements should contain(user4)

      val merged2 = c2.merge(c1)
      merged2.elements should contain(user1)
      merged2.elements should contain(user2)
      merged2.elements should not contain (user3)
      merged2.elements should contain(user4)
    }

    "be able to have its user set correctly merged with another ORSet with overlapping user sets" in {
      // set 1
      val c1 = ORSet().add(node1, user1).add(node1, user2).add(node1, user3).remove(node1, user1).remove(node1, user3)

      c1.elements should not contain (user1)
      c1.elements should contain(user2)
      c1.elements should not contain (user3)

      // set 2
      val c2 = ORSet().add(node2, user1).add(node2, user2).add(node2, user3).add(node2, user4).remove(node2, user3)

      c2.elements should contain(user1)
      c2.elements should contain(user2)
      c2.elements should not contain (user3)
      c2.elements should contain(user4)

      // merge both ways
      val merged1 = c1.merge(c2)
      merged1.elements should contain(user1)
      merged1.elements should contain(user2)
      merged1.elements should not contain (user3)
      merged1.elements should contain(user4)

      val merged2 = c2.merge(c1)
      merged2.elements should contain(user1)
      merged2.elements should contain(user2)
      merged2.elements should not contain (user3)
      merged2.elements should contain(user4)
    }

    "be able to have its user set correctly merged for concurrent updates" in {
      val c1 = ORSet().add(node1, user1).add(node1, user2).add(node1, user3)

      c1.elements should contain(user1)
      c1.elements should contain(user2)
      c1.elements should contain(user3)

      val c2 = c1.add(node2, user1).remove(node2, user2).remove(node2, user3)

      c2.elements should contain(user1)
      c2.elements should not contain (user2)
      c2.elements should not contain (user3)

      // merge both ways
      val merged1 = c1.merge(c2)
      merged1.elements should contain(user1)
      merged1.elements should not contain (user2)
      merged1.elements should not contain (user3)

      val merged2 = c2.merge(c1)
      merged2.elements should contain(user1)
      merged2.elements should not contain (user2)
      merged2.elements should not contain (user3)

      val c3 = c1.add(node1, user4).remove(node1, user3).add(node1, user2)

      // merge both ways
      val merged3 = c2.merge(c3)
      merged3.elements should contain(user1)
      merged3.elements should contain(user2)
      merged3.elements should not contain (user3)
      merged3.elements should contain(user4)

      val merged4 = c3.merge(c2)
      merged4.elements should contain(user1)
      merged4.elements should contain(user2)
      merged4.elements should not contain (user3)
      merged4.elements should contain(user4)
    }

    "be able to have its user set correctly merged after remove" in {
      val c1 = ORSet().add(node1, user1).add(node1, user2)
      val c2 = c1.remove(node2, user2)

      // merge both ways
      val merged1 = c1.merge(c2)
      merged1.elements should contain(user1)
      merged1.elements should not contain (user2)

      val merged2 = c2.merge(c1)
      merged2.elements should contain(user1)
      merged2.elements should not contain (user2)

      val c3 = c1.add(node1, user3)

      // merge both ways
      val merged3 = c3.merge(c2)
      merged3.elements should contain(user1)
      merged3.elements should not contain (user2)
      merged3.elements should contain(user3)

      val merged4 = c2.merge(c3)
      merged4.elements should contain(user1)
      merged4.elements should not contain (user2)
      merged4.elements should contain(user3)
    }

  }

  "ORSet deltas" must {

    def addDeltaOp(s: ORSet[String]): ORSet.AddDeltaOp[String] =
      asAddDeltaOp(s.delta.get)

    def asAddDeltaOp(delta: Any): ORSet.AddDeltaOp[String] =
      delta match {
        case d: ORSet.AddDeltaOp[String] @unchecked => d
        case _                                      => throw new IllegalArgumentException("Expected AddDeltaOp")
      }

    "work for additions" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.add(node1, "a")
      addDeltaOp(s2).underlying.elements should ===(Set("a"))
      s1.mergeDelta(s2.delta.get) should ===(s2)

      val s3 = s2.resetDelta.add(node1, "b").add(node1, "c")
      addDeltaOp(s3).underlying.elements should ===(Set("b", "c"))
      s2.mergeDelta(s3.delta.get) should ===(s3)

      // another node adds "d"
      val s4 = s3.resetDelta.add(node2, "d")
      addDeltaOp(s4).underlying.elements should ===(Set("d"))
      s3.mergeDelta(s4.delta.get) should ===(s4)

      // concurrent update
      val s5 = s3.resetDelta.add(node1, "e")
      val s6 = s5.merge(s4)
      s5.mergeDelta(s4.delta.get) should ===(s6)

      // concurrent add of same element
      val s7 = s3.resetDelta.add(node1, "d")
      val s8 = s7.merge(s4)
      // the dot contains both nodes
      s8.elementsMap("d").contains(node1)
      s8.elementsMap("d").contains(node2)
      // and same result when merging the deltas
      s7.mergeDelta(s4.delta.get) should ===(s8)
      s4.mergeDelta(s7.delta.get) should ===(s8)
    }

    "handle another concurrent add scenario" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.add(node1, "a")
      val s3 = s2.add(node1, "b")
      val s4 = s2.add(node2, "c")

      // full state merge for reference
      val s5 = s4.merge(s3)
      s5.elements should ===(Set("a", "b", "c"))

      val s6 = s4.mergeDelta(s3.delta.get)
      s6.elements should ===(Set("a", "b", "c"))
    }

    "merge deltas into delta groups" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.add(node1, "a")
      val d2 = s2.delta.get
      val s3 = s2.resetDelta.add(node1, "b")
      val d3 = s3.delta.get
      val d4 = d2.merge(d3)
      asAddDeltaOp(d4).underlying.elements should ===(Set("a", "b"))
      s1.mergeDelta(d4) should ===(s3)
      s2.mergeDelta(d4) should ===(s3)

      val s5 = s3.resetDelta.remove(node1, "b")
      val d5 = s5.delta.get
      val d6 = d4.merge(d5).asInstanceOf[ORSet.DeltaGroup[String]]
      d6.ops.last.getClass should ===(classOf[ORSet.RemoveDeltaOp[String]])
      d6.ops.size should ===(2)
      s3.mergeDelta(d6) should ===(s5)

      val s7 = s5.resetDelta.add(node1, "c")
      val s8 = s7.resetDelta.add(node1, "d")
      val d9 = d6.merge(s7.delta.get).merge(s8.delta.get).asInstanceOf[ORSet.DeltaGroup[String]]
      // the add "c" and add "d" are merged into one AddDeltaOp
      asAddDeltaOp(d9.ops.last).underlying.elements should ===(Set("c", "d"))
      d9.ops.size should ===(3)
      s5.mergeDelta(d9) should ===(s8)
      s5.mergeDelta(s7.delta.get).mergeDelta(s8.delta.get) should ===(s8)
    }

    "work for removals" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.add(node1, "a").add(node1, "b").resetDelta
      val s3 = s2.remove(node1, "b")
      s2.merge(s3) should ===(s3)
      s2.mergeDelta(s3.delta.get) should ===(s3)
      s2.mergeDelta(s3.delta.get).elements should ===(Set("a"))

      // concurrent update
      val s4 = s2.add(node2, "c").resetDelta
      val s5 = s4.merge(s3)
      s5.elements should ===(Set("a", "c"))
      s4.mergeDelta(s3.delta.get) should ===(s5)

      // add "b" again
      val s6 = s5.add(node2, "b")
      // merging the old delta should not remove it
      s6.mergeDelta(s3.delta.get) should ===(s6)
      s6.mergeDelta(s3.delta.get).elements should ===(Set("a", "b", "c"))
    }

    "work for clear" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.add(node1, "a").add(node1, "b")
      val s3 = s2.resetDelta.clear()
      val s4 = s3.resetDelta.add(node1, "c")
      s2.merge(s3) should ===(s3)
      s2.mergeDelta(s3.delta.get) should ===(s3)
      val s5 = s2.mergeDelta(s3.delta.get).mergeDelta(s4.delta.get)
      s5.elements should ===(Set("c"))
      s5 should ===(s4)

      // concurrent update
      val s6 = s2.resetDelta.add(node2, "d")
      val s7 = s6.merge(s3)
      s7.elements should ===(Set("d"))
      s6.mergeDelta(s3.delta.get) should ===(s7)

      // add "b" again
      val s8 = s7.add(node2, "b")
      // merging the old delta should not remove it
      s8.mergeDelta(s3.delta.get) should ===(s8)
      s8.mergeDelta(s3.delta.get).elements should ===(Set("b", "d"))
    }

    "handle a mixed add/remove scenario" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.resetDelta.remove(node1, "e")
      val s3 = s2.resetDelta.add(node1, "b")
      val s4 = s3.resetDelta.add(node1, "a")
      val s5 = s4.resetDelta.remove(node1, "b")

      val deltaGroup1 = s3.delta.get.merge(s4.delta.get).merge(s5.delta.get)

      val s7 = s2.mergeDelta(deltaGroup1)
      s7.elements should ===(Set("a"))
      // The above scenario was constructed from failing ReplicatorDeltaSpec,
      // some more checks...

      val s8 = s2.resetDelta.add(node2, "z") // concurrent update from node2
      val s9 = s8.mergeDelta(deltaGroup1)
      s9.elements should ===(Set("a", "z"))
    }

    "handle a mixed add/remove scenario 2" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.resetDelta.add(node1, "a")
      val s3 = s2.resetDelta.add(node1, "b")
      val s4 = s3.resetDelta.add(node2, "a")
      val s5 = s4.resetDelta.remove(node1, "a")

      s5.elements should ===(Set("b"))

      val delta1 = s2.delta.get.merge(s3.delta.get)
      val delta2 = s4.delta.get

      val t1 = ORSet.empty[String]
      val t2 = t1.mergeDelta(delta1).mergeDelta(delta2)
      t2.elements should ===(Set("a", "b"))
      val t3 = t2.resetDelta.add(node3, "z")

      val t4 = t3.mergeDelta(s5.delta.get)

      t4.elements should ===(Set("b", "z"))
    }

    "handle a mixed add/remove scenario 3" in {
      val s1 = ORSet.empty[String]
      val s2 = s1.resetDelta.add(node1, "a")
      val s3 = s2.resetDelta.add(node1, "b")
      val s4 = s3.resetDelta.add(node2, "a")
      val s5 = s4.resetDelta.remove(node1, "a")

      s5.elements should ===(Set("b"))

      val delta1 = s2.delta.get.merge(s3.delta.get)

      val t1 = ORSet.empty[String]

      val t2 = t1.mergeDelta(delta1)
      t2.elements should ===(Set("a", "b"))
      val t3 = t2.resetDelta.add(node3, "a")

      val t4 = t3.mergeDelta(s5.delta.get)

      t4.elements should ===(Set("b", "a"))
    }

    "not have anomalies for ORSet in complex but realistic scenario" in {
      val node1_1 = ORSet.empty[String].add(node1, "q").remove(node1, "q")
      val delta1_1 = node1_1.delta.get
      val node1_2 = node1_1.resetDelta.resetDelta.add(node1, "z").remove(node1, "z")
      val delta1_2 = node1_2.delta.get
      // we finished doing stuff on node1 - there are two separate deltas that will be propagated
      // node2 is created, then gets first delta from node1 and then adds an element "x"
      val node2_1 = ORSet.empty[String].mergeDelta(delta1_1).resetDelta.add(node2, "x")
      val delta2_1 = node2_1.delta.get
      // node2 continues its existence adding and later removing the element
      // it still didn't get the second update from node1 (that is fully legit :) )
      val node2_2 = node2_1.resetDelta.add(node2, "a").remove(node2, "a")
      val delta2_2 = node2_2.delta.get

      // in the meantime there is some node3
      // there is not much activity on it, it just gets the first delta from node1 then it gets
      // first delta from node2
      // then it gets the second delta from node1 (that node2 still didn't get, but, hey!, this is fine)
      val node3_1 = ORSet.empty[String].mergeDelta(delta1_1).mergeDelta(delta2_1).mergeDelta(delta1_2)

      // and node3_1 receives full update from node2 via gossip
      val merged1 = node3_1.merge(node2_2)

      merged1.contains("a") should be(false)

      // and node3_1 receives delta update from node2 (it just needs to get the second delta,
      // as it already got the first delta just a second ago)

      val merged2 = node3_1.mergeDelta(delta2_2)

      val mg2 = merged2 match {
        case ORSet(mg2) => mg2
        case _          => fail()
      }
      mg2 should be(Set("x")) // !!!
    }

    "require causal delivery of deltas" in {
      // This test illustrates why we need causal delivery of deltas.
      // Otherwise the following could happen.

      // s0 is the stable state that is initially replicated to all nodes
      val s0 = ORSet.empty[String].add(node1, "a")

      // add element "b" and "c" at node1
      val s11 = s0.resetDelta.add(node1, "b")
      val s12 = s11.resetDelta.add(node1, "c")

      // at the same time, add element "d" at node2
      val s21 = s0.resetDelta.add(node2, "d")

      // node3 receives delta for "d" and "c", but the delta for "b" is lost
      val s31 = s0.mergeDelta(s21.delta.get).mergeDelta(s12.delta.get)
      s31.elements should ===(Set("a", "c", "d"))

      // node4 receives all deltas
      val s41 = s0.mergeDelta(s11.delta.get).mergeDelta(s12.delta.get).mergeDelta(s21.delta.get)
      s41.elements should ===(Set("a", "b", "c", "d"))

      // node3 and node4 sync with full state gossip
      val s32 = s31.merge(s41)
      // one would expect elements "a", "b", "c", "d", but "b" is removed
      // because we applied s12.delta without applying s11.delta
      s32.elements should ===(Set("a", "c", "d"))
    }

  }

  "ORSet unit test" must {
    "verify subtractDots" in {
      val dot = VersionVector(TreeMap(nodeA -> 3L, nodeB -> 2L, nodeD -> 14L, nodeG -> 22L))
      val vvector =
        VersionVector(TreeMap(nodeA -> 4L, nodeB -> 1L, nodeC -> 1L, nodeD -> 14L, nodeE -> 5L, nodeF -> 2L))
      val expected = VersionVector(TreeMap(nodeB -> 2L, nodeG -> 22L))
      ORSet.subtractDots(dot, vvector) should be(expected)
    }

    "verify mergeCommonKeys" in {
      val commonKeys: Set[String] = Set("K1", "K2")
      val thisDot1 = VersionVector(TreeMap(nodeA -> 3L, nodeD -> 7L))
      val thisDot2 = VersionVector(TreeMap(nodeB -> 5L, nodeC -> 2L))
      val thisVvector = VersionVector(TreeMap(nodeA -> 3L, nodeB -> 5L, nodeC -> 2L, nodeD -> 7L))
      val thisSet = new ORSet(elementsMap = Map("K1" -> thisDot1, "K2" -> thisDot2), vvector = thisVvector)
      val thatDot1 = VersionVector(nodeA, 3L)
      val thatDot2 = VersionVector(nodeB, 6L)
      val thatVvector = VersionVector(TreeMap(nodeA -> 3L, nodeB -> 6L, nodeC -> 1L, nodeD -> 8L))
      val thatSet = new ORSet(elementsMap = Map("K1" -> thatDot1, "K2" -> thatDot2), vvector = thatVvector)

      val expectedDots = Map("K1" -> VersionVector(nodeA, 3L), "K2" -> VersionVector(TreeMap(nodeB -> 6L, nodeC -> 2L)))

      ORSet.mergeCommonKeys(commonKeys, thisSet, thatSet) should be(expectedDots)
    }

    "verify mergeDisjointKeys" in {
      val keys: Set[Any] = Set("K3", "K4", "K5")
      val elements: Map[Any, VersionVector] = Map(
        "K3" -> VersionVector(nodeA, 4L),
        "K4" -> VersionVector(TreeMap(nodeA -> 3L, nodeD -> 8L)),
        "K5" -> VersionVector(nodeA, 2L))
      val vvector = VersionVector(TreeMap(nodeA -> 3L, nodeD -> 7L))
      val acc: Map[Any, VersionVector] = Map("K1" -> VersionVector(nodeA, 3L))
      val expectedDots = acc ++ Map("K3" -> VersionVector(nodeA, 4L), "K4" -> VersionVector(nodeD, 8L)) // "a" -> 3 removed, optimized to include only those unseen

      ORSet.mergeDisjointKeys(keys, elements, vvector, acc) should be(expectedDots)
    }

    "verify disjoint merge" in {
      val a1 = ORSet().add(node1, "bar")
      val b1 = ORSet().add(node2, "baz")
      val c = a1.merge(b1)
      val a2 = a1.remove(node1, "bar")
      val d = a2.merge(c)
      d.elements should be(Set("baz"))
    }

    "verify removed after merge" in {
      // Add Z at node1 replica
      val a = ORSet().add(node1, "Z")
      // Replicate it to some node3, i.e. it has dot 'Z'->{node1 -> 1}
      val c = a
      // Remove Z at node1 replica
      val a2 = a.remove(node1, "Z")
      // Add Z at node2, a new replica
      val b = ORSet().add(node2, "Z")
      // Replicate b to node1, so now node1 has a Z, the one with a Dot of
      // {node2 -> 1} and version vector of [{node1 -> 1}, {node2 -> 1}]
      val a3 = b.merge(a2)
      a3.elements should be(Set("Z"))
      // Remove the 'Z' at node2 replica
      val b2 = b.remove(node2, "Z")
      // Both node3 (c) and node1 (a3) have a 'Z', but when they merge, there should be
      // no 'Z' as node3 (c)'s has been removed by node1 and node1 (a3)'s has been removed by
      // node2
      c.elements should be(Set("Z"))
      a3.elements should be(Set("Z"))
      b2.elements should be(Set())

      a3.merge(c).merge(b2).elements should be(Set.empty)
      a3.merge(b2).merge(c).elements should be(Set.empty)
      c.merge(b2).merge(a3).elements should be(Set.empty)
      c.merge(a3).merge(b2).elements should be(Set.empty)
      b2.merge(c).merge(a3).elements should be(Set.empty)
      b2.merge(a3).merge(c).elements should be(Set.empty)
    }

    "verify removed after merge 2" in {
      val a = ORSet().add(node1, "Z")
      val b = ORSet().add(node2, "Z")
      // replicate node3
      val c = a
      val a2 = a.remove(node1, "Z")
      // replicate b to node1, now node1 has node2's 'Z'
      val a3 = a2.merge(b)
      a3.elements should be(Set("Z"))
      // Remove node2's 'Z'
      val b2 = b.remove(node2, "Z")
      // Replicate c to node2, now node2 has node1's old 'Z'
      val b3 = b2.merge(c)
      b3.elements should be(Set("Z"))
      // Merge everytyhing
      a3.merge(c).merge(b3).elements should be(Set.empty)
      a3.merge(b3).merge(c).elements should be(Set.empty)
      c.merge(b3).merge(a3).elements should be(Set.empty)
      c.merge(a3).merge(b3).elements should be(Set.empty)
      b3.merge(c).merge(a3).elements should be(Set.empty)
      b3.merge(a3).merge(c).elements should be(Set.empty)
    }

    "not pollute the vvector of result during mergeRemoveDelta" in {
      val a = ORSet().add(nodeA, "a")
      val a1 = a.add(nodeB, "b").remove(nodeB, "b").resetDelta.remove(nodeA, "a") // a1 now contains dot from nodeB

      a.vvector.contains(nodeA) should be(true)
      a.vvector.contains(nodeB) should be(false)
      a1.vvector.contains(nodeA) should be(true)
      a1.vvector.contains(nodeB) should be(true)

      val a2 = ORSet().mergeDelta(a.delta.get).mergeDelta(a1.delta.get)
      a2.elements should be(Set.empty)
      a2.vvector.contains(nodeB) should be(false) // a2 should not be polluted by the nodeB dot, as all operations on it pertained only to elements from nodeA
    }

    "have unapply extractor" in {
      val s1 = ORSet.empty.add(node1, "a").add(node2, "b")
      val _: ORSet[String] = s1
      val elements1 = s1 match { // `unapply[A](s: ORSet[A])` is used here
        case ORSet(elements1) => elements1
        case _                => fail()
      }
      val elements2: Set[String] = elements1
      elements2 should be(Set("a", "b"))

      Changed(ORSetKey[String]("key"))(s1) match {
        case c @ Changed(ORSetKey("key")) =>
          val _: ORSet[String] = c.dataValue
          val elements3 = c.dataValue match {
            case ORSet(elements3) => elements3
            case _                => fail()
          }
          val elements4: Set[String] = elements3
          elements4 should be(Set("a", "b"))
        case changed =>
          fail(s"Failed to match [$changed]")
      }

      val msg: Any = Changed(ORSetKey[String]("key"))(s1)
      msg match {
        case c @ Changed(ORSetKey("key")) =>
          // FIXME we need to look into this for Scala 2.13.5
          // val ORSet(elements3) = c.dataValue // `unapply(a: ReplicatedData)` is used here
          // if `unapply(a: ReplicatedData)` isn't defined the next line doesn't compile:
          //   type mismatch; found : scala.collection.immutable.Set[A] where type A required: Set[Any] Note: A <: Any,
          //   but trait Set is invariant in type A. You may wish to investigate a wildcard type such as _ <: Any. (SLS 3.2.10)
          val elements3 = c.dataValue match {
            case ORSet(elements3) => elements3
            case _                => fail()
          }
          val elements4: Set[Any] = elements3
          elements4 should be(Set("a", "b"))
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

  }
}
