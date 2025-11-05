/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.ORSet.AddDeltaOp
import akka.cluster.ddata.Replicator.Changed

class ORMapSpec extends AnyWordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)

  "A ORMap" must {

    "be able to add entries" in {
      val m = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B")
      val a = m.entries("a") match {
        case GSet(a) => a
        case _       => fail()
      }
      a should be(Set("A"))
      val b = m.entries("b") match {
        case GSet(b) => b
        case _       => fail()
      }
      b should be(Set("B"))

      val m2 = m.put(node1, "a", GSet() + "C")
      val a2 = m2.entries("a") match {
        case GSet(a2) => a2
        case _        => fail()
      }
      a2 should be(Set("C"))

    }

    "be able to add entries with deltas" in {
      val m = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B")
      val md = m.delta.get

      val m1 = ORMap().mergeDelta(md)

      val a = m1.entries("a") match {
        case GSet(a) => a
        case _       => fail()
      }
      a should be(Set("A"))
      val b = m1.entries("b") match {
        case GSet(b) => b
        case _       => fail()
      }
      b should be(Set("B"))

      val m2 = m1.put(node1, "a", GSet() + "C")
      val a2 = m2.entries("a") match {
        case GSet(a2) => a2
        case _        => fail()
      }
      a2 should be(Set("C"))

    }

    "be able to remove entry" in {
      val m = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B").remove(node1, "a")
      m.entries.keySet should not contain ("a")
      m.entries.keySet should contain("b")
    }

    "be able to remove entry using a delta" in {
      val m = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B")
      val addDelta = m.delta.get

      val removeDelta = m.resetDelta.remove(node1, "a").delta.get

      val m1 = ORMap().mergeDelta(addDelta)
      m1.entries.keySet should contain("a")

      val m2 = m1.mergeDelta(removeDelta)
      m2.entries.keySet should not contain ("a")
      m2.entries.keySet should contain("b")
    }

    "be able to add removed" in {
      val m = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B").remove(node1, "a")
      m.entries.keySet should not contain ("a")
      m.entries.keySet should contain("b")
      val m2 = m.put(node1, "a", GSet() + "C")
      m2.entries.keySet should contain("a")
      m2.entries.keySet should contain("b")
    }

    "be able to have its entries correctly merged with another ORMap with other entries" in {
      val m1 = ORMap().put(node1, "a", GSet() + "A").put(node1, "b", GSet() + "B")
      val m2 = ORMap().put(node2, "c", GSet() + "C")

      // merge both ways
      val merged1 = m1.merge(m2)
      merged1.entries.keySet should contain("a")
      merged1.entries.keySet should contain("b")
      merged1.entries.keySet should contain("c")

      val merged2 = m2.merge(m1)
      merged2.entries.keySet should contain("a")
      merged2.entries.keySet should contain("b")
      merged2.entries.keySet should contain("c")
    }

    "be able to have its entries correctly merged with another ORMap with overlapping entries" in {
      val m1 = ORMap()
        .put(node1, "a", GSet() + "A1")
        .put(node1, "b", GSet() + "B1")
        .remove(node1, "a")
        .put(node1, "d", GSet() + "D1")
      val m2 = ORMap()
        .put(node2, "c", GSet() + "C2")
        .put(node2, "a", GSet() + "A2")
        .put(node2, "b", GSet() + "B2")
        .remove(node2, "b")
        .put(node2, "d", GSet() + "D2")

      // merge both ways
      val merged1 = m1.merge(m2)
      merged1.entries.keySet should contain("a")
      val a1 = merged1.entries("a") match {
        case GSet(a1) => a1
        case _        => fail()
      }
      a1 should be(Set("A2"))
      merged1.entries.keySet should contain("b")
      val b1 = merged1.entries("b") match {
        case GSet(b1) => b1
        case _        => fail()
      }
      b1 should be(Set("B1"))
      merged1.entries.keySet should contain("c")
      merged1.entries.keySet should contain("d")
      val d1 = merged1.entries("d") match {
        case GSet(d1) => d1
        case _        => fail()
      }
      d1 should be(Set("D1", "D2"))

      val merged2 = m2.merge(m1)
      merged2.entries.keySet should contain("a")
      val a2 = merged1.entries("a") match {
        case GSet(a2) => a2
        case _        => fail()
      }
      a2 should be(Set("A2"))
      merged2.entries.keySet should contain("b")
      val b2 = merged2.entries("b") match {
        case GSet(b2) => b2
        case _        => fail()
      }
      b2 should be(Set("B1"))
      merged2.entries.keySet should contain("c")
      merged2.entries.keySet should contain("d")
      val d2 = merged2.entries("d") match {
        case GSet(d2) => d2
        case _        => fail()
      }
      d2 should be(Set("D1", "D2"))
    }

    "illustrate the danger of using remove+put to replace an entry" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node1, "b", GSet.empty + "B")
      val m2 = ORMap.empty.put(node2, "c", GSet.empty + "C")

      val merged1 = m1.merge(m2)

      val m3 = merged1.remove(node1, "b").put(node1, "b", GSet.empty + "B2")
      // same thing if only put is used
      //      val m3 = merged1.put(node1, "b", GSet.empty + "B2")
      val merged2 = merged1.merge(m3)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is included, because GSet("B") is merged with GSet("B2")
      merged2.entries("b").elements should be(Set("B", "B2"))
      merged2.entries("c").elements should be(Set("C"))
    }

    "do not have divergence in dot versions between the underlying map and ormap delta" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A")

      val deltaVersion = m1.delta.get match {
        case ORMap.PutDeltaOp(delta, _, _) =>
          delta match {
            case AddDeltaOp(u: ORSet[Any]) =>
              if (u.elementsMap.contains("a"))
                Some(u.elementsMap("a").versionAt(node1))
              else
                None
            case _ => None
          }
        case _ => None
      }

      val fullVersion =
        if (m1.keys.elementsMap.contains("a"))
          Some(m1.keys.elementsMap("a").versionAt(node1))
        else
          None
      deltaVersion should ===(fullVersion)
    }

    "not have anomalies for remove+updated scenario and deltas" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node1, "b", GSet.empty + "B")
      val m2 = ORMap.empty.put(node2, "c", GSet.empty + "C")

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta.updated(node1, "b", GSet.empty[String])(_.add("B2"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is included, because GSet("B") is merged with GSet("B2")
      merged2.entries("b").elements should be(Set("B", "B2"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is included, because GSet("B") is merged with GSet("B2")
      merged3.entries("b").elements should be(Set("B", "B2"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 2" in {
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B"))
      val m2 = ORMap.empty.put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta.remove(node1, "b").updated(node1, "b", ORSet.empty[String])(_.add(node1, "B2"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged2.entries("b").elements should be(Set("B2"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged3.entries("b").elements should be(Set("B2"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 3" in {
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B"))
      val m2 = ORMap.empty.put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta.remove(node2, "b").updated(node2, "b", ORSet.empty[String])(_.add(node2, "B2"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged2.entries("b").elements should be(Set("B2"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged3.entries("b").elements should be(Set("B2"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 4" in {
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B"))
      val m2 = ORMap.empty.put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta.updated(node1, "b", ORSet.empty[String])(_.add(node1, "B2"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is included, because ORSet("B") is merged with ORSet("B2")
      merged2.entries("b").elements should be(Set("B", "B2"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is included, because ORSet("B") is merged with ORSet("B2")
      merged3.entries("b").elements should be(Set("B", "B2"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 5" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node1, "b", GSet.empty + "B")
      val m2 = ORMap.empty.put(node2, "c", GSet.empty + "C")

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta.put(node2, "b", GSet.empty + "B2")

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged2.entries("b").elements should be(Set("B2"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged3.entries("b").elements should be(Set("B2"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 6" in {
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B"))
      val m2 = ORMap.empty.put(node2, "b", ORSet.empty.add(node2, "B3"))

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b")
      val m4 = merged1.resetDelta
        .remove(node2, "b")
        .updated(node2, "b", ORSet.empty[String])(_.add(node2, "B1"))
        .updated(node2, "b", ORSet.empty[String])(_.add(node2, "B2"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged2.entries("b").elements should be(Set("B1", "B2"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      // note that B is not included, because it was removed in both timelines
      merged3.entries("b").elements should be(Set("B1", "B2"))
    }

    "not have anomalies for remove+updated scenario and deltas 7" in {
      val m1 = ORMap.empty
        .put(node1, "a", ORSet.empty.add(node1, "A"))
        .put(node1, "b", ORSet.empty.add(node1, "B1"))
        .remove(node1, "b")
      val m2 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B2"))
      val m2d = m2.resetDelta.remove(node1, "b")
      val m2u = m2.resetDelta
        .updated(node1, "b", ORSet.empty[String])(_.add(node1, "B3"))
        .updated(node2, "b", ORSet.empty[String])(_.add(node2, "B4"))

      val merged1 = (m1.merge(m2d)).mergeDelta(m2u.delta.get)

      merged1.entries("a").elements should be(Set("A"))
      // note that B1 is lost as it was added and removed earlier in timeline than B2
      merged1.entries("b").elements should be(Set("B2", "B3", "B4"))
    }

    "not have anomalies for remove+updated scenario and deltas 8" in {
      val m1 = ORMap.empty
        .put(node1, "a", GSet.empty + "A")
        .put(node1, "b", GSet.empty + "B")
        .put(node2, "b", GSet.empty + "B")
      val m2 = ORMap.empty.put(node2, "c", GSet.empty + "C")

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b").remove(node2, "b")
      val m4 = merged1.resetDelta.put(node2, "b", GSet.empty + "B2").put(node2, "b", GSet.empty + "B3")

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      merged2.entries("b").elements should be(Set("B3"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = (merged1.mergeDelta(m3.delta.get)).mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B3"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 9" in {
      val m1 = ORMap.empty
        .put(node1, "a", GSet.empty + "A")
        .put(node1, "b", GSet.empty + "B")
        .put(node2, "b", GSet.empty + "B")
      val m2 = ORMap.empty.put(node2, "c", GSet.empty + "C")

      val merged1 = m1.merge(m2)

      val m3 = merged1.resetDelta.remove(node1, "b").remove(node2, "b")
      val m4 = merged1.resetDelta
        .updated(node2, "b", GSet.empty[String])(_.add("B2"))
        .updated(node2, "b", GSet.empty[String])(_.add("B3"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      merged2.entries("b").elements should be(Set("B2", "B3"))
      merged2.entries("c").elements should be(Set("C"))

      val merged3 = (merged1.mergeDelta(m3.delta.get)).mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B2", "B3"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "not have anomalies for remove+updated scenario and deltas 10" in {
      val m1 = ORMap.empty.put(node2, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")

      val m3 = m1.resetDelta.remove(node2, "b")
      val m4 = m3.resetDelta.put(node2, "b", GSet.empty + "B2").updated(node2, "b", GSet.empty[String])(_.add("B3"))

      val merged2 = m3.merge(m4)

      merged2.entries("a").elements should be(Set("A"))
      merged2.entries("b").elements should be(Set("B2", "B3"))

      val merged3 = m3.mergeDelta(m4.delta.get)

      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B2", "B3"))
    }

    "not have anomalies for remove+updated scenario and deltas 11" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A")

      val m2 = ORMap.empty.put(node2, "a", GSet.empty[String]).remove(node2, "a")

      val merged1 = m1.merge(m2)

      merged1.entries("a").elements should be(Set("A"))

      val merged2 = m1.mergeDelta(m2.delta.get)

      merged2.entries("a").elements should be(Set("A"))
    }

    "have the usual anomalies for remove+updated scenario" in {
      // please note that the current ORMultiMap has the same anomaly
      // because the condition of keeping global vvector is violated
      // by removal of the whole entry for the removed key "b" which results in removal of it's value's vvector
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A")).put(node1, "b", ORSet.empty.add(node1, "B"))
      val m2 = ORMap.empty.put(node2, "c", ORSet.empty.add(node2, "C"))

      // m1 - node1 gets the update from m2
      val merged1 = m1.merge(m2)
      // m2 - node2 gets the update from m1
      val merged2 = m2.merge(m1)

      // RACE CONDITION ahead!
      val m3 = merged1.resetDelta.remove(node1, "b")
      // let's imagine that m3 (node1) update gets propagated here (full state or delta - doesn't matter)
      // and is in flight, but in the meantime, an element is being added somewhere else (m4 - node2)
      // and the update is propagated before the update from node1 is merged
      val m4 = merged2.resetDelta.updated(node2, "b", ORSet.empty[String])(_.add(node2, "B2"))
      // and later merged on node1
      val merged3 = m3.merge(m4)
      // and the other way round...
      val merged4 = m4.merge(m3)

      // result -  the element "B" is kept on both sides...
      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B", "B2"))
      merged3.entries("c").elements should be(Set("C"))

      merged4.entries("a").elements should be(Set("A"))
      merged4.entries("b").elements should be(Set("B", "B2"))
      merged4.entries("c").elements should be(Set("C"))

      // but if the timing was slightly different, so that the update from node1
      // would get merged just before update on node2:
      val merged5 = m2.merge(m3).resetDelta.updated(node2, "b", ORSet.empty[String])(_.add(node2, "B2"))
      // the update propagated ... and merged on node1:
      val merged6 = m3.merge(merged5)

      // then the outcome is different... because the vvector of value("b") was lost...
      merged5.entries("a").elements should be(Set("A"))
      // this time it's different...
      merged5.entries("b").elements should be(Set("B2"))
      merged5.entries("c").elements should be(Set("C"))

      merged6.entries("a").elements should be(Set("A"))
      // this time it's different...
      merged6.entries("b").elements should be(Set("B2"))
      merged6.entries("c").elements should be(Set("C"))
    }

    "work with delta-coalescing scenario 1" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")
      val m2 = m1.resetDelta.put(node2, "b", GSet.empty + "B2").updated(node2, "b", GSet.empty[String])(_.add("B3"))

      val merged1 = m1.merge(m2)

      merged1.entries("a").elements should be(Set("A"))
      merged1.entries("b").elements should be(Set("B", "B2", "B3"))

      val merged2 = m1.mergeDelta(m2.delta.get)

      merged2.entries("a").elements should be(Set("A"))
      merged2.entries("b").elements should be(Set("B", "B2", "B3"))

      val m3 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")
      val m4 = m3.resetDelta.put(node2, "b", GSet.empty + "B2").put(node2, "b", GSet.empty + "B3")

      val merged3 = m3.merge(m4)

      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B", "B3"))

      val merged4 = m3.mergeDelta(m4.delta.get)

      merged4.entries("a").elements should be(Set("A"))
      merged4.entries("b").elements should be(Set("B", "B3"))

      val m5 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")
      val m6 = m5.resetDelta
        .put(node2, "b", GSet.empty + "B2")
        .updated(node2, "b", GSet.empty[String])(_.add("B3"))
        .updated(node2, "b", GSet.empty[String])(_.add("B4"))

      val merged5 = m5.merge(m6)

      merged5.entries("a").elements should be(Set("A"))
      merged5.entries("b").elements should be(Set("B", "B2", "B3", "B4"))

      val merged6 = m5.mergeDelta(m6.delta.get)

      merged6.entries("a").elements should be(Set("A"))
      merged6.entries("b").elements should be(Set("B", "B2", "B3", "B4"))

      val m7 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")
      val m8 = m7.resetDelta
        .put(node2, "b", GSet.empty + "B2")
        .put(node2, "d", GSet.empty + "D")
        .put(node2, "b", GSet.empty + "B3")

      val merged7 = m7.merge(m8)

      merged7.entries("a").elements should be(Set("A"))
      merged7.entries("b").elements should be(Set("B", "B3"))
      merged7.entries("d").elements should be(Set("D"))

      val merged8 = m7.mergeDelta(m8.delta.get)

      merged8.entries("a").elements should be(Set("A"))
      merged8.entries("b").elements should be(Set("B", "B3"))
      merged8.entries("d").elements should be(Set("D"))

      val m9 = ORMap.empty.put(node1, "a", GSet.empty + "A").put(node2, "b", GSet.empty + "B")
      val m10 = m9.resetDelta
        .put(node2, "b", GSet.empty + "B2")
        .put(node2, "d", GSet.empty + "D")
        .remove(node2, "d")
        .put(node2, "b", GSet.empty + "B3")

      val merged9 = m9.merge(m10)

      merged9.entries("a").elements should be(Set("A"))
      merged9.entries("b").elements should be(Set("B", "B3"))

      val merged10 = m9.mergeDelta(m10.delta.get)

      merged10.entries("a").elements should be(Set("A"))
      merged10.entries("b").elements should be(Set("B", "B3"))
    }

    "work with deltas and updated for GSet elements type" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A")
      val m2 = m1.resetDelta.updated(node1, "a", GSet.empty[String])(_.add("B"))
      val m3 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get)
      val d3 = m3.entries("a") match {
        case GSet(d3) => d3
        case _        => fail()
      }
      d3 should be(Set("A", "B"))
    }

    "work with deltas and updated for ORSet elements type" in {
      val m1 = ORMap.empty.put(node1, "a", ORSet.empty.add(node1, "A"))
      val m2 = m1.resetDelta.updated(node1, "a", ORSet.empty[String])(_.add(node1, "B"))
      val m3 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get)

      val d3 = m3.entries("a") match {
        case ORSet(d3) => d3
        case _         => fail()
      }
      d3 should be(Set("A", "B"))
    }

    "work with aggregated deltas and updated for GSet elements type" in {
      val m1 = ORMap.empty.put(node1, "a", GSet.empty + "A")
      val m2 = m1.resetDelta
        .updated(node1, "a", GSet.empty[String])(_.add("B"))
        .updated(node1, "a", GSet.empty[String])(_.add("C"))
      val m3 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get)
      val d3 = m3.entries("a") match {
        case GSet(d3) => d3
        case _        => fail()
      }
      d3 should be(Set("A", "B", "C"))
    }

    "work with deltas and updated for GCounter elements type" in {
      val m1 = ORMap.empty.put(node1, "a", GCounter.empty)
      val m2 = m1.resetDelta.updated(node1, "a", GCounter.empty)(_.increment(node1, 10))
      val m3 = m2.resetDelta.updated(node2, "a", GCounter.empty)(_.increment(node2, 10))
      val m4 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get).mergeDelta(m3.delta.get)
      val num = m4.entries("a") match {
        case GCounter(num) => num
        case _             => fail()
      }
      num should ===(20)
    }

    "work with deltas and updated for PNCounter elements type" in {
      val m1 = ORMap.empty.put(node1, "a", PNCounter.empty)
      val m2 = m1.resetDelta.updated(node1, "a", PNCounter.empty)(_.increment(node1, 10))
      val m3 = m2.resetDelta.updated(node2, "a", PNCounter.empty)(_.decrement(node2, 10))
      val m4 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get).mergeDelta(m3.delta.get)
      val num = m4.entries("a") match {
        case PNCounter(num) => num
        case _              => fail()
      }
      num should ===(0)
    }

    "work with deltas and updated for Flag elements type" in {
      val m1 = ORMap.empty.put(node1, "a", Flag(false))
      val m2 = m1.resetDelta.updated(node1, "a", Flag.Disabled)(_.switchOn)
      val m3 = ORMap().mergeDelta(m1.delta.get).mergeDelta(m2.delta.get)
      val d3 = m3.entries("a") match {
        case Flag(d3) => d3
        case _        => fail()
      }
      d3 should be(true)
    }

    "not allow put for ORSet elements type" in {
      val m = ORMap().put(node1, "a", ORSet().add(node1, "A"))

      intercept[IllegalArgumentException] {
        m.put(node1, "a", ORSet().add(node1, "B"))
      }
    }

    "be able to update entry" in {
      val m1 = ORMap
        .empty[String, ORSet[String]]
        .put(node1, "a", ORSet.empty.add(node1, "A"))
        .put(node1, "b", ORSet.empty.add(node1, "B01").add(node1, "B02").add(node1, "B03"))
      val m2 = ORMap.empty[String, ORSet[String]].put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1: ORMap[String, ORSet[String]] = m1.merge(m2)

      val m3 = merged1.updated(node1, "b", ORSet.empty[String])(_.clear().add(node1, "B2"))

      val merged2 = merged1.merge(m3)
      merged2.entries("a").elements should be(Set("A"))
      merged2.entries("b").elements should be(Set("B2"))
      merged2.entries("c").elements should be(Set("C"))

      val m4 = merged1.updated(node2, "b", ORSet.empty[String])(_.add(node2, "B3"))
      val merged3 = m3.merge(m4)
      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B2", "B3"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "be able to update ORSet entry with remove+put" in {
      val m1 = ORMap
        .empty[String, ORSet[String]]
        .put(node1, "a", ORSet.empty.add(node1, "A01"))
        .updated(node1, "a", ORSet.empty[String])(_.add(node1, "A02"))
        .updated(node1, "a", ORSet.empty[String])(_.add(node1, "A03"))
        .put(node1, "b", ORSet.empty.add(node1, "B01").add(node1, "B02").add(node1, "B03"))
      val m2 = ORMap.empty[String, ORSet[String]].put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1 = m1.merge(m2)

      // note that remove + put work because the new VersionVector version is incremented
      // from a global counter
      val m3 = merged1.remove(node1, "b").put(node1, "b", ORSet.empty.add(node1, "B2"))

      val merged2 = merged1.merge(m3)
      merged2.entries("a").elements should be(Set("A01", "A02", "A03"))
      merged2.entries("b").elements should be(Set("B2"))
      merged2.entries("c").elements should be(Set("C"))

      val m4 = merged1.updated(node2, "b", ORSet.empty[String])(_.add(node2, "B3"))
      val merged3 = m3.merge(m4)
      merged3.entries("a").elements should be(Set("A01", "A02", "A03"))
      merged3.entries("b").elements should be(Set("B2", "B3"))
      merged3.entries("c").elements should be(Set("C"))
    }

    "be able to update ORSet entry with remove -> merge -> put" in {
      val m1 = ORMap.empty
        .put(node1, "a", ORSet.empty.add(node1, "A"))
        .put(node1, "b", ORSet.empty.add(node1, "B01").add(node1, "B02").add(node1, "B03"))
      val m2 = ORMap.empty.put(node2, "c", ORSet.empty.add(node2, "C"))

      val merged1 = m1.merge(m2)

      val m3 = merged1.remove(node1, "b")

      val merged2 = merged1.merge(m3)
      merged2.entries("a").elements should be(Set("A"))
      merged2.contains("b") should be(false)
      merged2.entries("c").elements should be(Set("C"))

      val m4 = merged2.put(node1, "b", ORSet.empty.add(node1, "B2"))
      val m5 = merged2
        .updated(node2, "c", ORSet.empty[String])(_.add(node2, "C2"))
        .put(node2, "b", ORSet.empty.add(node2, "B3"))

      val merged3 = m5.merge(m4)
      merged3.entries("a").elements should be(Set("A"))
      merged3.entries("b").elements should be(Set("B2", "B3"))
      merged3.entries("c").elements should be(Set("C", "C2"))
    }

    "have unapply extractor" in {
      val m1 = ORMap.empty.put(node1, "a", Flag(true)).put(node2, "b", Flag(false))
      val _: ORMap[String, Flag] = m1
      val entries1 = m1 match {
        case ORMap(entries1) => entries1
        case _               => fail()
      }
      val entries2: Map[String, Flag] = entries1
      entries2 should be(Map("a" -> Flag(true), "b" -> Flag(false)))

      Changed(ORMapKey[String, Flag]("key"))(m1) match {
        case c @ Changed(ORMapKey("key")) =>
          val entries3 = c.dataValue match {
            case ORMap(entries3) => entries3
            case _               => fail()
          }
          val entries4: Map[String, ReplicatedData] = entries3
          entries4 should be(Map("a" -> Flag(true), "b" -> Flag(false)))
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }

  }
}
