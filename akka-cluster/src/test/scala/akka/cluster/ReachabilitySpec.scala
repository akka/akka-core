/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address

class ReachabilitySpec extends AnyWordSpec with Matchers {

  import Reachability.{ Reachable, Record, Terminated, Unreachable }

  val nodeA = UniqueAddress(Address("akka", "sys", "a", 2552), 1L)
  val nodeB = UniqueAddress(Address("akka", "sys", "b", 2552), 2L)
  val nodeC = UniqueAddress(Address("akka", "sys", "c", 2552), 3L)
  val nodeD = UniqueAddress(Address("akka", "sys", "d", 2552), 4L)
  val nodeE = UniqueAddress(Address("akka", "sys", "e", 2552), 5L)

  "Reachability table" must {

    "be reachable when empty" in {
      val r = Reachability.empty
      r.isReachable(nodeA) should ===(true)
      r.allUnreachable should ===(Set.empty)
    }

    "be unreachable when one observed unreachable" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA)
      r.isReachable(nodeA) should ===(false)
      r.allUnreachable should ===(Set(nodeA))
    }

    "not be reachable when terminated" in {
      val r = Reachability.empty.terminated(nodeB, nodeA)
      r.isReachable(nodeA) should ===(false)
      // allUnreachable doesn't include terminated
      r.allUnreachable should ===(Set.empty)
      r.allUnreachableOrTerminated should ===(Set(nodeA))
    }

    "not change terminated entry" in {
      val r = Reachability.empty.terminated(nodeB, nodeA)
      (r.reachable(nodeB, nodeA) should be).theSameInstanceAs(r)
      (r.unreachable(nodeB, nodeA) should be).theSameInstanceAs(r)
    }

    "not change when same status" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA)
      (r.unreachable(nodeB, nodeA) should be).theSameInstanceAs(r)
    }

    "be unreachable when some observed unreachable and others reachable" in {
      val r = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeA).reachable(nodeD, nodeA)
      r.isReachable(nodeA) should ===(false)
    }

    "be reachable when all observed reachable again" in {
      val r = Reachability.empty
        .unreachable(nodeB, nodeA)
        .unreachable(nodeC, nodeA)
        .reachable(nodeB, nodeA)
        .reachable(nodeC, nodeA)
        .unreachable(nodeB, nodeC)
        .unreachable(nodeC, nodeB)
      r.isReachable(nodeA) should ===(true)
    }

    "exclude observations from specific (downed) nodes" in {
      val r = Reachability.empty
        .unreachable(nodeC, nodeA)
        .reachable(nodeC, nodeA)
        .unreachable(nodeC, nodeB)
        .unreachable(nodeB, nodeA)
        .unreachable(nodeB, nodeC)

      r.isReachable(nodeA) should ===(false)
      r.isReachable(nodeB) should ===(false)
      r.isReachable(nodeC) should ===(false)
      r.allUnreachableOrTerminated should ===(Set(nodeA, nodeB, nodeC))
      r.removeObservers(Set(nodeB)).allUnreachableOrTerminated should ===(Set(nodeB))
    }

    "be pruned when all records of an observer are Reachable" in {
      val r = Reachability.empty
        .unreachable(nodeB, nodeA)
        .unreachable(nodeB, nodeC)
        .unreachable(nodeD, nodeC)
        .reachable(nodeB, nodeA)
        .reachable(nodeB, nodeC)
      r.isReachable(nodeA) should ===(true)
      r.isReachable(nodeC) should ===(false)
      r.records should ===(Vector(Record(nodeD, nodeC, Unreachable, 1L)))

      val r2 = r.unreachable(nodeB, nodeD).unreachable(nodeB, nodeE)
      r2.records.toSet should ===(
        Set(
          Record(nodeD, nodeC, Unreachable, 1L),
          Record(nodeB, nodeD, Unreachable, 5L),
          Record(nodeB, nodeE, Unreachable, 6L)))
    }

    "have correct aggregated status" in {
      val records = Vector(
        Reachability.Record(nodeA, nodeB, Reachable, 2),
        Reachability.Record(nodeC, nodeB, Unreachable, 2),
        Reachability.Record(nodeA, nodeD, Unreachable, 3),
        Reachability.Record(nodeD, nodeB, Terminated, 4))
      val versions = Map(nodeA -> 3L, nodeC -> 3L, nodeD -> 4L)
      val r = Reachability(records, versions)
      r.status(nodeA) should ===(Reachable)
      r.status(nodeB) should ===(Terminated)
      r.status(nodeD) should ===(Unreachable)
    }

    "have correct status for a mix of nodes" in {
      val r = Reachability.empty
        .unreachable(nodeB, nodeA)
        .unreachable(nodeC, nodeA)
        .unreachable(nodeD, nodeA)
        .unreachable(nodeC, nodeB)
        .reachable(nodeC, nodeB)
        .unreachable(nodeD, nodeB)
        .unreachable(nodeD, nodeC)
        .reachable(nodeD, nodeC)
        .reachable(nodeE, nodeD)
        .unreachable(nodeA, nodeE)
        .terminated(nodeB, nodeE)

      r.status(nodeB, nodeA) should ===(Unreachable)
      r.status(nodeC, nodeA) should ===(Unreachable)
      r.status(nodeD, nodeA) should ===(Unreachable)

      r.status(nodeC, nodeB) should ===(Reachable)
      r.status(nodeD, nodeB) should ===(Unreachable)

      r.status(nodeA, nodeE) should ===(Unreachable)
      r.status(nodeB, nodeE) should ===(Terminated)

      r.isReachable(nodeA) should ===(false)
      r.isReachable(nodeB) should ===(false)
      r.isReachable(nodeC) should ===(true)
      r.isReachable(nodeD) should ===(true)
      r.isReachable(nodeE) should ===(false)

      r.allUnreachable should ===(Set(nodeA, nodeB))
      r.allUnreachableFrom(nodeA) should ===(Set(nodeE))
      r.allUnreachableFrom(nodeB) should ===(Set(nodeA))
      r.allUnreachableFrom(nodeC) should ===(Set(nodeA))
      r.allUnreachableFrom(nodeD) should ===(Set(nodeA, nodeB))

      r.observersGroupedByUnreachable should ===(
        Map(nodeA -> Set(nodeB, nodeC, nodeD), nodeB -> Set(nodeD), nodeE -> Set(nodeA)))
    }

    "merge by picking latest version of each record" in {
      val r1 = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeD)
      val r2 = r1.reachable(nodeB, nodeA).unreachable(nodeD, nodeE).unreachable(nodeC, nodeA)
      val merged = r1.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r2)

      merged.status(nodeB, nodeA) should ===(Reachable)
      merged.status(nodeC, nodeA) should ===(Unreachable)
      merged.status(nodeC, nodeD) should ===(Unreachable)
      merged.status(nodeD, nodeE) should ===(Unreachable)
      merged.status(nodeE, nodeA) should ===(Reachable)

      merged.isReachable(nodeA) should ===(false)
      merged.isReachable(nodeD) should ===(false)
      merged.isReachable(nodeE) should ===(false)

      val merged2 = r2.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r1)
      merged2.records.toSet should ===(merged.records.toSet)
    }

    "merge by taking allowed set into account" in {
      val r1 = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeD)
      val r2 = r1.reachable(nodeB, nodeA).unreachable(nodeD, nodeE).unreachable(nodeC, nodeA)
      // nodeD not in allowed set
      val allowed = Set(nodeA, nodeB, nodeC, nodeE)
      val merged = r1.merge(allowed, r2)

      merged.status(nodeB, nodeA) should ===(Reachable)
      merged.status(nodeC, nodeA) should ===(Unreachable)
      merged.status(nodeC, nodeD) should ===(Reachable)
      merged.status(nodeD, nodeE) should ===(Reachable)
      merged.status(nodeE, nodeA) should ===(Reachable)

      merged.isReachable(nodeA) should ===(false)
      merged.isReachable(nodeD) should ===(true)
      merged.isReachable(nodeE) should ===(true)

      merged.versions.keySet should ===(Set(nodeB, nodeC))

      val merged2 = r2.merge(allowed, r1)
      merged2.records.toSet should ===(merged.records.toSet)
      merged2.versions should ===(merged.versions)
    }

    "merge correctly after pruning" in {
      val r1 = Reachability.empty.unreachable(nodeB, nodeA).unreachable(nodeC, nodeD)
      val r2 = r1.unreachable(nodeA, nodeE)
      val r3 = r1.reachable(nodeB, nodeA) // nodeB pruned
      val merged = r2.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r3)

      merged.records.toSet should ===(Set(Record(nodeA, nodeE, Unreachable, 1), Record(nodeC, nodeD, Unreachable, 1)))

      val merged3 = r3.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r2)
      merged3.records.toSet should ===(merged.records.toSet)
    }

    "merge versions correctly" in {
      val r1 = Reachability(Vector.empty, Map(nodeA -> 3L, nodeB -> 5L, nodeC -> 7L))
      val r2 = Reachability(Vector.empty, Map(nodeA -> 6L, nodeB -> 2L, nodeD -> 1L))
      val merged = r1.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r2)

      val expected = Map(nodeA -> 6L, nodeB -> 5L, nodeC -> 7L, nodeD -> 1L)
      merged.versions should ===(expected)

      val merged2 = r2.merge(Set(nodeA, nodeB, nodeC, nodeD, nodeE), r1)
      merged2.versions should ===(expected)
    }

    "remove node" in {
      val r = Reachability.empty
        .unreachable(nodeB, nodeA)
        .unreachable(nodeC, nodeD)
        .unreachable(nodeB, nodeC)
        .unreachable(nodeB, nodeE)
        .remove(Set(nodeA, nodeB))

      r.status(nodeB, nodeA) should ===(Reachable)
      r.status(nodeC, nodeD) should ===(Unreachable)
      r.status(nodeB, nodeC) should ===(Reachable)
      r.status(nodeB, nodeE) should ===(Reachable)
    }

    "remove correctly after pruning" in {
      val r = Reachability.empty
        .unreachable(nodeB, nodeA)
        .unreachable(nodeB, nodeC)
        .unreachable(nodeD, nodeC)
        .reachable(nodeB, nodeA)
        .reachable(nodeB, nodeC)
      r.records should ===(Vector(Record(nodeD, nodeC, Unreachable, 1L)))
      val r2 = r.remove(List(nodeB))
      r2.allObservers should ===(Set(nodeD))
      r2.versions.keySet should ===(Set(nodeD))
    }

    "be able to filter records" in {
      val r = Reachability.empty.unreachable(nodeC, nodeB).unreachable(nodeB, nodeA).unreachable(nodeB, nodeC)

      val filtered1 = r.filterRecords(record => record.observer != nodeC)
      filtered1.isReachable(nodeB) should ===(true)
      filtered1.isReachable(nodeA) should ===(false)
      filtered1.allObservers should ===(Set(nodeB))
    }

  }
}
