/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable.TreeMap

import akka.testkit.AkkaSpec

class VectorClockSpec extends AkkaSpec {
  import VectorClock._

  "A VectorClock" must {

    "have zero versions when created" in {
      val clock = VectorClock()
      clock.versions should ===(TreeMap.empty[VectorClock.Node, Long])
    }

    "not happen before itself" in {
      val clock1 = VectorClock()
      val clock2 = VectorClock()

      clock1 <> clock2 should ===(false)
    }

    "pass misc comparison test 1" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("1")
      val clock3_1 = clock2_1 :+ Node("2")
      val clock4_1 = clock3_1 :+ Node("1")

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ Node("1")
      val clock3_2 = clock2_2 :+ Node("2")
      val clock4_2 = clock3_2 :+ Node("1")

      clock4_1 <> clock4_2 should ===(false)
    }

    "pass misc comparison test 2" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("1")
      val clock3_1 = clock2_1 :+ Node("2")
      val clock4_1 = clock3_1 :+ Node("1")

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ Node("1")
      val clock3_2 = clock2_2 :+ Node("2")
      val clock4_2 = clock3_2 :+ Node("1")
      val clock5_2 = clock4_2 :+ Node("3")

      clock4_1 < clock5_2 should ===(true)
    }

    "pass misc comparison test 3" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("1")

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ Node("2")

      clock2_1 <> clock2_2 should ===(true)
    }

    "pass misc comparison test 4" in {
      val clock1_3 = VectorClock()
      val clock2_3 = clock1_3 :+ Node("1")
      val clock3_3 = clock2_3 :+ Node("2")
      val clock4_3 = clock3_3 :+ Node("1")

      val clock1_4 = VectorClock()
      val clock2_4 = clock1_4 :+ Node("1")
      val clock3_4 = clock2_4 :+ Node("1")
      val clock4_4 = clock3_4 :+ Node("3")

      clock4_3 <> clock4_4 should ===(true)
    }

    "pass misc comparison test 5" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("2")
      val clock3_1 = clock2_1 :+ Node("2")

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ Node("1")
      val clock3_2 = clock2_2 :+ Node("2")
      val clock4_2 = clock3_2 :+ Node("2")
      val clock5_2 = clock4_2 :+ Node("3")

      clock3_1 < clock5_2 should ===(true)
      clock5_2 > clock3_1 should ===(true)
    }

    "pass misc comparison test 6" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("1")
      val clock3_1 = clock2_1 :+ Node("2")

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ Node("1")
      val clock3_2 = clock2_2 :+ Node("1")

      clock3_1 <> clock3_2 should ===(true)
      clock3_2 <> clock3_1 should ===(true)
    }

    "pass misc comparison test 7" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node("1")
      val clock3_1 = clock2_1 :+ Node("2")
      val clock4_1 = clock3_1 :+ Node("2")
      val clock5_1 = clock4_1 :+ Node("3")

      val clock1_2 = clock4_1
      val clock2_2 = clock1_2 :+ Node("2")
      val clock3_2 = clock2_2 :+ Node("2")

      clock5_1 <> clock3_2 should ===(true)
      clock3_2 <> clock5_1 should ===(true)
    }

    "pass misc comparison test 8" in {
      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ Node.fromHash("1")
      val clock3_1 = clock2_1 :+ Node.fromHash("3")

      val clock1_2 = clock3_1 :+ Node.fromHash("2")

      val clock4_1 = clock3_1 :+ Node.fromHash("3")

      clock4_1 <> clock1_2 should ===(true)
      clock1_2 <> clock4_1 should ===(true)
    }

    "correctly merge two clocks" in {
      val node1 = Node("1")
      val node2 = Node("2")
      val node3 = Node("3")

      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ node1
      val clock3_1 = clock2_1 :+ node2
      val clock4_1 = clock3_1 :+ node2
      val clock5_1 = clock4_1 :+ node3

      val clock1_2 = clock4_1
      val clock2_2 = clock1_2 :+ node2
      val clock3_2 = clock2_2 :+ node2

      val merged1 = clock3_2.merge(clock5_1)
      merged1.versions.size should ===(3)
      merged1.versions.contains(node1) should ===(true)
      merged1.versions.contains(node2) should ===(true)
      merged1.versions.contains(node3) should ===(true)

      val merged2 = clock5_1.merge(clock3_2)
      merged2.versions.size should ===(3)
      merged2.versions.contains(node1) should ===(true)
      merged2.versions.contains(node2) should ===(true)
      merged2.versions.contains(node3) should ===(true)

      clock3_2 < merged1 should ===(true)
      clock5_1 < merged1 should ===(true)

      clock3_2 < merged2 should ===(true)
      clock5_1 < merged2 should ===(true)

      merged1 == merged2 should ===(true)
    }

    "correctly merge two disjoint vector clocks" in {
      val node1 = Node("1")
      val node2 = Node("2")
      val node3 = Node("3")
      val node4 = Node("4")

      val clock1_1 = VectorClock()
      val clock2_1 = clock1_1 :+ node1
      val clock3_1 = clock2_1 :+ node2
      val clock4_1 = clock3_1 :+ node2
      val clock5_1 = clock4_1 :+ node3

      val clock1_2 = VectorClock()
      val clock2_2 = clock1_2 :+ node4
      val clock3_2 = clock2_2 :+ node4

      val merged1 = clock3_2.merge(clock5_1)
      merged1.versions.size should ===(4)
      merged1.versions.contains(node1) should ===(true)
      merged1.versions.contains(node2) should ===(true)
      merged1.versions.contains(node3) should ===(true)
      merged1.versions.contains(node4) should ===(true)

      val merged2 = clock5_1.merge(clock3_2)
      merged2.versions.size should ===(4)
      merged2.versions.contains(node1) should ===(true)
      merged2.versions.contains(node2) should ===(true)
      merged2.versions.contains(node3) should ===(true)
      merged2.versions.contains(node4) should ===(true)

      clock3_2 < merged1 should ===(true)
      clock5_1 < merged1 should ===(true)

      clock3_2 < merged2 should ===(true)
      clock5_1 < merged2 should ===(true)

      merged1 == merged2 should ===(true)
    }

    "pass blank clock incrementing" in {
      val node1 = Node("1")
      val node2 = Node("2")

      val v1 = VectorClock()
      val v2 = VectorClock()

      val vv1 = v1 :+ node1
      val vv2 = v2 :+ node2

      (vv1 > v1) should ===(true)
      (vv2 > v2) should ===(true)

      (vv1 > v2) should ===(true)
      (vv2 > v1) should ===(true)

      (vv2 > vv1) should ===(false)
      (vv1 > vv2) should ===(false)
    }

    "pass merging behavior" in {
      val node1 = Node("1")
      val node2 = Node("2")
      val node3 = Node("3")

      val a = VectorClock()
      val b = VectorClock()

      val a1 = a :+ node1
      val b1 = b :+ node2

      val a2 = a1 :+ node1
      val c = a2.merge(b1)
      val c1 = c :+ node3

      (c1 > a2) should ===(true)
      (c1 > b1) should ===(true)
    }

    "support pruning" in {
      val node1 = Node("1")
      val node2 = Node("2")
      val node3 = Node("3")

      val a = VectorClock()
      val b = VectorClock()

      val a1 = a :+ node1
      val b1 = b :+ node2

      val c = a1.merge(b1)
      val c1 = c.prune(node1) :+ node3
      c1.versions.contains(node1) should be(false)
      (c1 <> c) should be(true)

      c.prune(node1).merge(c1).versions.contains(node1) should be(false)

      val c2 = c :+ node2
      (c1 <> c2) should be(true)
    }
  }
}
