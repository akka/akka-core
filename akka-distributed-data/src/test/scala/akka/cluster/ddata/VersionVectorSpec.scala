/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorSystem
import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.testkit.TestKit

class VersionVectorSpec
    extends TestKit(ActorSystem("VersionVectorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)
  val node4 = UniqueAddress(node1.address.copy(port = Some(2554)), 4L)

  override def afterAll(): Unit = {
    shutdown()
  }

  "A VersionVector" must {

    "have zero versions when created" in {
      val vv = VersionVector()
      vv.size should be(0)
    }

    "not happen before itself" in {
      val vv1 = VersionVector()
      val vv2 = VersionVector()

      vv1 <> vv2 should be(false)
    }

    "increment correctly" in {
      val vv1 = VersionVector()
      val vv2 = vv1 + node1
      vv2.versionAt(node1) should be > vv1.versionAt(node1)
      val vv3 = vv2 + node1
      vv3.versionAt(node1) should be > vv2.versionAt(node1)

      val vv4 = vv3 + node2
      vv4.versionAt(node1) should be(vv3.versionAt(node1))
      vv4.versionAt(node2) should be > vv3.versionAt(node2)
    }

    "pass misc comparison test 1" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2
      val vv4_1 = vv3_1 + node1

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node1
      val vv3_2 = vv2_2 + node2
      val vv4_2 = vv3_2 + node1

      vv4_1 <> vv4_2 should be(false)
    }

    "pass misc comparison test 2" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2
      val vv4_1 = vv3_1 + node1

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node1
      val vv3_2 = vv2_2 + node2
      val vv4_2 = vv3_2 + node1
      val vv5_2 = vv4_2 + node3

      vv4_1 < vv5_2 should be(true)
    }

    "pass misc comparison test 3" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node2

      vv2_1 <> vv2_2 should be(true)
    }

    "pass misc comparison test 4" in {
      val vv1_3 = VersionVector()
      val vv2_3 = vv1_3 + node1
      val vv3_3 = vv2_3 + node2
      val vv4_3 = vv3_3 + node1

      val vv1_4 = VersionVector()
      val vv2_4 = vv1_4 + node1
      val vv3_4 = vv2_4 + node1
      val vv4_4 = vv3_4 + node3

      vv4_3 <> vv4_4 should be(true)
    }

    "pass misc comparison test 5" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node2
      val vv3_1 = vv2_1 + node2

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node1
      val vv3_2 = vv2_2 + node2
      val vv4_2 = vv3_2 + node2
      val vv5_2 = vv4_2 + node3

      vv3_1 < vv5_2 should be(true)
      vv5_2 > vv3_1 should be(true)
    }

    "pass misc comparison test 6" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node1
      val vv3_2 = vv2_2 + node1

      vv3_1 <> vv3_2 should be(true)
      vv3_2 <> vv3_1 should be(true)
    }

    "pass misc comparison test 7" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2
      val vv4_1 = vv3_1 + node2
      val vv5_1 = vv4_1 + node3

      val vv1_2 = vv4_1
      val vv2_2 = vv1_2 + node2
      val vv3_2 = vv2_2 + node2

      vv5_1 <> vv3_2 should be(true)
      vv3_2 <> vv5_1 should be(true)
    }

    "pass misc comparison test 8" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node3

      val vv1_2 = vv3_1 + node2

      val vv4_1 = vv3_1 + node3

      vv4_1 <> vv1_2 should be(true)
      vv1_2 <> vv4_1 should be(true)
    }

    "correctly merge two version vectors" in {
      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2
      val vv4_1 = vv3_1 + node2
      val vv5_1 = vv4_1 + node3

      val vv1_2 = vv4_1
      val vv2_2 = vv1_2 + node2
      val vv3_2 = vv2_2 + node2

      val merged1 = vv3_2.merge(vv5_1)
      merged1.size should be(3)
      merged1.contains(node1) should be(true)
      merged1.contains(node2) should be(true)
      merged1.contains(node3) should be(true)

      val merged2 = vv5_1.merge(vv3_2)
      merged2.size should be(3)
      merged2.contains(node1) should be(true)
      merged2.contains(node2) should be(true)
      merged2.contains(node3) should be(true)

      vv3_2 < merged1 should be(true)
      vv5_1 < merged1 should be(true)

      vv3_2 < merged2 should be(true)
      vv5_1 < merged2 should be(true)

      merged1 == merged2 should be(true)
    }

    "correctly merge two disjoint version vectors" in {

      val vv1_1 = VersionVector()
      val vv2_1 = vv1_1 + node1
      val vv3_1 = vv2_1 + node2
      val vv4_1 = vv3_1 + node2
      val vv5_1 = vv4_1 + node3

      val vv1_2 = VersionVector()
      val vv2_2 = vv1_2 + node4
      val vv3_2 = vv2_2 + node4

      val merged1 = vv3_2.merge(vv5_1)
      merged1.size should be(4)
      merged1.contains(node1) should be(true)
      merged1.contains(node2) should be(true)
      merged1.contains(node3) should be(true)
      merged1.contains(node4) should be(true)

      val merged2 = vv5_1.merge(vv3_2)
      merged2.size should be(4)
      merged2.contains(node1) should be(true)
      merged2.contains(node2) should be(true)
      merged2.contains(node3) should be(true)
      merged2.contains(node4) should be(true)

      vv3_2 < merged1 should be(true)
      vv5_1 < merged1 should be(true)

      vv3_2 < merged2 should be(true)
      vv5_1 < merged2 should be(true)

      merged1 == merged2 should be(true)
    }

    "pass blank version vector incrementing" in {
      val v1 = VersionVector()
      val v2 = VersionVector()

      val vv1 = v1 + node1
      val vv2 = v2 + node2

      (vv1 > v1) should be(true)
      (vv2 > v2) should be(true)

      (vv1 > v2) should be(true)
      (vv2 > v1) should be(true)

      (vv2 > vv1) should be(false)
      (vv1 > vv2) should be(false)
    }

    "pass merging behavior" in {
      val a = VersionVector()
      val b = VersionVector()

      val a1 = a + node1
      val b1 = b + node2

      val a2 = a1 + node1
      val c = a2.merge(b1)
      val c1 = c + node3

      (c1 > a2) should be(true)
      (c1 > b1) should be(true)
    }
  }
}
