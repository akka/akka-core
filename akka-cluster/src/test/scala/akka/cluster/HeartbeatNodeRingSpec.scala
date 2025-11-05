/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address

class HeartbeatNodeRingSpec extends AnyWordSpec with Matchers {

  val aa = UniqueAddress(Address("akka", "sys", "aa", 2552), 1L)
  val bb = UniqueAddress(Address("akka", "sys", "bb", 2552), 2L)
  val cc = UniqueAddress(Address("akka", "sys", "cc", 2552), 3L)
  val dd = UniqueAddress(Address("akka", "sys", "dd", 2552), 4L)
  val ee = UniqueAddress(Address("akka", "sys", "ee", 2552), 5L)
  val ff = UniqueAddress(Address("akka", "sys", "ff", 2552), 6L)

  val nodes = Set(aa, bb, cc, dd, ee, ff)

  "A HashedNodeRing" must {

    "pick specified number of nodes as receivers" in {
      val ring = HeartbeatNodeRing(cc, nodes, Set.empty, 3)
      ring.myReceivers should ===(ring.receivers(cc))

      nodes.foreach { n =>
        val receivers = ring.receivers(n)
        receivers.size should ===(3)
        receivers should not contain (n)
      }
    }

    "pick specified number of nodes + unreachable as receivers" in {
      val ring = HeartbeatNodeRing(cc, nodes, unreachable = Set(aa, dd, ee), monitoredByNrOfMembers = 3)
      ring.myReceivers should ===(ring.receivers(cc))

      ring.receivers(aa) should ===(Set(bb, cc, dd, ff)) // unreachable ee skipped
      ring.receivers(bb) should ===(Set(cc, dd, ee, ff)) // unreachable aa skipped
      ring.receivers(cc) should ===(Set(dd, ee, ff, bb)) // unreachable aa skipped
      ring.receivers(dd) should ===(Set(ee, ff, aa, bb, cc))
      ring.receivers(ee) should ===(Set(ff, aa, bb, cc))
      ring.receivers(ff) should ===(Set(aa, bb, cc)) // unreachable dd and ee skipped
    }

    "pick all except own as receivers when less than total number of nodes" in {
      val expected = Set(aa, bb, dd, ee, ff)
      HeartbeatNodeRing(cc, nodes, Set.empty, 5).myReceivers should ===(expected)
      HeartbeatNodeRing(cc, nodes, Set.empty, 6).myReceivers should ===(expected)
      HeartbeatNodeRing(cc, nodes, Set.empty, 7).myReceivers should ===(expected)
    }

    "pick none when alone" in {
      val ring = HeartbeatNodeRing(cc, Set(cc), Set.empty, 3)
      ring.myReceivers should ===(Set())
    }

  }
}
