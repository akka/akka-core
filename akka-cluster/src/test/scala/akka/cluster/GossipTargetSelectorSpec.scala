/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.MemberStatus.Up

@nowarn("msg=Use Akka Distributed Cluster")
class GossipTargetSelectorSpec extends AnyWordSpec with Matchers {

  val aDc1 = TestMember(Address("akka", "sys", "a", 2552), Up, Set.empty, dataCenter = "dc1")
  val bDc1 = TestMember(Address("akka", "sys", "b", 2552), Up, Set.empty, dataCenter = "dc1")
  val cDc1 = TestMember(Address("akka", "sys", "c", 2552), Up, Set.empty, dataCenter = "dc1")

  val eDc2 = TestMember(Address("akka", "sys", "e", 2552), Up, Set.empty, dataCenter = "dc2")
  val fDc2 = TestMember(Address("akka", "sys", "f", 2552), Up, Set.empty, dataCenter = "dc2")

  val gDc3 = TestMember(Address("akka", "sys", "g", 2552), Up, Set.empty, dataCenter = "dc3")
  val hDc3 = TestMember(Address("akka", "sys", "h", 2552), Up, Set.empty, dataCenter = "dc3")

  val iDc4 = TestMember(Address("akka", "sys", "i", 2552), Up, Set.empty, dataCenter = "dc4")

  val defaultSelector =
    new GossipTargetSelector(reduceGossipDifferentViewProbability = 400, crossDcGossipProbability = 0.2)

  "The gossip target selection" should {

    "select remote nodes in a multi dc setting for a single node cluster regardless of probability" in {
      val realSelector = new GossipTargetSelector(400, 0.0)

      val state = MembershipState(Gossip(SortedSet(iDc4, eDc2, fDc2)), iDc4, iDc4.dataCenter, crossDcConnections = 5)
      val gossipTo = realSelector.gossipTargets(state)

      gossipTo should ===(Vector[UniqueAddress](eDc2, fDc2))
    }

    "select local nodes in a multi dc setting when chance says so" in {
      val alwaysLocalSelector = new GossipTargetSelector(400, 0.2) {
        override protected def selectDcLocalNodes(s: MembershipState): Boolean = true
      }

      val state =
        MembershipState(Gossip(SortedSet(aDc1, bDc1, eDc2, fDc2)), aDc1, aDc1.dataCenter, crossDcConnections = 5)
      val gossipTo = alwaysLocalSelector.gossipTargets(state)

      // only one other local node
      gossipTo should ===(Vector[UniqueAddress](bDc1))
    }

    "select cross dc nodes when chance says so" in {
      val alwaysCrossDcSelector = new GossipTargetSelector(400, 0.2) {
        override protected def selectDcLocalNodes(s: MembershipState): Boolean = false
      }

      val state =
        MembershipState(Gossip(SortedSet(aDc1, bDc1, eDc2, fDc2)), aDc1, aDc1.dataCenter, crossDcConnections = 5)
      val gossipTo = alwaysCrossDcSelector.gossipTargets(state)

      // only one other local node
      gossipTo should (contain(eDc2.uniqueAddress).or(contain(fDc2.uniqueAddress)))
    }

    "select local nodes that hasn't seen the gossip when chance says so" in {
      val alwaysLocalSelector = new GossipTargetSelector(400, 0.2) {
        override protected def preferNodesWithDifferentView(s: MembershipState): Boolean = true
      }

      val state =
        MembershipState(Gossip(SortedSet(aDc1, bDc1, cDc1)).seen(bDc1), aDc1, aDc1.dataCenter, crossDcConnections = 5)
      val gossipTo = alwaysLocalSelector.gossipTargets(state)

      // a1 is self, b1 has seen so only option is c1
      gossipTo should ===(Vector[UniqueAddress](cDc1))
    }

    "select among all local nodes regardless if they saw the gossip already when chance says so" in {
      val alwaysLocalSelector = new GossipTargetSelector(400, 0.2) {
        override protected def preferNodesWithDifferentView(s: MembershipState): Boolean = false
      }

      val state =
        MembershipState(Gossip(SortedSet(aDc1, bDc1, cDc1)).seen(bDc1), aDc1, aDc1.dataCenter, crossDcConnections = 5)
      val gossipTo = alwaysLocalSelector.gossipTargets(state)

      // a1 is self, b1 is the only that has seen
      gossipTo should ===(Vector[UniqueAddress](bDc1, cDc1))
    }

    "not choose unreachable nodes" in {
      val alwaysLocalSelector = new GossipTargetSelector(400, 0.2) {
        override protected def preferNodesWithDifferentView(s: MembershipState): Boolean = false
      }

      val state = MembershipState(
        Gossip(
          members = SortedSet(aDc1, bDc1, cDc1),
          overview = GossipOverview(reachability = Reachability.empty.unreachable(aDc1, bDc1))),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 5)
      val gossipTo = alwaysLocalSelector.gossipTargets(state)

      // a1 cannot reach b1 so only option is c1
      gossipTo should ===(Vector[UniqueAddress](cDc1))
    }

    "select among unreachable nodes if marked as unreachable by someone else" in {
      val alwaysLocalSelector = new GossipTargetSelector(400, 0.2) {
        override protected def preferNodesWithDifferentView(s: MembershipState): Boolean = false
      }

      val state = MembershipState(
        Gossip(
          members = SortedSet(aDc1, bDc1, cDc1),
          overview = GossipOverview(reachability = Reachability.empty.unreachable(aDc1, bDc1).unreachable(bDc1, cDc1))),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 5)
      val gossipTo = alwaysLocalSelector.gossipTargets(state)

      // a1 marked b as unreachable so will not pick b
      // b marked c as unreachable so that is ok as target
      gossipTo should ===(Vector[UniqueAddress](cDc1))
    }

    "continue with the next dc when doing cross dc and no node where suitable" in {
      val selector = new GossipTargetSelector(400, 0.2) {
        override protected def selectDcLocalNodes(s: MembershipState): Boolean = false
        override protected def dcsInRandomOrder(dcs: List[DataCenter]): List[DataCenter] = dcs.sorted // sort on name
      }

      val state = MembershipState(
        Gossip(
          members = SortedSet(aDc1, bDc1, eDc2, fDc2, gDc3, hDc3),
          overview = GossipOverview(reachability = Reachability.empty.unreachable(aDc1, eDc2).unreachable(aDc1, fDc2))),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 5)
      val gossipTo = selector.gossipTargets(state)
      gossipTo should ===(Vector[UniqueAddress](gDc3, hDc3))
    }

    "not care about seen/unseen for cross dc" in {
      val selector = new GossipTargetSelector(400, 0.2) {
        override protected def selectDcLocalNodes(s: MembershipState): Boolean = false
        override protected def dcsInRandomOrder(dcs: List[DataCenter]): List[DataCenter] = dcs.sorted // sort on name
      }

      val state = MembershipState(
        Gossip(members = SortedSet(aDc1, bDc1, eDc2, fDc2, gDc3, hDc3)).seen(fDc2).seen(hDc3),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 5)
      val gossipTo = selector.gossipTargets(state)
      gossipTo should ===(Vector[UniqueAddress](eDc2, fDc2))
    }

    "limit the numbers of chosen cross dc nodes to the crossDcConnections setting" in {
      val selector = new GossipTargetSelector(400, 0.2) {
        override protected def selectDcLocalNodes(s: MembershipState): Boolean = false
        override protected def dcsInRandomOrder(dcs: List[DataCenter]): List[DataCenter] = dcs.sorted // sort on name
      }

      val state = MembershipState(
        Gossip(members = SortedSet(aDc1, bDc1, eDc2, fDc2, gDc3, hDc3)),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 1)
      val gossipTo = selector.gossipTargets(state)
      gossipTo should ===(Vector[UniqueAddress](eDc2))
    }

    "select N random local nodes when single dc" in {
      val state = MembershipState(
        Gossip(members = SortedSet(aDc1, bDc1, cDc1)),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 1) // means only a e and g are oldest

      val randomNodes = defaultSelector.randomNodesForFullGossip(state, 3)

      randomNodes.toSet should ===(Set[UniqueAddress](bDc1, cDc1))
    }

    "select N random local nodes when not self among oldest" in {
      val state = MembershipState(
        Gossip(members = SortedSet(aDc1, bDc1, cDc1, eDc2, fDc2, gDc3, hDc3)),
        bDc1,
        bDc1.dataCenter,
        crossDcConnections = 1) // means only a, e and g are oldest

      val randomNodes = defaultSelector.randomNodesForFullGossip(state, 3)

      randomNodes.toSet should ===(Set[UniqueAddress](aDc1, cDc1))
    }

    "select N-1 random local nodes plus one cross dc oldest node when self among oldest" in {
      val state = MembershipState(
        Gossip(members = SortedSet(aDc1, bDc1, cDc1, eDc2, fDc2)),
        aDc1,
        aDc1.dataCenter,
        crossDcConnections = 1) // means only a and e are oldest

      val randomNodes = defaultSelector.randomNodesForFullGossip(state, 3)

      randomNodes.toSet should ===(Set[UniqueAddress](bDc1, cDc1, eDc2))
    }

  }

  // made the test so much easier to read
  import scala.language.implicitConversions
  private implicit def memberToUniqueAddress(m: Member): UniqueAddress = m.uniqueAddress
}
