/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable.SortedSet

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.MemberStatus.Up

class MembershipStateSpec extends AnyWordSpec with Matchers {
  // DC-a is in reverse age order
  val a1 = TestMember(Address("akka", "sys", "a4", 2552), Up, 1, "dc-a")
  val a2 = TestMember(Address("akka", "sys", "a3", 2552), Up, 2, "dc-a")
  val a3 = TestMember(Address("akka", "sys", "a2", 2552), Up, 3, "dc-a")
  val a4 = TestMember(Address("akka", "sys", "a1", 2552), Up, 4, "dc-a")

  // DC-b it is the first and the last that are the oldest
  val b1 = TestMember(Address("akka", "sys", "b3", 2552), Up, 1, "dc-b")
  val b3 = TestMember(Address("akka", "sys", "b2", 2552), Up, 3, "dc-b")
  // Won't be replaced by b3
  val b2 = TestMember(Address("akka", "sys", "b1", 2552), Up, 2, "dc-b")
  // for the case that we don't replace it ever
  val bOldest = TestMember(Address("akka", "sys", "b0", 2552), Up, 0, "dc-b")

  "Membership state" must {
    "sort by upNumber for oldest top members" in {
      val gossip = Gossip(SortedSet(a1, a2, a3, a4, b1, b2, b3, bOldest))
      val membershipState = MembershipState(gossip, a1.uniqueAddress, "dc-a", 2)

      membershipState.ageSortedTopOldestMembersPerDc should equal(
        Map("dc-a" -> SortedSet(a1, a2), "dc-b" -> SortedSet(bOldest, b1)))
    }

    "find two oldest as targets for Exiting change" in {
      val a1Exiting = a1.copy(MemberStatus.Leaving).copy(MemberStatus.Exiting)
      val gossip = Gossip(SortedSet(a1Exiting, a2, a3, a4))
      val membershipState = MembershipState(gossip, a1.uniqueAddress, "dc-a", 2)

      membershipState.gossipTargetsForLeavingAndExitingMembers(Set(a1Exiting)) should ===(Set(a1Exiting, a2))
    }

    "find two oldest in DC as targets for Exiting change" in {
      val a4Exiting = a4.copy(MemberStatus.Leaving).copy(MemberStatus.Exiting)
      val gossip = Gossip(SortedSet(a2, a3, a4Exiting, b1, b2))
      val membershipState = MembershipState(gossip, a1.uniqueAddress, "dc-a", 2)

      membershipState.gossipTargetsForLeavingAndExitingMembers(Set(a4Exiting)) should ===(Set(a2, a3))
    }

    "find two oldest per role as targets for Exiting change" in {
      val a5 = TestMember(
        Address("akka", "sys", "a5", 2552),
        MemberStatus.Exiting,
        roles = Set("role1", "role2"),
        upNumber = 5,
        dataCenter = "dc-a")
      val a6 = TestMember(
        Address("akka", "sys", "a6", 2552),
        MemberStatus.Exiting,
        roles = Set("role1", "role3"),
        upNumber = 6,
        dataCenter = "dc-a")
      val a7 = TestMember(
        Address("akka", "sys", "a7", 2552),
        MemberStatus.Exiting,
        roles = Set("role1"),
        upNumber = 7,
        dataCenter = "dc-a")
      val a8 = TestMember(
        Address("akka", "sys", "a8", 2552),
        MemberStatus.Exiting,
        roles = Set("role1"),
        upNumber = 8,
        dataCenter = "dc-a")
      val a9 = TestMember(
        Address("akka", "sys", "a9", 2552),
        MemberStatus.Exiting,
        roles = Set("role2"),
        upNumber = 9,
        dataCenter = "dc-a")
      val b5 = TestMember(
        Address("akka", "sys", "b5", 2552),
        MemberStatus.Exiting,
        roles = Set("role1"),
        upNumber = 5,
        dataCenter = "dc-b")
      val b6 = TestMember(
        Address("akka", "sys", "b6", 2552),
        MemberStatus.Exiting,
        roles = Set("role2"),
        upNumber = 6,
        dataCenter = "dc-b")
      val theExiting = Set(a5, a6)
      val gossip = Gossip(SortedSet(a1, a2, a3, a4, a5, a6, a7, a8, a9, b1, b2, b3, b5, b6))
      val membershipState = MembershipState(gossip, a1.uniqueAddress, "dc-a", 2)

      membershipState.gossipTargetsForLeavingAndExitingMembers(theExiting) should ===(Set(a1, a2, a5, a6, a9))
    }
  }
}
