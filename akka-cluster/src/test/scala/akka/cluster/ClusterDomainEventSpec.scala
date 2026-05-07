/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address

@nowarn("msg=Use Akka Distributed Cluster")
class ClusterDomainEventSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  import ClusterEvent._
  import MemberStatus._

  val aRoles = Set("AA", "AB")
  val aJoining = TestMember(Address("akka", "sys", "a", 2552), Joining, aRoles)
  val aUp = TestMember(Address("akka", "sys", "a", 2552), Up, aRoles)
  val aRemoved = TestMember(Address("akka", "sys", "a", 2552), Removed, aRoles)
  val bRoles = Set("AB", "BB")
  val bUp = TestMember(Address("akka", "sys", "b", 2552), Up, bRoles)
  val bDown = TestMember(Address("akka", "sys", "b", 2552), Down, bRoles)
  val bRemoved = TestMember(Address("akka", "sys", "b", 2552), Removed, bRoles)
  val cRoles = Set.empty[String]
  val cUp = TestMember(Address("akka", "sys", "c", 2552), Up, cRoles)
  val cLeaving = TestMember(Address("akka", "sys", "c", 2552), Leaving, cRoles)
  val dRoles = Set("DD", "DE")
  val dLeaving = TestMember(Address("akka", "sys", "d", 2552), Leaving, dRoles)
  val dExiting = TestMember(Address("akka", "sys", "d", 2552), Exiting, dRoles)
  val dRemoved = TestMember(Address("akka", "sys", "d", 2552), Removed, dRoles)
  val eRoles = Set("EE", "DE")
  val eJoining = TestMember(Address("akka", "sys", "e", 2552), Joining, eRoles)
  val eUp = TestMember(Address("akka", "sys", "e", 2552), Up, eRoles)
  val eDown = TestMember(Address("akka", "sys", "e", 2552), Down, eRoles)
  val selfDummyAddress = UniqueAddress(Address("akka", "sys", "selfDummy", 2552), 17L)

  private val originalClusterAssert = sys.props.get("akka.cluster.assert").getOrElse("false")
  override protected def beforeAll(): Unit = {
    System.setProperty("akka.cluster.assert", "on")
  }

  override protected def afterAll(): Unit = {
    System.setProperty("akka.cluster.assert", originalClusterAssert)
  }

  private[cluster] def converge(gossip: Gossip): (Gossip, Set[UniqueAddress]) =
    gossip.members.foldLeft((gossip, Set.empty[UniqueAddress])) {
      case ((gs, as), m) => (gs.seen(m.uniqueAddress), as + m.uniqueAddress)
    }

  private def state(g: Gossip): MembershipState =
    state(g, selfDummyAddress)

  private def state(g: Gossip, self: UniqueAddress): MembershipState =
    MembershipState(g, self, ClusterSettings.DefaultDataCenter, crossDcConnections = 5)

  "Domain events" must {

    "be empty for the same gossip" in {
      val g1 = Gossip(members = SortedSet(aUp))

      diffUnreachable(state(g1), state(g1)) should ===(Seq.empty)
    }

    "be produced for new members" in {
      val (g1, _) = converge(Gossip(members = SortedSet(aUp)))
      val (g2, s2) = converge(Gossip(members = SortedSet(aUp, bUp, eJoining)))

      diffMemberEvents(state(g1), state(g2)) should ===(Seq(MemberUp(bUp), MemberJoined(eJoining)))
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
      diffSeen(state(g1), state(g2)) should ===(Seq(SeenChanged(convergence = true, seenBy = s2.map(_.address))))
    }

    "be produced for changed status of members" in {
      val (g1, _) = converge(Gossip(members = SortedSet(aJoining, bUp, cUp)))
      val (g2, s2) = converge(Gossip(members = SortedSet(aUp, bUp, cLeaving, eJoining)))

      diffMemberEvents(state(g1), state(g2)) should ===(
        Seq(MemberUp(aUp), MemberLeft(cLeaving), MemberJoined(eJoining)))
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
      diffSeen(state(g1), state(g2)) should ===(Seq(SeenChanged(convergence = true, seenBy = s2.map(_.address))))
    }

    "be produced for members in unreachable" in {
      val reachability1 = Reachability.empty
        .unreachable(aUp.uniqueAddress, cUp.uniqueAddress)
        .unreachable(aUp.uniqueAddress, eUp.uniqueAddress)
      val g1 = Gossip(members = SortedSet(aUp, bUp, cUp, eUp), overview = GossipOverview(reachability = reachability1))
      val reachability2 = reachability1.unreachable(aUp.uniqueAddress, bDown.uniqueAddress)
      val g2 =
        Gossip(members = SortedSet(aUp, cUp, bDown, eDown), overview = GossipOverview(reachability = reachability2))

      diffUnreachable(state(g1), state(g2)) should ===(Seq(UnreachableMember(bDown)))
      // never include self member in unreachable

      diffUnreachable(state(g1, bDown.uniqueAddress), state(g2, bDown.uniqueAddress)) should ===(Seq())
      diffSeen(state(g1), state(g2)) should ===(Seq.empty)
    }

    "be produced for reachability observations between data centers" in {
      val dc2AMemberUp = TestMember(Address("akka", "sys", "dc2A", 2552), Up, Set.empty[String], "dc2")
      val dc2AMemberDown = TestMember(Address("akka", "sys", "dc2A", 2552), Down, Set.empty[String], "dc2")
      val dc2BMemberUp = TestMember(Address("akka", "sys", "dc2B", 2552), Up, Set.empty[String], "dc2")

      val dc3AMemberUp = TestMember(Address("akka", "sys", "dc3A", 2552), Up, Set.empty[String], "dc3")
      val dc3BMemberUp = TestMember(Address("akka", "sys", "dc3B", 2552), Up, Set.empty[String], "dc3")

      val reachability1 = Reachability.empty
      val g1 = Gossip(
        members = SortedSet(aUp, bUp, dc2AMemberUp, dc2BMemberUp, dc3AMemberUp, dc3BMemberUp),
        overview = GossipOverview(reachability = reachability1))

      val reachability2 = reachability1
        .unreachable(aUp.uniqueAddress, dc2AMemberDown.uniqueAddress)
        .unreachable(dc2BMemberUp.uniqueAddress, dc2AMemberDown.uniqueAddress)
      val g2 = Gossip(
        members = SortedSet(aUp, bUp, dc2AMemberDown, dc2BMemberUp, dc3AMemberUp, dc3BMemberUp),
        overview = GossipOverview(reachability = reachability2))

      Set(aUp, bUp, dc2AMemberUp, dc2BMemberUp, dc3AMemberUp, dc3BMemberUp).foreach { member =>
        val otherDc =
          if (member.dataCenter == ClusterSettings.DefaultDataCenter) Seq("dc2")
          else Seq()

        diffUnreachableDataCenter(
          MembershipState(g1, member.uniqueAddress, member.dataCenter, crossDcConnections = 5),
          MembershipState(g2, member.uniqueAddress, member.dataCenter, crossDcConnections = 5)) should ===(
          otherDc.map(UnreachableDataCenter.apply))

        diffReachableDataCenter(
          MembershipState(g2, member.uniqueAddress, member.dataCenter, crossDcConnections = 5),
          MembershipState(g1, member.uniqueAddress, member.dataCenter, crossDcConnections = 5)) should ===(
          otherDc.map(ReachableDataCenter.apply))
      }
    }

    "not be produced for same reachability observations between data centers" in {
      val dc2AMemberUp = TestMember(Address("akka", "sys", "dc2A", 2552), Up, Set.empty[String], "dc2")
      val dc2AMemberDown = TestMember(Address("akka", "sys", "dc2A", 2552), Down, Set.empty[String], "dc2")

      val reachability1 = Reachability.empty
      val g1 = Gossip(members = SortedSet(aUp, dc2AMemberUp), overview = GossipOverview(reachability = reachability1))

      val reachability2 = reachability1.unreachable(aUp.uniqueAddress, dc2AMemberDown.uniqueAddress)
      val g2 = Gossip(members = SortedSet(aUp, dc2AMemberDown), overview = GossipOverview(reachability = reachability2))

      diffUnreachableDataCenter(
        MembershipState(g1, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5),
        MembershipState(g1, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5)) should ===(Seq())

      diffUnreachableDataCenter(
        MembershipState(g2, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5),
        MembershipState(g2, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5)) should ===(Seq())

      diffReachableDataCenter(
        MembershipState(g1, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5),
        MembershipState(g1, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5)) should ===(Seq())

      diffReachableDataCenter(
        MembershipState(g2, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5),
        MembershipState(g2, aUp.uniqueAddress, aUp.dataCenter, crossDcConnections = 5)) should ===(Seq())
    }

    "be produced correctly for scenario in issue #24955" in {

      // The scenario as seen from dc2MemberC was a sequence of reachability changes
      // - empty
      // - C --unreachable--> A
      // - C --unreachable--> B
      // - empty
      // - B --unreachable--> C

      val dc1MemberA = TestMember(Address("akka", "sys", "dc2A", 2552), Up, Set.empty[String], "dc2")
      val dc1MemberB = TestMember(Address("akka", "sys", "dc2B", 2552), Up, Set.empty[String], "dc2")
      val dc2MemberC = TestMember(Address("akka", "sys", "dc3A", 2552), Up, Set.empty[String], "dc3")

      val members = SortedSet(dc1MemberA, dc1MemberB, dc2MemberC)

      val reachability1 = Reachability.empty
      val g1 = Gossip(members, overview = GossipOverview(reachability = reachability1))

      // - C --unreachable--> A
      // cross unreachable => UnreachableDataCenter
      val reachability2 = reachability1.unreachable(dc2MemberC.uniqueAddress, dc1MemberA.uniqueAddress)
      val g2 = Gossip(members, overview = GossipOverview(reachability = reachability2))
      diffUnreachableDataCenter(
        MembershipState(g1, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g2, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(
        Seq(UnreachableDataCenter(dc1MemberA.dataCenter)))
      diffReachableDataCenter(
        MembershipState(g1, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g2, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())

      // - C --unreachable--> B
      // adding one more cross unreachable to same DC shouldn't publish anything new
      // this was the problem in issue #24955, it published another UnreachableDataCenter
      val reachability3 = reachability2.unreachable(dc2MemberC.uniqueAddress, dc1MemberB.uniqueAddress)
      val g3 = Gossip(members, overview = GossipOverview(reachability = reachability3))
      diffUnreachableDataCenter(
        MembershipState(g2, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g3, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())
      diffReachableDataCenter(
        MembershipState(g2, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g3, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())

      // - empty
      // reachable again => ReachableDataCenter
      val reachability4 = Reachability.empty
      val g4 = Gossip(members, overview = GossipOverview(reachability = reachability4))
      diffUnreachableDataCenter(
        MembershipState(g3, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g4, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())
      diffReachableDataCenter(
        MembershipState(g3, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g4, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(
        Seq(ReachableDataCenter(dc1MemberA.dataCenter)))

      // - B --unreachable--> C
      // unreachable opposite direction shouldn't publish anything new
      val reachability5 = reachability4.unreachable(dc1MemberB.uniqueAddress, dc2MemberC.uniqueAddress)
      val g5 = Gossip(members, overview = GossipOverview(reachability = reachability5))
      diffUnreachableDataCenter(
        MembershipState(g4, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g5, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())
      diffReachableDataCenter(
        MembershipState(g4, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5),
        MembershipState(g5, dc2MemberC.uniqueAddress, dc2MemberC.dataCenter, crossDcConnections = 5)) should ===(Seq())

    }

    "be produced for members becoming reachable after unreachable" in {
      val reachability1 = Reachability.empty
        .unreachable(aUp.uniqueAddress, cUp.uniqueAddress)
        .reachable(aUp.uniqueAddress, cUp.uniqueAddress)
        .unreachable(aUp.uniqueAddress, eUp.uniqueAddress)
        .unreachable(aUp.uniqueAddress, bUp.uniqueAddress)
      val g1 = Gossip(members = SortedSet(aUp, bUp, cUp, eUp), overview = GossipOverview(reachability = reachability1))
      val reachability2 =
        reachability1.unreachable(aUp.uniqueAddress, cUp.uniqueAddress).reachable(aUp.uniqueAddress, bUp.uniqueAddress)
      val g2 = Gossip(members = SortedSet(aUp, cUp, bUp, eUp), overview = GossipOverview(reachability = reachability2))

      diffUnreachable(state(g1), state(g2)) should ===(Seq(UnreachableMember(cUp)))
      // never include self member in unreachable
      diffUnreachable(state(g1, cUp.uniqueAddress), state(g2, cUp.uniqueAddress)) should ===(Seq())
      diffReachable(state(g1), state(g2)) should ===(Seq(ReachableMember(bUp)))
      // never include self member in reachable
      diffReachable(state(g1, bUp.uniqueAddress), state(g2, bUp.uniqueAddress)) should ===(Seq())
    }

    "be produced for downed members" in {
      val (g1, _) = converge(Gossip(members = SortedSet(aUp, eUp)))
      val (g2, _) = converge(Gossip(members = SortedSet(aUp, eDown)))

      diffMemberEvents(state(g1), state(g2)) should ===(Seq(MemberDowned(eDown)))
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
    }

    "be produced for removed members" in {
      val (g1, _) = converge(Gossip(members = SortedSet(aUp, dExiting)))
      val (g2, s2) = converge(Gossip(members = SortedSet(aUp)))

      diffMemberEvents(state(g1), state(g2)) should ===(Seq(MemberRemoved(dRemoved, Exiting)))
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
      diffSeen(state(g1), state(g2)) should ===(Seq(SeenChanged(convergence = true, seenBy = s2.map(_.address))))
    }

    "be produced for removed and rejoined member in another data center" in {
      val bUpDc2 = TestMember(bUp.address, Up, bRoles, dataCenter = "dc2")
      val bUpDc2Removed = TestMember(bUpDc2.address, Removed, bRoles, dataCenter = "dc2")
      val bUpDc2Restarted =
        TestMember.withUniqueAddress(UniqueAddress(bUpDc2.address, 2L), Up, bRoles, dataCenter = "dc2")
      val g1 = Gossip(members = SortedSet(aUp, bUpDc2))
      val g2 = g1
        .remove(bUpDc2.uniqueAddress, System.currentTimeMillis()) // adds tombstone
        .copy(members = SortedSet(aUp, bUpDc2Restarted))
        .merge(g1)

      diffMemberEvents(state(g1), state(g2)) should ===(
        Seq(MemberRemoved(bUpDc2Removed, Up), MemberUp(bUpDc2Restarted)))
    }

    "be produced for convergence changes" in {
      val g1 = Gossip(members = SortedSet(aUp, bUp, eJoining))
        .seen(aUp.uniqueAddress)
        .seen(bUp.uniqueAddress)
        .seen(eJoining.uniqueAddress)
      val g2 = Gossip(members = SortedSet(aUp, bUp, eJoining)).seen(aUp.uniqueAddress).seen(bUp.uniqueAddress)

      diffMemberEvents(state(g1), state(g2)) should ===(Seq.empty)
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
      diffSeen(state(g1), state(g2)) should ===(
        Seq(SeenChanged(convergence = true, seenBy = Set(aUp.address, bUp.address))))
      diffMemberEvents(state(g2), state(g1)) should ===(Seq.empty)
      diffUnreachable(state(g2), state(g1)) should ===(Seq.empty)
      diffSeen(state(g2), state(g1)) should ===(
        Seq(SeenChanged(convergence = true, seenBy = Set(aUp.address, bUp.address, eJoining.address))))
    }

    "be produced for leader changes" in {
      val (g1, _) = converge(Gossip(members = SortedSet(aUp, bUp, eJoining)))
      val (g2, s2) = converge(Gossip(members = SortedSet(bUp, eJoining)))

      diffMemberEvents(state(g1), state(g2)) should ===(Seq(MemberRemoved(aRemoved, Up)))
      diffUnreachable(state(g1), state(g2)) should ===(Seq.empty)
      diffSeen(state(g1), state(g2)) should ===(Seq(SeenChanged(convergence = true, seenBy = s2.map(_.address))))
      diffLeader(state(g1), state(g2)) should ===(Seq(LeaderChanged(Some(bUp.address))))
    }

    "be produced for tombstone changes" in {
      val s1 = state(Gossip(members = SortedSet(aUp)))
      val s2 = state(
        Gossip(members = SortedSet(aUp)).copy(tombstones = Map(eDown.uniqueAddress -> System.currentTimeMillis())))

      diffTombstones(s1, s2) should ===(Seq(MemberTombstonesChanged(Set(eDown.uniqueAddress))))
    }

    "be produced for removed and rejoined members" in {
      val up = TestMember(bUp.address, Up, bRoles)
      val removed = TestMember(up.address, Removed, bRoles)
      val restarted =
        TestMember.withUniqueAddress(UniqueAddress(up.address, 2L), Up, bRoles, ClusterSettings.DefaultDataCenter)
      val g1 = Gossip(members = SortedSet(aUp, up))
      val g2 = g1
        .remove(up.uniqueAddress, System.currentTimeMillis()) // adds tombstone
        .copy(members = SortedSet(aUp, restarted))
        .merge(g1)

      diffMemberEvents(state(g1), state(g2)) should ===(Seq(MemberRemoved(removed, Up), MemberUp(restarted)))
    }

    "be produced for role leader changes in the same data center" in {
      val g0 = Gossip.empty
      val g1 = Gossip(members = SortedSet(aUp, bUp, cUp, dLeaving, eJoining))
      val g2 = Gossip(members = SortedSet(bUp, cUp, dExiting, eJoining))
      diffRolesLeader(state(g0), state(g1)) should ===(
        Set(
          // since this role is implicitly added
          RoleLeaderChanged(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter, Some(aUp.address)),
          RoleLeaderChanged("AA", Some(aUp.address)),
          RoleLeaderChanged("AB", Some(aUp.address)),
          RoleLeaderChanged("BB", Some(bUp.address)),
          RoleLeaderChanged("DD", Some(dLeaving.address)),
          RoleLeaderChanged("DE", Some(dLeaving.address)),
          RoleLeaderChanged("EE", Some(eUp.address))))
      diffRolesLeader(state(g1), state(g2)) should ===(
        Set(
          RoleLeaderChanged(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter, Some(bUp.address)),
          RoleLeaderChanged("AA", None),
          RoleLeaderChanged("AB", Some(bUp.address)),
          RoleLeaderChanged("DE", Some(eJoining.address))))
    }

    "not be produced for role leader changes in other data centers" in {
      val g0 = Gossip.empty
      val s0 = state(g0).copy(selfDc = "dc2")
      val g1 = Gossip(members = SortedSet(aUp, bUp, cUp, dLeaving, eJoining))
      val s1 = state(g1).copy(selfDc = "dc2")
      val g2 = Gossip(members = SortedSet(bUp, cUp, dExiting, eJoining))
      val s2 = state(g2).copy(selfDc = "dc2")

      diffRolesLeader(s0, s1) should ===(Set.empty[String])
      diffRolesLeader(s1, s2) should ===(Set.empty[String])
    }
  }
}
