/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.ClusterSettings.DefaultDataCenter
import akka.cluster.Gossip.vclockName

@nowarn("msg=Use Akka Distributed Cluster")
class GossipSpec extends AnyWordSpec with Matchers {

  import MemberStatus._

  val a1 = TestMember(Address("akka", "sys", "a", 2552), Up)
  val a2 = TestMember(a1.address, Joining)
  val b1 = TestMember(Address("akka", "sys", "b", 2552), Up)
  val b2 = TestMember(b1.address, Removed)
  val c1 = TestMember(Address("akka", "sys", "c", 2552), Leaving)
  val c2 = TestMember(c1.address, Up)
  val c3 = TestMember(c1.address, Exiting)
  val d1 = TestMember(Address("akka", "sys", "d", 2552), Leaving)
  val e1 = TestMember(Address("akka", "sys", "e", 2552), Joining)
  val e2 = TestMember(e1.address, Up)
  val e3 = TestMember(e1.address, Down)
  val f1 = TestMember(Address("akka", "sys", "f", 2552), Joining)

  val dc1a1 = TestMember(Address("akka", "sys", "a", 2552), Up, Set.empty, dataCenter = "dc1")
  val dc1b1 = TestMember(Address("akka", "sys", "b", 2552), Up, Set.empty, dataCenter = "dc1")
  val dc2c1 = TestMember(Address("akka", "sys", "c", 2552), Up, Set.empty, dataCenter = "dc2")
  val dc2d1 = TestMember(Address("akka", "sys", "d", 2552), Up, Set.empty, dataCenter = "dc2")
  val dc2d2 = TestMember(dc2d1.address, status = Down, roles = Set.empty, dataCenter = dc2d1.dataCenter)
  // restarted with another uid
  val dc2d3 =
    TestMember.withUniqueAddress(UniqueAddress(dc2d1.address, longUid = 3L), Up, Set.empty, dataCenter = "dc2")

  private def state(g: Gossip, selfMember: Member = a1): MembershipState =
    MembershipState(g, selfMember.uniqueAddress, selfMember.dataCenter, crossDcConnections = 5)

  "A Gossip" must {

    "have correct test setup" in {
      List(a1, a2, b1, b2, c1, c2, c3, d1, e1, e2, e3).foreach(m => m.dataCenter should ===(DefaultDataCenter))
    }

    "reach convergence when it's empty" in {
      state(Gossip.empty).convergence(Set.empty) should ===(true)
    }

    "reach convergence for one node" in {
      val g1 = Gossip(members = SortedSet(a1)).seen(a1.uniqueAddress)
      state(g1).convergence(Set.empty) should ===(true)
    }

    "not reach convergence until all have seen version" in {
      val g1 = Gossip(members = SortedSet(a1, b1)).seen(a1.uniqueAddress)
      state(g1).convergence(Set.empty) should ===(false)
    }

    "reach convergence for two nodes" in {
      val g1 = Gossip(members = SortedSet(a1, b1)).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      state(g1).convergence(Set.empty) should ===(true)
    }

    "reach convergence, skipping joining" in {
      // e1 is joining
      val g1 = Gossip(members = SortedSet(a1, b1, e1)).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      state(g1).convergence(Set.empty) should ===(true)
    }

    "reach convergence, skipping down" in {
      // e3 is down
      val g1 = Gossip(members = SortedSet(a1, b1, e3)).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      state(g1).convergence(Set.empty) should ===(true)
    }

    "reach convergence, skipping Leaving with exitingConfirmed" in {
      // c1 is Leaving
      val g1 = Gossip(members = SortedSet(a1, b1, c1)).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      state(g1).convergence(Set(c1.uniqueAddress)) should ===(true)
    }

    "reach convergence, skipping unreachable Leaving with exitingConfirmed" in {
      // c1 is Leaving
      val r1 = Reachability.empty.unreachable(b1.uniqueAddress, c1.uniqueAddress)
      val g1 = Gossip(members = SortedSet(a1, b1, c1), overview = GossipOverview(reachability = r1))
        .seen(a1.uniqueAddress)
        .seen(b1.uniqueAddress)
      state(g1).convergence(Set(c1.uniqueAddress)) should ===(true)
    }

    "not reach convergence when unreachable" in {
      val r1 = Reachability.empty.unreachable(b1.uniqueAddress, a1.uniqueAddress)
      val g1 = (Gossip(members = SortedSet(a1, b1), overview = GossipOverview(reachability = r1)))
        .seen(a1.uniqueAddress)
        .seen(b1.uniqueAddress)
      state(g1, b1).convergence(Set.empty) should ===(false)
      // but from a1's point of view (it knows that itself is not unreachable)
      state(g1).convergence(Set.empty) should ===(true)
    }

    "reach convergence when downed node has observed unreachable" in {
      // e3 is Down
      val r1 = Reachability.empty.unreachable(e3.uniqueAddress, a1.uniqueAddress)
      val g1 = (Gossip(members = SortedSet(a1, b1, e3), overview = GossipOverview(reachability = r1)))
        .seen(a1.uniqueAddress)
        .seen(b1.uniqueAddress)
        .seen(e3.uniqueAddress)
      state(g1, b1).convergence(Set.empty) should ===(true)
    }

    "merge members by status priority" in {
      val g1 = Gossip(members = SortedSet(a1, c1, e1))
      val g2 = Gossip(members = SortedSet(a2, c2, e2))

      val merged1 = g1.merge(g2)
      merged1.members should ===(SortedSet(a2, c1, e1))
      merged1.members.toSeq.map(_.status) should ===(Seq(Up, Leaving, Up))

      val merged2 = g2.merge(g1)
      merged2.members should ===(SortedSet(a2, c1, e1))
      merged2.members.toSeq.map(_.status) should ===(Seq(Up, Leaving, Up))

    }

    "merge unreachable" in {
      val r1 = Reachability.empty
        .unreachable(b1.uniqueAddress, a1.uniqueAddress)
        .unreachable(b1.uniqueAddress, c1.uniqueAddress)
      val g1 = Gossip(members = SortedSet(a1, b1, c1), overview = GossipOverview(reachability = r1))
      val r2 = Reachability.empty.unreachable(a1.uniqueAddress, d1.uniqueAddress)
      val g2 = Gossip(members = SortedSet(a1, b1, c1, d1), overview = GossipOverview(reachability = r2))

      val merged1 = g1.merge(g2)
      merged1.overview.reachability.allUnreachable should ===(Set(a1.uniqueAddress, c1.uniqueAddress, d1.uniqueAddress))

      val merged2 = g2.merge(g1)
      merged2.overview.reachability.allUnreachable should ===(merged1.overview.reachability.allUnreachable)
    }

    "merge members by removing removed members" in {
      // c3 removed
      val r1 = Reachability.empty.unreachable(b1.uniqueAddress, a1.uniqueAddress)
      val g1 = Gossip(members = SortedSet(a1, b1), overview = GossipOverview(reachability = r1))
      val r2 = r1.unreachable(b1.uniqueAddress, c3.uniqueAddress)
      val g2 = Gossip(members = SortedSet(a1, b1, c3), overview = GossipOverview(reachability = r2))

      val merged1 = g1.merge(g2)
      merged1.members should ===(SortedSet(a1, b1))
      merged1.overview.reachability.allUnreachable should ===(Set(a1.uniqueAddress))

      val merged2 = g2.merge(g1)
      merged2.overview.reachability.allUnreachable should ===(merged1.overview.reachability.allUnreachable)
      merged2.members should ===(merged1.members)
    }

    "have leader as first member based on ordering, except Exiting status" in {
      state(Gossip(members = SortedSet(c2, e2)), c2).leader should ===(Some(c2.uniqueAddress))
      state(Gossip(members = SortedSet(c3, e2)), c3).leader should ===(Some(e2.uniqueAddress))
      state(Gossip(members = SortedSet(c3)), c3).leader should ===(Some(c3.uniqueAddress))
    }

    "have leader as first reachable member based on ordering" in {
      val r1 = Reachability.empty.unreachable(e2.uniqueAddress, c2.uniqueAddress)
      val g1 = Gossip(members = SortedSet(c2, e2), overview = GossipOverview(reachability = r1))
      state(g1, e2).leader should ===(Some(e2.uniqueAddress))
      // but when c2 is selfUniqueAddress
      state(g1, c2).leader should ===(Some(c2.uniqueAddress))
    }

    "not have Down member as leader" in {
      state(Gossip(members = SortedSet(e3)), e3).leader should ===(None)
    }

    "have a leader per data center" in {
      val g1 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))

      // dc1a1 being leader of dc1
      state(g1, dc1a1).leader should ===(Some(dc1a1.uniqueAddress))
      state(g1, dc1b1).leader should ===(Some(dc1a1.uniqueAddress))

      // and dc2c1 being leader of dc2
      state(g1, dc2c1).leader should ===(Some(dc2c1.uniqueAddress))
      state(g1, dc2d1).leader should ===(Some(dc2c1.uniqueAddress))
    }

    "merge seen table correctly" in {
      val vclockNode = VectorClock.Node("something")
      val g1 = (Gossip(members = SortedSet(a1, b1, c1, d1)) :+ vclockNode).seen(a1.uniqueAddress).seen(b1.uniqueAddress)
      val g2 = (Gossip(members = SortedSet(a1, b1, c1, d1)) :+ vclockNode).seen(a1.uniqueAddress).seen(c1.uniqueAddress)
      val g3 = g1.copy(version = g2.version).seen(d1.uniqueAddress)

      def checkMerged(merged: Gossip): Unit = {
        val seen = merged.overview.seen.toSeq
        seen.length should ===(0)

        merged.seenByNode(a1.uniqueAddress) should ===(false)
        merged.seenByNode(b1.uniqueAddress) should ===(false)
        merged.seenByNode(c1.uniqueAddress) should ===(false)
        merged.seenByNode(d1.uniqueAddress) should ===(false)
        merged.seenByNode(e1.uniqueAddress) should ===(false)
      }

      checkMerged(g3.merge(g2))
      checkMerged(g2.merge(g3))
    }

    "know who is youngest" in {
      // a2 and e1 is Joining
      val g1 =
        Gossip(
          members = SortedSet(a2, b1.copyUp(3), e1),
          overview = GossipOverview(reachability = Reachability.empty.unreachable(a2.uniqueAddress, e1.uniqueAddress)))
      state(g1).youngestMember should ===(b1)
      val g2 = Gossip(
        members = SortedSet(a2, b1.copyUp(3), e1),
        overview = GossipOverview(
          reachability = Reachability.empty
            .unreachable(a2.uniqueAddress, b1.uniqueAddress)
            .unreachable(a2.uniqueAddress, e1.uniqueAddress)))
      state(g2).youngestMember should ===(b1)
      val g3 = Gossip(members = SortedSet(a2, b1.copyUp(3), e2.copyUp(4)))
      state(g3).youngestMember should ===(e2)
    }

    "reach convergence per data center" in {
      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1a1.uniqueAddress)
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
        .seen(dc2d1.uniqueAddress)
      state(g, dc1a1).leader should ===(Some(dc1a1.uniqueAddress))
      state(g, dc1a1).convergence(Set.empty) should ===(true)

      state(g, dc2c1).leader should ===(Some(dc2c1.uniqueAddress))
      state(g, dc2c1).convergence(Set.empty) should ===(true)
    }

    "reach convergence per data center even if members of another data center has not seen the gossip" in {
      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1a1.uniqueAddress)
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
      // dc2d1 has not seen the gossip

      // so dc1 can reach convergence
      state(g, dc1a1).leader should ===(Some(dc1a1.uniqueAddress))
      state(g, dc1a1).convergence(Set.empty) should ===(true)

      // but dc2 cannot
      state(g, dc2c1).leader should ===(Some(dc2c1.uniqueAddress))
      state(g, dc2c1).convergence(Set.empty) should ===(false)
    }

    "not reach convergence for first member of other data center until all have seen the gossip" in {
      val dc2e1 = TestMember(e1.address, status = Joining, roles = Set.empty, dataCenter = "dc2")
      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2e1)).seen(dc1a1.uniqueAddress).seen(dc2e1.uniqueAddress)
      // dc1b1 has not seen the gossip

      // dc1 hasn't reached convergence because dc1b1 hasn't marked it as seen
      state(g, dc1a1).convergence(Set.empty) should ===(false)

      // and not dc2 because dc2e1 is only Joining
      state(g, dc2e1).convergence(Set.empty) should ===(false)

      // until all have seen it
      val g2 = g.seen(dc1b1.uniqueAddress)
      state(g2, dc2e1).convergence(Set.empty) should ===(true)
    }

    "not reach convergence for first member of other data center until all have seen the gossip 2" in {
      // reproducer test for issue #29486
      val dc2e1 = TestMember(e1.address, status = Joining, roles = Set.empty, dataCenter = "dc2")
      val dc2f1 = TestMember(f1.address, status = Joining, roles = Set.empty, dataCenter = "dc2")
      val g =
        Gossip(members = SortedSet(dc1a1, dc1b1, dc2e1, dc2f1))
          .seen(dc1a1.uniqueAddress)
          .seen(dc1b1.uniqueAddress)
          .seen(dc2f1.uniqueAddress)

      // dc2 hasn't reached convergence because dc2e1 has not seen it (and that matters even though it is only Joining)
      state(g, dc2f1).convergence(Set.empty) should ===(false)

      // until all have seen it
      val g2 = g.seen(dc2e1.uniqueAddress)
      state(g2, dc2f1).convergence(Set.empty) should ===(true)
    }

    "reach convergence per data center even if another data center contains unreachable" in {
      val r1 = Reachability.empty.unreachable(dc2c1.uniqueAddress, dc2d1.uniqueAddress)

      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1), overview = GossipOverview(reachability = r1))
        .seen(dc1a1.uniqueAddress)
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
        .seen(dc2d1.uniqueAddress)

      // this data center doesn't care about dc2 having reachability problems and can reach convergence
      state(g, dc1a1).leader should ===(Some(dc1a1.uniqueAddress))
      state(g, dc1a1).convergence(Set.empty) should ===(true)

      // this data center is cannot reach convergence because of unreachability within the data center
      state(g, dc2c1).leader should ===(Some(dc2c1.uniqueAddress))
      state(g, dc2c1).convergence(Set.empty) should ===(false)
    }

    "reach convergence per data center even if there is unreachable nodes in another data center" in {
      val r1 = Reachability.empty
        .unreachable(dc1a1.uniqueAddress, dc2d1.uniqueAddress)
        .unreachable(dc2d1.uniqueAddress, dc1a1.uniqueAddress)

      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1), overview = GossipOverview(reachability = r1))
        .seen(dc1a1.uniqueAddress)
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
        .seen(dc2d1.uniqueAddress)

      // neither data center is affected by the inter data center unreachability as far as convergence goes
      state(g, dc1a1).leader should ===(Some(dc1a1.uniqueAddress))
      state(g, dc1a1).convergence(Set.empty) should ===(true)

      state(g, dc2c1).leader should ===(Some(dc2c1.uniqueAddress))
      state(g, dc2c1).convergence(Set.empty) should ===(true)
    }

    "ignore cross data center unreachability when determining inside of data center reachability" in {
      val r1 = Reachability.empty
        .unreachable(dc1a1.uniqueAddress, dc2c1.uniqueAddress)
        .unreachable(dc2c1.uniqueAddress, dc1a1.uniqueAddress)

      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1), overview = GossipOverview(reachability = r1))

      // inside of the data center we don't care about the cross data center unreachability
      g.isReachable(dc1a1.uniqueAddress, dc1b1.uniqueAddress) should ===(true)
      g.isReachable(dc1b1.uniqueAddress, dc1a1.uniqueAddress) should ===(true)
      g.isReachable(dc2c1.uniqueAddress, dc2d1.uniqueAddress) should ===(true)
      g.isReachable(dc2d1.uniqueAddress, dc2c1.uniqueAddress) should ===(true)

      state(g, dc1a1).isReachableExcludingDownedObservers(dc1b1.uniqueAddress) should ===(true)
      state(g, dc1b1).isReachableExcludingDownedObservers(dc1a1.uniqueAddress) should ===(true)
      state(g, dc2c1).isReachableExcludingDownedObservers(dc2d1.uniqueAddress) should ===(true)
      state(g, dc2d1).isReachableExcludingDownedObservers(dc2c1.uniqueAddress) should ===(true)

      // between data centers it matters though
      g.isReachable(dc1a1.uniqueAddress, dc2c1.uniqueAddress) should ===(false)
      g.isReachable(dc2c1.uniqueAddress, dc1a1.uniqueAddress) should ===(false)
      // this isReachable method only says false for specific unreachable entries between the nodes
      g.isReachable(dc1b1.uniqueAddress, dc2c1.uniqueAddress) should ===(true)
      g.isReachable(dc2d1.uniqueAddress, dc1a1.uniqueAddress) should ===(true)

      // this one looks at all unreachable-entries for the to-address
      state(g, dc1a1).isReachableExcludingDownedObservers(dc2c1.uniqueAddress) should ===(false)
      state(g, dc1b1).isReachableExcludingDownedObservers(dc2c1.uniqueAddress) should ===(false)
      state(g, dc2c1).isReachableExcludingDownedObservers(dc1a1.uniqueAddress) should ===(false)
      state(g, dc2d1).isReachableExcludingDownedObservers(dc1a1.uniqueAddress) should ===(false)

      // between the two other nodes there is no unreachability
      g.isReachable(dc1b1.uniqueAddress, dc2d1.uniqueAddress) should ===(true)
      g.isReachable(dc2d1.uniqueAddress, dc1b1.uniqueAddress) should ===(true)

      state(g, dc1b1).isReachableExcludingDownedObservers(dc2d1.uniqueAddress) should ===(true)
      state(g, dc2d1).isReachableExcludingDownedObservers(dc1b1.uniqueAddress) should ===(true)
    }

    "not returning a downed data center leader" in {
      val g = Gossip(members = SortedSet(dc1a1.copy(Down), dc1b1))
      state(g, dc1b1).leaderOf(g.members) should ===(Some(dc1b1.uniqueAddress))
    }

    "ignore cross data center unreachability when determining data center leader" in {
      val r1 = Reachability.empty
        .unreachable(dc1a1.uniqueAddress, dc2d1.uniqueAddress)
        .unreachable(dc2d1.uniqueAddress, dc1a1.uniqueAddress)

      val g = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1), overview = GossipOverview(reachability = r1))

      state(g, dc1a1).leaderOf(g.members) should ===(Some(dc1a1.uniqueAddress))
      state(g, dc1b1).leaderOf(g.members) should ===(Some(dc1a1.uniqueAddress))

      state(g, dc2c1).leaderOf(g.members) should ===(Some(dc2c1.uniqueAddress))
      state(g, dc2d1).leaderOf(g.members) should ===(Some(dc2c1.uniqueAddress))
    }

    // TODO test coverage for when leaderOf returns None - I have not been able to figure it out

    "clear out a bunch of stuff when removing a node" in {
      val g = Gossip(
        members = SortedSet(dc1a1, dc1b1, dc2d2),
        overview = GossipOverview(
          reachability = Reachability.empty
            .unreachable(dc1b1.uniqueAddress, dc2d2.uniqueAddress)
            .unreachable(dc2d2.uniqueAddress, dc1b1.uniqueAddress)))
        .:+(VectorClock.Node(Gossip.vclockName(dc1b1.uniqueAddress)))
        .:+(VectorClock.Node(Gossip.vclockName(dc2d2.uniqueAddress)))
        .remove(dc1b1.uniqueAddress, System.currentTimeMillis())

      g.seenBy should not contain (dc1b1.uniqueAddress)
      g.overview.reachability.records.map(_.observer) should not contain (dc1b1.uniqueAddress)
      g.overview.reachability.records.map(_.subject) should not contain (dc1b1.uniqueAddress)

      // sort order should be kept
      g.members.toList should ===(List(dc1a1, dc2d2))
      g.version.versions.keySet should not contain (VectorClock.Node(Gossip.vclockName(dc1b1.uniqueAddress)))
      g.version.versions.keySet should contain(VectorClock.Node(Gossip.vclockName(dc2d2.uniqueAddress)))
    }

    "not reintroduce members from out-of data center gossip when merging" in {
      // dc1 does not know about any unreachability nor that the node has been downed
      val gdc1 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
        .:+(VectorClock.Node(vclockName(dc2d1.uniqueAddress))) // just to make sure these are also pruned

      // dc2 has downed the dc2d1 node, seen it as unreachable and removed it
      val gdc2 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1a1.uniqueAddress)
        .remove(dc2d1.uniqueAddress, System.currentTimeMillis())

      gdc2.tombstones.keys should contain(dc2d1.uniqueAddress)
      gdc2.members should not contain (dc2d1)
      gdc2.overview.reachability.records.filter(r =>
        r.subject == dc2d1.uniqueAddress || r.observer == dc2d1.uniqueAddress) should be(empty)
      gdc2.overview.reachability.versions.keys should not contain (dc2d1.uniqueAddress)

      // when we merge the two, it should not be reintroduced
      val merged1 = gdc2.merge(gdc1)
      merged1.members should ===(SortedSet(dc1a1, dc1b1, dc2c1))

      merged1.tombstones.keys should contain(dc2d1.uniqueAddress)
      merged1.members should not contain (dc2d1)
      merged1.overview.reachability.records.filter(r =>
        r.subject == dc2d1.uniqueAddress || r.observer == dc2d1.uniqueAddress) should be(empty)
      merged1.overview.reachability.versions.keys should not contain (dc2d1.uniqueAddress)
      merged1.version.versions.keys should not contain (VectorClock.Node(vclockName(dc2d1.uniqueAddress)))
    }

    "replace member when removed and rejoined in another data center" in {
      // dc1 does not know removal and rejoin of new incarnation
      val gdc1 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1b1.uniqueAddress)
        .seen(dc2c1.uniqueAddress)
        .:+(VectorClock.Node(vclockName(dc2d1.uniqueAddress))) // just to make sure these are also pruned

      // dc2 has removed the dc2d1 node, and then same host:port is restarted and joins again, without dc1 knowning
      val gdc2 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d3))
        .seen(dc1a1.uniqueAddress)
        .remove(dc2d1.uniqueAddress, System.currentTimeMillis())
        .copy(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d3))

      gdc2.tombstones.keys should contain(dc2d1.uniqueAddress)
      gdc2.members.map(_.uniqueAddress) should not contain (dc2d1.uniqueAddress)
      gdc2.members.map(_.uniqueAddress) should contain(dc2d3.uniqueAddress)

      // when we merge the two, it should replace the old with new
      val merged1 = gdc2.merge(gdc1)
      merged1.members should ===(SortedSet(dc1a1, dc1b1, dc2c1, dc2d3))
      merged1.members.map(_.uniqueAddress) should not contain (dc2d1.uniqueAddress)
      merged1.members.map(_.uniqueAddress) should contain(dc2d3.uniqueAddress)

      merged1.tombstones.keys should contain(dc2d1.uniqueAddress)
      merged1.tombstones.keys should not contain (dc2d3.uniqueAddress)
      merged1.overview.reachability.records.filter(r =>
        r.subject == dc2d1.uniqueAddress || r.observer == dc2d1.uniqueAddress) should be(empty)
      merged1.overview.reachability.versions.keys should not contain (dc2d1.uniqueAddress)
      merged1.version.versions.keys should not contain (VectorClock.Node(vclockName(dc2d1.uniqueAddress)))
    }

    "correctly prune vector clocks based on tombstones when merging" in {
      val gdc1 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .:+(VectorClock.Node(vclockName(dc1a1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc1b1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc2c1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc2d1.uniqueAddress)))
        .remove(dc1b1.uniqueAddress, System.currentTimeMillis())

      gdc1.version.versions.keySet should not contain (VectorClock.Node(vclockName(dc1b1.uniqueAddress)))

      val gdc2 = Gossip(members = SortedSet(dc1a1, dc1b1, dc2c1, dc2d1))
        .seen(dc1a1.uniqueAddress)
        .:+(VectorClock.Node(vclockName(dc1a1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc1b1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc2c1.uniqueAddress)))
        .:+(VectorClock.Node(vclockName(dc2d1.uniqueAddress)))
        .remove(dc2c1.uniqueAddress, System.currentTimeMillis())

      gdc2.version.versions.keySet should not contain (VectorClock.Node(vclockName(dc2c1.uniqueAddress)))

      // when we merge the two, the nodes should not be reintroduced
      val merged1 = gdc2.merge(gdc1)
      merged1.members should ===(SortedSet(dc1a1, dc2d1))

      merged1.version.versions.keySet should ===(
        Set(VectorClock.Node(vclockName(dc1a1.uniqueAddress)), VectorClock.Node(vclockName(dc2d1.uniqueAddress))))
    }

    "prune old tombstones" in {
      val timestamp = 352684800
      val g = Gossip(members = SortedSet(dc1a1, dc1b1)).remove(dc1b1.uniqueAddress, timestamp)

      g.tombstones.keys should contain(dc1b1.uniqueAddress)

      val pruned = g.pruneTombstones(timestamp + 1)

      // when we merge the two, it should not be reintroduced
      pruned.tombstones.keys should not contain (dc1b1.uniqueAddress)
    }

    "mark a node as down" in {
      val g =
        Gossip(members = SortedSet(dc1a1, dc1b1)).seen(dc1a1.uniqueAddress).seen(dc1b1.uniqueAddress).markAsDown(dc1b1)

      g.member(dc1b1.uniqueAddress).status should ===(MemberStatus.Down)
      g.overview.seen should not contain (dc1b1.uniqueAddress)

      // obviously the other member should be unaffected
      g.member(dc1a1.uniqueAddress).status should ===(dc1a1.status)
      g.overview.seen should contain(dc1a1.uniqueAddress)
    }

    "update members" in {
      val joining = TestMember(Address("akka", "sys", "d", 2552), Joining, Set.empty, dataCenter = "dc2")
      val g = Gossip(members = SortedSet(dc1a1, joining))

      g.member(joining.uniqueAddress).status should ===(Joining)

      val updated = g.update(SortedSet(joining.copy(status = Up)))

      updated.member(joining.uniqueAddress).status should ===(Up)

      // obviously the other member should be unaffected
      updated.member(dc1a1.uniqueAddress).status should ===(dc1a1.status)

      // order should be kept
      updated.members.toList.map(_.uniqueAddress) should ===(List(dc1a1.uniqueAddress, joining.uniqueAddress))
    }
  }
}
