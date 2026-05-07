/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

import org.scalatest.BeforeAndAfterEach

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.ClusterEvent._
import akka.cluster.ClusterSettings.DefaultDataCenter
import akka.cluster.InternalClusterAction._
import akka.cluster.MemberStatus._
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

object ClusterDomainEventPublisherSpec {
  val config = """
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    """
}

@nowarn("msg=Use Akka Distributed Cluster")
class ClusterDomainEventPublisherSpec
    extends AkkaSpec(ClusterDomainEventPublisherSpec.config)
    with BeforeAndAfterEach
    with ImplicitSender {

  val protocol = "akka"

  var publisher: ActorRef = _

  final val OtherDataCenter = "dc2"

  val aUp = TestMember(Address(protocol, "sys", "a", 2552), Up)
  val aLeaving = aUp.copy(status = Leaving)
  val aExiting = aLeaving.copy(status = Exiting)
  val aRemoved = aExiting.copy(status = Removed)
  val bExiting = TestMember(Address(protocol, "sys", "b", 2552), Exiting)
  val bRemoved = bExiting.copy(status = Removed)
  val cJoining = TestMember(Address(protocol, "sys", "c", 2552), Joining, Set("GRP"))
  val cUp = cJoining.copy(status = Up)
  val cRemoved = cUp.copy(status = Removed)
  val a51Up = TestMember(Address(protocol, "sys", "a", 2551), Up)
  val dUp = TestMember(Address(protocol, "sys", "d", 2552), Up, Set("GRP"))
  val eUp = TestMember(Address(protocol, "sys", "e", 2552), Up, Set("GRP"), OtherDataCenter)

  private def state(gossip: Gossip, self: UniqueAddress) =
    MembershipState(gossip, self, DefaultDataCenter, crossDcConnections = 5)

  val emptyMembershipState = state(Gossip.empty, aUp.uniqueAddress)

  val g0 = Gossip(members = SortedSet(aUp)).seen(aUp.uniqueAddress)
  val state0 = state(g0, aUp.uniqueAddress)
  val g1 = Gossip(members = SortedSet(aUp, cJoining)).seen(aUp.uniqueAddress).seen(cJoining.uniqueAddress)
  val state1 = state(g1, aUp.uniqueAddress)
  val g2 = Gossip(members = SortedSet(aUp, bExiting, cUp)).seen(aUp.uniqueAddress)
  val state2 = state(g2, aUp.uniqueAddress)
  val g3 = g2.seen(bExiting.uniqueAddress).seen(cUp.uniqueAddress)
  val state3 = state(g3, aUp.uniqueAddress)
  val g4 = Gossip(members = SortedSet(a51Up, aUp, bExiting, cUp)).seen(aUp.uniqueAddress)
  val state4 = state(g4, aUp.uniqueAddress)
  val g5 = Gossip(members = SortedSet(a51Up, aUp, bExiting, cUp))
    .seen(aUp.uniqueAddress)
    .seen(bExiting.uniqueAddress)
    .seen(cUp.uniqueAddress)
    .seen(a51Up.uniqueAddress)
  val state5 = state(g5, aUp.uniqueAddress)
  val g6 = Gossip(members = SortedSet(aLeaving, bExiting, cUp)).seen(aUp.uniqueAddress)
  val state6 = state(g6, aUp.uniqueAddress)
  val g7 = Gossip(members = SortedSet(aExiting, bExiting, cUp)).seen(aUp.uniqueAddress)
  val state7 = state(g7, aUp.uniqueAddress)
  val g8 = Gossip(
    members = SortedSet(aUp, bExiting, cUp, dUp),
    overview = GossipOverview(reachability = Reachability.empty.unreachable(aUp.uniqueAddress, dUp.uniqueAddress)))
    .seen(aUp.uniqueAddress)
  val state8 = state(g8, aUp.uniqueAddress)
  val g9 = Gossip(
    members = SortedSet(aUp, bExiting, cUp, dUp, eUp),
    overview = GossipOverview(reachability = Reachability.empty.unreachable(aUp.uniqueAddress, eUp.uniqueAddress)))
  val state9 = state(g9, aUp.uniqueAddress)
  val g10 = Gossip(
    members = SortedSet(aUp, bExiting, cUp, dUp, eUp),
    overview = GossipOverview(reachability = Reachability.empty))
  val state10 = state(g10, aUp.uniqueAddress)

  // created in beforeEach
  var memberSubscriber: TestProbe = _

  override def beforeEach(): Unit = {
    memberSubscriber = TestProbe()
    system.eventStream.subscribe(memberSubscriber.ref, classOf[MemberEvent])
    system.eventStream.subscribe(memberSubscriber.ref, classOf[LeaderChanged])
    system.eventStream.subscribe(memberSubscriber.ref, ClusterShuttingDown.getClass)

    publisher = system.actorOf(Props[ClusterDomainEventPublisher]())
    publisher ! PublishChanges(state0)
    memberSubscriber.expectMsg(MemberUp(aUp))
    memberSubscriber.expectMsg(LeaderChanged(Some(aUp.address)))
  }

  "ClusterDomainEventPublisher" must {

    "publish MemberJoined" in {
      publisher ! PublishChanges(state1)
      memberSubscriber.expectMsg(MemberJoined(cJoining))
    }

    "publish MemberUp" in {
      publisher ! PublishChanges(state2)
      publisher ! PublishChanges(state3)
      memberSubscriber.expectMsg(MemberExited(bExiting))
      memberSubscriber.expectMsg(MemberUp(cUp))
    }

    "publish leader changed" in {
      publisher ! PublishChanges(state4)
      memberSubscriber.expectMsg(MemberUp(a51Up))
      memberSubscriber.expectMsg(MemberExited(bExiting))
      memberSubscriber.expectMsg(MemberUp(cUp))
      memberSubscriber.expectMsg(LeaderChanged(Some(a51Up.address)))
      memberSubscriber.expectNoMessage()
    }

    "publish leader changed when old leader leaves and is removed" in {
      publisher ! PublishChanges(state3)
      memberSubscriber.expectMsg(MemberExited(bExiting))
      memberSubscriber.expectMsg(MemberUp(cUp))
      publisher ! PublishChanges(state6)
      memberSubscriber.expectMsg(MemberLeft(aLeaving))
      publisher ! PublishChanges(state7)
      memberSubscriber.expectMsg(MemberExited(aExiting))
      memberSubscriber.expectMsg(LeaderChanged(Some(cUp.address)))
      memberSubscriber.expectNoMessage()
      // at the removed member a an empty gossip is the last thing
      publisher ! PublishChanges(emptyMembershipState)
      memberSubscriber.expectMsg(MemberRemoved(aRemoved, Exiting))
      memberSubscriber.expectMsg(MemberRemoved(bRemoved, Exiting))
      memberSubscriber.expectMsg(MemberRemoved(cRemoved, Up))
      memberSubscriber.expectMsg(LeaderChanged(None))
    }

    "not publish leader changed when same leader" in {
      publisher ! PublishChanges(state4)
      memberSubscriber.expectMsg(MemberUp(a51Up))
      memberSubscriber.expectMsg(MemberExited(bExiting))
      memberSubscriber.expectMsg(MemberUp(cUp))
      memberSubscriber.expectMsg(LeaderChanged(Some(a51Up.address)))

      publisher ! PublishChanges(state5)
      memberSubscriber.expectNoMessage()
    }

    "publish role leader changed" in {
      val subscriber = TestProbe()
      publisher ! Subscribe(subscriber.ref, InitialStateAsSnapshot, Set(classOf[RoleLeaderChanged]))
      subscriber.expectMsgType[CurrentClusterState]
      publisher ! PublishChanges(state(Gossip(members = SortedSet(cJoining, dUp)), dUp.uniqueAddress))
      subscriber.expectMsgAllOf(
        RoleLeaderChanged("GRP", Some(dUp.address)),
        RoleLeaderChanged(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter, Some(dUp.address)))
      publisher ! PublishChanges(state(Gossip(members = SortedSet(cUp, dUp)), dUp.uniqueAddress))
      subscriber.expectMsg(RoleLeaderChanged("GRP", Some(cUp.address)))
    }

    "send CurrentClusterState when subscribe" in {
      val subscriber = TestProbe()
      publisher ! Subscribe(subscriber.ref, InitialStateAsSnapshot, Set(classOf[ClusterDomainEvent]))
      subscriber.expectMsgType[CurrentClusterState]
      // but only to the new subscriber
      memberSubscriber.expectNoMessage()
    }

    "send events corresponding to current state when subscribe" in {
      val subscriber = TestProbe()
      publisher ! PublishChanges(state8)
      publisher ! Subscribe(subscriber.ref, InitialStateAsEvents, Set(classOf[MemberEvent], classOf[ReachabilityEvent]))
      subscriber.receiveN(4).toSet should be(Set(MemberUp(aUp), MemberUp(cUp), MemberUp(dUp), MemberExited(bExiting)))
      subscriber.expectMsg(UnreachableMember(dUp))
      subscriber.expectNoMessage()
    }

    "send datacenter reachability events" in {
      val subscriber = TestProbe()
      publisher ! PublishChanges(state9)
      publisher ! Subscribe(subscriber.ref, InitialStateAsEvents, Set(classOf[DataCenterReachabilityEvent]))
      subscriber.expectMsg(UnreachableDataCenter(OtherDataCenter))
      subscriber.expectNoMessage()
      publisher ! PublishChanges(state10)
      subscriber.expectMsg(ReachableDataCenter(OtherDataCenter))
      subscriber.expectNoMessage()
    }

    "support unsubscribe" in {
      val subscriber = TestProbe()
      publisher ! Subscribe(subscriber.ref, InitialStateAsSnapshot, Set(classOf[MemberEvent]))
      subscriber.expectMsgType[CurrentClusterState]
      publisher ! Unsubscribe(subscriber.ref, Some(classOf[MemberEvent]))
      publisher ! PublishChanges(state3)
      subscriber.expectNoMessage()
      // but memberSubscriber is still subscriber
      memberSubscriber.expectMsg(MemberExited(bExiting))
      memberSubscriber.expectMsg(MemberUp(cUp))
    }

    "publish SeenChanged" in {
      val subscriber = TestProbe()
      publisher ! Subscribe(subscriber.ref, InitialStateAsSnapshot, Set(classOf[SeenChanged]))
      subscriber.expectMsgType[CurrentClusterState]
      publisher ! PublishChanges(state2)
      subscriber.expectMsgType[SeenChanged]
      subscriber.expectNoMessage()
      publisher ! PublishChanges(state3)
      subscriber.expectMsgType[SeenChanged]
      subscriber.expectNoMessage()
    }

    "publish ClusterShuttingDown and Removed when stopped" in {
      publisher ! PoisonPill
      memberSubscriber.expectMsg(ClusterShuttingDown)
      memberSubscriber.expectMsg(MemberRemoved(aRemoved, Up))
    }

  }
}
