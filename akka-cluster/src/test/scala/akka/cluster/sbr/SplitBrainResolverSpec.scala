/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.cluster._
import akka.cluster.ClusterEvent.LeaderChanged
import akka.cluster.ClusterEvent.MemberExited
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberWeaklyUp
import akka.cluster.ClusterEvent.ReachabilityChanged
import akka.cluster.ClusterEvent.ReachableDataCenter
import akka.cluster.ClusterEvent.ReachableMember
import akka.cluster.ClusterEvent.UnreachableDataCenter
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.MemberStatus._
import akka.coordination.lease.LeaseSettings
import akka.coordination.lease.TestLease
import akka.coordination.lease.TimeoutSettings
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter

object SplitBrainResolverSpec {

  final case class DownCalled(address: Address)

  object DowningTestActor {
    def props(
        stableAfter: FiniteDuration,
        strategy: DowningStrategy,
        probe: ActorRef,
        selfUniqueAddress: UniqueAddress,
        selfDc: DataCenter,
        downAllWhenUnstable: FiniteDuration,
        tickInterval: FiniteDuration): Props =
      Props(
        new DowningTestActor(
          stableAfter,
          strategy,
          probe,
          selfUniqueAddress,
          selfDc,
          downAllWhenUnstable,
          tickInterval))
  }

  class DowningTestActor(
      stableAfter: FiniteDuration,
      strategy: DowningStrategy,
      probe: ActorRef,
      override val selfUniqueAddress: UniqueAddress,
      override val selfDc: DataCenter,
      override val downAllWhenUnstable: FiniteDuration,
      val tick: FiniteDuration)
      extends SplitBrainResolverBase(stableAfter, strategy) {

    // manual ticks used in this test
    override def tickInterval: FiniteDuration =
      if (tick == Duration.Zero) super.tickInterval else tick

    // immediate overdue if Duration.Zero is used
    override def newStableDeadline(): Deadline = super.newStableDeadline() - 1.nanos

    var downed = Set.empty[Address]

    override def down(node: UniqueAddress, decision: DowningStrategy.Decision): Unit = {
      if (leader && !downed(node.address)) {
        downed += node.address
        probe ! DownCalled(node.address)
      } else if (!leader)
        probe ! "down must only be done by leader"
    }

    override def receive: Receive =
      ({
        case UnreachableMember(m) if strategy.unreachable(m.uniqueAddress) => // already unreachable
        case ReachableMember(m) if !strategy.unreachable(m.uniqueAddress)  => // already reachable
      }: Receive).orElse(super.receive)
  }
}

@nowarn("msg=Use Akka Distributed Cluster")
class SplitBrainResolverSpec
    extends AkkaSpec("""
  |akka {
  |  actor.provider = cluster
  |  cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
  |  cluster.split-brain-resolver.active-strategy=keep-majority
  |  remote.artery.canonical {
  |    hostname = "127.0.0.1"
  |    port = 0
  |  }
  |}
  """.stripMargin)
    with Eventually {

  import DowningStrategy._
  import SplitBrainResolverSpec._
  import TestAddresses._

  private val selfDc = TestAddresses.defaultDataCenter
  private lazy val selfUniqueAddress = Cluster(system).selfUniqueAddress

  private val testLeaseSettings =
    new LeaseSettings("akka-sbr", "test", new TimeoutSettings(1.second, 2.minutes, 3.seconds), ConfigFactory.empty)

  def createReachability(unreachability: Seq[(Member, Member)]): Reachability = {
    Reachability(unreachability.map {
      case (from, to) => Reachability.Record(from.uniqueAddress, to.uniqueAddress, Reachability.Unreachable, 1)
    }.toIndexedSeq, unreachability.map {
      case (from, _) => from.uniqueAddress -> 1L
    }.toMap)
  }

  def extSystem: ExtendedActorSystem = system.asInstanceOf[ExtendedActorSystem]

  abstract class StrategySetup {
    def createStrategy(): DowningStrategy

    var side1: Set[Member] = Set.empty
    var side2: Set[Member] = Set.empty
    var side3: Set[Member] = Set.empty

    def side1Nodes: Set[UniqueAddress] = side1.map(_.uniqueAddress)
    def side2Nodes: Set[UniqueAddress] = side2.map(_.uniqueAddress)
    def side3Nodes: Set[UniqueAddress] = side3.map(_.uniqueAddress)

    var indirectlyConnected: Seq[(Member, Member)] = Nil

    private def initStrategy(): DowningStrategy = {
      val strategy = createStrategy()
      (side1 ++ side2 ++ side3).foreach(strategy.add)
      strategy
    }

    def assertDowning(members: Set[Member]): Unit = {
      assertDowningSide(side1, members)
      assertDowningSide(side2, members)
      assertDowningSide(side3, members)
    }

    def assertDowningSide(side: Set[Member], members: Set[Member]): Unit = {
      if (side.nonEmpty)
        strategy(side).nodesToDown() should be(members.map(_.uniqueAddress))
    }

    def strategy(side: Set[Member]): DowningStrategy = {
      val others = side1 ++ side2 ++ side3 -- side
      (side -- others) should be(side)

      if (side.nonEmpty) {
        val strategy = initStrategy()
        val unreachability = (indirectlyConnected ++ others.map(o => side.head -> o)).toSet.toList
        val r = createReachability(unreachability)
        strategy.setReachability(r)

        unreachability.foreach { case (_, to) => strategy.addUnreachable(to) }

        strategy.setSeenBy(side.map(_.address))

        strategy
      } else
        createStrategy()
    }

  }

  "StaticQuorum" must {
    class Setup2(size: Int, role: Option[String]) extends StrategySetup {
      override def createStrategy() =
        new StaticQuorum(selfDc, size, role, selfUniqueAddress)
    }

    "down unreachable when enough reachable nodes" in new Setup2(3, None) {
      side1 = Set(memberA, memberC, memberE)
      side2 = Set(memberB, memberD)
      assertDowning(side2)
    }

    "down reachable when not enough reachable nodes" in {
      val setup = new Setup2(size = 3, None) {
        side1 = Set(memberA, memberB)
        side2 = Set(memberC, memberD)
      }
      import setup._
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownReachable)
    }

    "down unreachable when enough reachable nodes with role" in new Setup2(2, Some("role3")) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE)
      assertDowning(side2)
    }

    "down all if N > static-quorum.size * 2 - 1" in new Setup2(3, None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF)
      assertDowning(side1.union(side2))
    }

    "handle joining" in {
      val setup = new Setup2(size = 3, None) {
        side1 = Set(memberA, memberB, joining(memberC))
        side2 = Set(memberD, memberE, joining(memberF))
      }
      import setup._
      // Joining not counted
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownReachable)

      // if C becomes Up
      side1 = Set(memberA, memberB, memberC)
      strategy(side1).decide() should ===(DownUnreachable)
      strategy(side2).decide() should ===(DownReachable)

      // if F becomes Up, C still Joining
      side1 = Set(memberA, memberB, joining(memberC))
      side2 = Set(memberD, memberE, memberF)
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownUnreachable)

      // if both C and F become Up, too many
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF)
      strategy(side1).decide() should ===(DownAll)
      strategy(side2).decide() should ===(DownAll)
    }

    "handle leaving/exiting" in {
      val setup = new Setup2(size = 3, None) {
        side1 = Set(memberA, memberB, leaving(memberC))
        side2 = Set(memberD, memberE)
      }
      import setup._
      strategy(side1).decide() should ===(DownUnreachable)
      strategy(side2).decide() should ===(DownReachable)

      side1 = Set(memberA, memberB, exiting(memberC))
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownReachable)
    }
  }

  "KeepMajority" must {
    class Setup2(role: Option[String]) extends StrategySetup {
      override def createStrategy() =
        new KeepMajority(selfDc, role, selfUniqueAddress)
    }

    "down minority partition: {A, C, E} | {B, D} => {A, C, E}" in new Setup2(role = None) {
      side1 = Set(memberA, memberC, memberE)
      side2 = Set(memberB, memberD)
      assertDowning(side2)
    }

    "down minority partition: {A, B} | {C, D, E} => {C, D, E}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB)
      side2 = Set(memberC, memberD, memberE)
      assertDowning(side1)
    }

    "down self when alone: {B} | {A, C} => {A, C}" in new Setup2(role = None) {
      side1 = Set(memberB)
      side2 = Set(memberA, memberC)
      assertDowning(side1)
    }

    "keep half with lowest address when equal size partition: {A, B} | {C, D} => {A, B}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB)
      side2 = Set(memberC, memberD)
      assertDowning(side2)
    }

    "keep node with lowest address in two node cluster: {A} | {B} => {A}" in new Setup2(role = None) {
      side1 = Set(memberA)
      side2 = Set(memberB)
      assertDowning(side2)
    }

    "down minority partition with role: {A*, B*} | {C, D*, E} => {A*, B*}" in new Setup2(role = Some("role3")) {
      side1 = Set(memberA, memberB)
      side2 = Set(memberC, memberD, memberE)
      assertDowning(side2)
    }

    "keep half with lowest address with role when equal size partition: {A, D*, E} | {B, C*} => {B, C*}" in
    new Setup2(role = Some("role2")) {
      side1 = Set(memberA, memberD, memberE)
      side2 = Set(memberB, memberC)
      // memberC is lowest with role2
      assertDowning(side1)
    }

    "down all when no node with role: {C} | {E} => {}" in new Setup2(role = Some("role3")) {
      side1 = Set(memberC)
      side2 = Set(memberE)
      assertDowning(side1 ++ side2)
    }

    "not count joining node, but down it: {B, D} | {Aj, C} => {B, D}" in new Setup2(role = None) {
      side1 = Set(memberB, memberD)
      side2 = Set(joining(memberA), memberC)
      assertDowning(side2)
    }

    "down minority partition and joining node: {A, Bj} | {C, D, E} => {C, D, E}" in new Setup2(role = None) {
      side1 = Set(memberA, joining(memberB))
      side2 = Set(memberC, memberD, memberE)
      assertDowning(side1)
    }

    "down each part when split in 3 too small parts: {A, B} | {C, D} | {E} => {}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB)
      side2 = Set(memberC, memberD)
      side3 = Set(memberE)
      assertDowningSide(side1, side1)
      assertDowningSide(side2, side2)
      assertDowningSide(side3, side3)
    }

    "detect edge case of membership change: {A, B, F', G'} | {C, D, E} => {A, B, F, G}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB, memberF, memberG)
        side2 = Set(memberC, memberD, memberE)
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(DownUnreachable)
      strategy1.nodesToDown(decision1) should ===(side2Nodes)

      // F and G were moved to Up by side1 at the same time as the partition, and that has not been seen by
      // side2 so they are still joining
      side1 = Set(memberA, memberB, joining(memberF), joining(memberG))
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(DownAll)
      strategy2.nodesToDown(decision2) should ===(side1Nodes.union(side2Nodes))
    }

    "detect edge case of membership change when equal size: {A, B, F'} | {C, D, E} => {A, B, F}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB, memberF)
        side2 = Set(memberC, memberD, memberE)
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      // memberA is lowest address
      decision1 should ===(DownUnreachable)
      strategy1.nodesToDown(decision1) should ===(side2Nodes)

      // F was moved to Up by side1 at the same time as the partition, and that has not been seen by
      // side2 so it is still joining
      side1 = Set(memberA, memberB, joining(memberF))
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      // when counting the joining F it becomes equal size
      decision2 should ===(DownAll)
      strategy2.nodesToDown(decision2) should ===(side1Nodes.union(side2Nodes))
    }

    "detect safe edge case of membership change: {A, B} | {C, D, E, F'} => {C, D, E, F}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB)
        side2 = Set(memberC, memberD, memberE, joining(memberF))
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(DownReachable)
      strategy1.nodesToDown(decision1) should ===(side1Nodes)

      // F was moved to Up by side2 at the same time as the partition
      side2 = Set(memberC, memberD, memberE, memberF)
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(DownUnreachable)
      strategy2.nodesToDown(decision2) should ===(side1Nodes)
    }

    "detect edge case of leaving/exiting membership change: {A', B} | {C, D} => {C, D}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(leaving(memberA), memberB, joining(memberE))
        side2 = Set(memberC, memberD)
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(DownAll)
      strategy1.nodesToDown(decision1) should ===(side1Nodes.union(side2Nodes))

      // A was moved to Exiting by side2 at the same time as the partition, and that has not been seen by
      // side1 so it is still Leaving there
      side1 = Set(exiting(memberA), memberB)
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(DownUnreachable)
      // A is already Exiting so not downed
      strategy2.nodesToDown(decision2) should ===(side1Nodes - memberA.uniqueAddress)
    }

    "down indirectly connected: {(A, B)} => {}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      assertDowning(Set(memberA, memberB))
    }

    "down indirectly connected: {(A, B), C} => {C}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      // keep fully connected memberC
      assertDowning(Set(memberA, memberB))
    }

    "down indirectly connected: {(A, B, C)} => {}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberC, memberC -> memberA)
      assertDowning(Set(memberA, memberB, memberC))
    }

    "down indirectly connected: {(A, B, C, D)} => {}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC, memberD)
      indirectlyConnected = List(memberA -> memberD, memberD -> memberA, memberB -> memberC, memberC -> memberB)
      assertDowning(Set(memberA, memberB, memberC, memberD))
    }

    "down indirectly connected: {(A, B, C), D, E} => {D, E}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC, memberD, memberE)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberC, memberC -> memberA)
      // keep fully connected memberD, memberE
      assertDowning(Set(memberA, memberB, memberC))
    }

    "down indirectly connected{A, (B, C), D, (E, F), G} => {A, D, G}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC, memberD, memberE, memberF, memberG)
      // two groups of indirectly connected, 4 in total
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB, memberE -> memberF, memberF -> memberE)
      // keep fully connected memberA, memberD, memberG
      assertDowning(Set(memberB, memberC, memberE, memberF))
    }

    "down indirectly connected, detected via seen: {(A, B, C)} => {}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC)
      indirectlyConnected = List(memberA -> memberB, memberA -> memberC)
      assertDowning(Set(memberA, memberB, memberC))
    }

    "down indirectly connected, detected via seen: {(A, B, C, D), E} => {E}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberC, memberD, memberE)
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB, memberA -> memberD)
      // keep fully connected memberE
      assertDowning(Set(memberA, memberB, memberC, memberD))
    }

    "down indirectly connected when combined with crashed: {(A, B), D, E} | {C} => {D, E}" in new Setup2(role = None) {
      side1 = Set(memberA, memberB, memberD, memberE)
      side2 = Set(memberC)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      // keep fully connected memberD, memberE
      // note that crashed memberC is also downed
      assertDowningSide(side1, Set(memberA, memberB, memberC))
    }

    "down indirectly connected when combined with clean partition: {A, (B, C)} | {D, E} => {A}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE)
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)

      // from side1 of the partition
      // keep fully connected memberA
      // note that memberD and memberE on the other side of the partition are also downed because side1
      // is majority of clean partition
      assertDowningSide(side1, Set(memberB, memberC, memberD, memberE))

      // from side2 of the partition
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      // Note that memberC is not downed, as on the other side, because those indirectly connected
      // not seen from this side. That outcome is OK.
      assertDowningSide(side2, Set(memberD, memberE))

      // alternative scenario from side2 of the partition
      // indirectly connected on side1 happens before the clean partition
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)
      assertDowningSide(side2, Set(memberB, memberC, memberD, memberE))
    }

    "down indirectly connected on minority side, when combined with clean partition: {A, (B, C)} | {D, E, F, G} => {D, E, F, G}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)

      // from side1 of the partition, minority
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // from side2 of the partition, majority
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      assertDowningSide(side2, Set(memberA, memberB, memberC))

      // alternative scenario from side2 of the partition
      // indirectly connected on side1 happens before the clean partition
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)
      assertDowningSide(side2, Set(memberA, memberB, memberC))
    }

    "down indirectly connected on majority side, when combined with clean partition: {A, B, C} | {(D, E), F, G} => {F, G}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)

      // from side1 of the partition, minority
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // alternative scenario from side1 of the partition
      // indirectly connected on side2 happens before the clean partition
      indirectlyConnected = List(memberD -> memberE, memberE -> memberD)
      // note that indirectly connected memberD and memberE are also downed
      assertDowningSide(side1, Set(memberA, memberB, memberC, memberD, memberE))

      // from side2 of the partition, majority
      indirectlyConnected = List(memberD -> memberE, memberE -> memberD)
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberD, memberE))
    }

    "down indirectly connected spanning across a clean partition: {A, (B), C} | {D, (E, F), G} => {D, G}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)
      indirectlyConnected = List(memberB -> memberE, memberE -> memberF, memberF -> memberB)

      // from side1 of the partition, minority
      assertDowningSide(side1, Set(memberA, memberB, memberC, memberE, memberF))

      // from side2 of the partition,  majority
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberE, memberF))
    }

    "down indirectly connected, detected via seen, combined with clean partition: {A, B, C} | {(D, E), (F, G)} => {}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)

      // from side1 of the partition, minority
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // from side2 of the partition,  majority
      indirectlyConnected = List(memberD -> memberE, memberG -> memberF)
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberD, memberE, memberF, memberG))
    }

    "down all when indirectly connected happens (covering all of majority side) before clean partition: {A, B, C} | {(D, E), (F, G)} => {}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)
      // trouble when indirectly connected happens before clean partition
      indirectlyConnected = List(memberD -> memberE, memberG -> memberF)

      // from side1 of the partition, minority
      // D and G are observers and marked E and F as unreachable
      // D, E, F, G unreachable due to the partition
      //
      // The records D->E, G->F are not removed in the second decision because they are not detected via seenB
      // due to clean partition.
      // For the third decision, D->E and G->F are removed because E and F are known to be unreachable by A by
      // the clean partition
      assertDowningSide(side1, Set(memberA, memberB, memberC, memberD, memberG))

      // from side2 of the partition, "majority"
      // A, B, C are on minority side
      // E, F are also unreachable
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberD, memberE, memberF, memberG))
    }

    "retain majority when indirectly connected happens (minority side) before clean partition { (A, B), C } | { D, E, F, G } => { D, E, F, G}" in new Setup2(
      role = None) {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)
      indirectlyConnected = List(memberA -> memberB)

      // from side 1 (minority)
      // A observed B unreachable before partition
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // from side 2 (majority)
      // A observed B unreachable before partition
      // A, B, C unreachable due to partition
      // A is indirectly connected (A observed and is observed unreachable)
      //
      // A->B not removed for second decision because B is not indirectly connected => A still indirectly connected
      // Since B is observed unreachable from a directly connected node, decide as if we hadn't seen the A->B observation
      // side2 is a reachable majority, so DownUnreachable
      assertDowningSide(side2, Set(memberA, memberB, memberC))
    }

    "double DownIndirectlyConnected when indirectly connected happens (large majority side) before clean partition { A } | { (C, E, G), B, D, F, H } => {}" in new Setup2(
      role = None) {
      side1 = Set(memberA)
      side2 = Set(memberB, memberC, memberD, memberE, memberF, memberG, memberH)
      indirectlyConnected = List(memberE -> memberG, memberG -> memberC)

      assertDowningSide(side1, Set(memberA, memberE, memberG))

      // assertDowningSide assumes that all on the majority side have seen latest gossip, which removes the indirectly
      // connnected when doing the additional decision
      {
        val strategy = {
          val s = createStrategy()
          (side1 ++ side2).foreach(s.add)
          s
        }
        val unreachability = (indirectlyConnected ++ side1.map(o => side2.head -> o)).toSet.toList
        val r = createReachability(unreachability)
        strategy.setReachability(r)

        unreachability.foreach { case (_, to) => strategy.addUnreachable(to) }
        // let's say that neither C nor G is has seen gossip
        strategy.setSeenBy(side2.filterNot(m => (m eq memberC) || (m eq memberG)).map(_.address))

        an[IllegalStateException] shouldBe thrownBy {
          strategy.nodesToDown() should be((side1 ++ side2).map(_.uniqueAddress))
        }
      }
    }
  }

  "KeepOldest" must {
    class Setup2(downIfAlone: Boolean = true, role: Option[String] = None) extends StrategySetup {
      override def createStrategy() = new KeepOldest(selfDc, downIfAlone, role, selfUniqueAddress)
    }

    "keep partition with oldest" in new Setup2 {
      // E is the oldest
      side1 = Set(memberA, memberE)
      side2 = Set(memberB, memberC, memberD)
      assertDowning(side2)
    }

    "keep partition with oldest with role" in new Setup2(role = Some("role2")) {
      // C and D have role2, D is the oldest
      side1 = Set(memberA, memberE)
      side2 = Set(memberB, memberC, memberD)
      assertDowning(side1)
    }

    "keep partition with oldest unless alone" in new Setup2(downIfAlone = true) {
      side1 = Set(memberE)
      side2 = Set(memberA, memberB, memberC, memberD)
      assertDowning(side1)
    }

    "keep partition with oldest in two nodes cluster" in new Setup2 {
      side1 = Set(memberB)
      side2 = Set(memberA)
      assertDowning(side2)
    }

    "keep one single oldest" in new Setup2 {
      side1 = Set.empty
      side2 = Set(memberA)
      assertDowning(side1)
    }

    "keep oldest even when alone when downIfAlone = false" in new Setup2(downIfAlone = false) {
      side1 = Set(memberE)
      side2 = Set(memberA, memberB, memberC, memberD)
      assertDowning(side2)
    }

    "detect leaving/exiting edge case: keep partition with oldest, scenario 1" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB, memberD)
        side2 = Set(memberC, exiting(memberE))
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      // E is Exiting so not counted as oldest, D is oldest
      decision1 should ===(DownUnreachable)
      // side2 is downed, but E is already exiting and therefore not downed
      strategy1.nodesToDown(decision1) should ===(side2Nodes - memberE.uniqueAddress)

      // E was changed to Exiting by side1 but that is not seen on side2 due to the partition, so still Leaving
      side2 = Set(memberC, leaving(memberE))
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()

      decision2 should ===(DownAll)
      strategy2.nodesToDown(decision2) should ===(side1Nodes.union(side2Nodes))
    }

    "detect leaving/exiting edge case: keep partition with oldest, scenario 2" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, leaving(memberE))
        side2 = Set(memberB, memberC, memberD)
      }
      import setup._
      strategy(side1).decide() should ===(DownAll)
      strategy(side2).decide() should ===(DownReachable)
    }

    "detect leaving/exiting edge case: keep partition with oldest, scenario 3" in new Setup2 {
      // E is the oldest
      side1 = Set(memberA, memberE)
      side2 = Set(leaving(memberB), leaving(memberC), memberD)
      assertDowning(side2)
    }

    "detect leaving/exiting edge case: keep partition with oldest unless alone, scenario 1" in {
      val setup = new Setup2(role = None) {
        side1 = Set(leaving(memberD), memberE)
        side2 = Set(memberA, memberB, memberC)
      }
      import setup._
      strategy(side1).decide() should ===(DownAll)
      strategy(side2).decide() should ===(DownReachable)
    }

    "detect leaving/exiting edge case: keep partition with oldest unless alone, scenario 4" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberE)
        side2 = Set(memberA, memberB, leaving(memberC))
      }
      import setup._
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownUnreachable)
    }

    "detect leaving/exiting edge case: keep partition with oldest unless alone, scenario 3" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberE)
        side2 = Set(memberA, leaving(memberB), leaving(memberC), leaving(memberD))
      }
      import setup._
      strategy(side1).decide() should ===(DownReachable)
      strategy(side2).decide() should ===(DownAll)
    }

    "detect leaving/exiting edge case: DownReachable on both sides when oldest leaving/exiting is alone" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberD, exiting(memberE))
        side2 = Set(memberA, memberB, memberC)
      }
      import setup._
      // E is Exiting so not counted as oldest, D is oldest, but it's alone so keep side2 anyway
      strategy(side1).decide() should ===(DownReachable)

      // E was changed to Exiting by side1 but that is not seen on side2 due to the partition, so still Leaving
      side1 = Set(memberD, leaving(memberE))
      strategy(side2).decide() should ===(DownReachable)
    }

    "detect leaving/exiting edge case: when one single oldest" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA)
        side2 = Set(exiting(memberB))
      }
      import setup._
      // B is Exiting so not counted as oldest, A is oldest
      strategy(side1).decide() should ===(DownUnreachable)

      // B was changed to Exiting by side1 but that is not seen on side2 due to the partition, so still Leaving
      side2 = Set(leaving(memberB))
      strategy(side2).decide() should ===(DownAll)
    }

    "detect joining/up edge case: keep partition with oldest unless alone, scenario 1" in {
      val setup = new Setup2(role = None) {
        side1 = Set(joining(memberA), memberE)
        side2 = Set(memberB, memberC, memberD)
      }
      import setup._
      // E alone when not counting joining A
      strategy(side1).decide() should ===(DownReachable)
      // but A could have been up on other side1 and therefore side2 has to down all
      strategy(side2).decide() should ===(DownAll)
    }

    "detect joining/up edge case: keep oldest even when alone when downIfAlone = false" in {
      val setup = new Setup2(downIfAlone = false) {
        side1 = Set(joining(memberA), memberE)
        side2 = Set(memberB, memberC, memberD)
      }
      import setup._
      // joining A shouldn't matter when downIfAlone = false
      strategy(side1).decide() should ===(DownUnreachable)
      strategy(side2).decide() should ===(DownReachable)
    }

    "down indirectly connected: {(A, B), C} => {C}" in new Setup2 {
      side1 = Set(memberA, memberB, memberC)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      assertDowning(Set(memberA, memberB))
    }

    "down indirectly connected on younger side, when combined with clean partition: {A, (B, C)} | {D, E, F, G} => {D, E, F, G}" in new Setup2 {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)

      // from side1 of the partition, younger
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // from side2 of the partition, oldest
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      assertDowningSide(side2, Set(memberA, memberB, memberC))

      // alternative scenario from side2 of the partition
      // indirectly connected on side1 happens before the clean partition
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)
      assertDowningSide(side2, Set(memberA, memberB, memberC))
    }

    "down indirectly connected on oldest side, when combined with clean partition: {A, B, C} | {(D, E), F, G} => {F, G}" in new Setup2 {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE, memberF, memberG)

      // from side1 of the partition, younger
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      assertDowningSide(side1, Set(memberA, memberB, memberC))

      // alternative scenario from side1 of the partition
      // indirectly connected on side2 happens before the clean partition
      indirectlyConnected = List(memberD -> memberE, memberE -> memberD)
      // note that indirectly connected memberD and memberE are also downed
      assertDowningSide(side1, Set(memberA, memberB, memberC, memberD, memberE))

      // from side2 of the partition, oldest
      indirectlyConnected = List(memberD -> memberE, memberE -> memberD)
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberD, memberE))
    }

  }

  "DownAllNodes" must {
    class Setup2 extends StrategySetup {
      override def createStrategy() = new DownAllNodes(selfDc, selfUniqueAddress)
    }

    "down all" in new Setup2 {
      side1 = Set(memberA, memberB, memberC)
      side2 = Set(memberD, memberE)
      assertDowning(side1.union(side2))
    }

    "down all when any indirectly connected: {(A, B), C} => {}" in new Setup2 {
      side1 = Set(memberA, memberB, memberC)
      indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      assertDowning(side1)
    }
  }

  "LeaseMajority" must {
    class Setup2(role: Option[String]) extends StrategySetup {
      val testLease: TestLease = new TestLease(testLeaseSettings, extSystem)

      val acquireLeaseDelayForMinority: FiniteDuration = 2.seconds

      override def createStrategy() =
        new LeaseMajority(
          selfDc,
          role,
          testLease,
          acquireLeaseDelayForMinority,
          releaseAfter = 10.seconds,
          selfUniqueAddress)
    }

    "decide AcquireLeaseAndDownUnreachable, and DownReachable as reverse decision" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberC, memberE)
        side2 = Set(memberB, memberD)
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(AcquireLeaseAndDownUnreachable(Duration.Zero))
      strategy1.nodesToDown(decision1) should ===(side2Nodes)
      val reverseDecision1 = strategy1.reverseDecision(decision1.asInstanceOf[AcquireLeaseDecision])
      reverseDecision1 should ===(DownReachable)
      strategy1.nodesToDown(reverseDecision1) should ===(side1Nodes)

      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(AcquireLeaseAndDownUnreachable(acquireLeaseDelayForMinority))
      strategy2.nodesToDown(decision2) should ===(side1Nodes)
      val reverseDecision2 = strategy2.reverseDecision(decision2.asInstanceOf[AcquireLeaseDecision])
      reverseDecision2 should ===(DownReachable)
      strategy2.nodesToDown(reverseDecision2) should ===(side2Nodes)
    }

    "try to keep half with lowest address when equal size partition" in {
      val setup = new Setup2(role = Some("role2")) {
        side1 = Set(memberA, memberD, memberE)
        side2 = Set(memberB, memberC)
        // memberC is lowest with role2
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      // delay on side1 because memberC is lowest address with role2
      decision1 should ===(AcquireLeaseAndDownUnreachable(acquireLeaseDelayForMinority))
      strategy1.nodesToDown(decision1) should ===(side2Nodes)

      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(AcquireLeaseAndDownUnreachable(Duration.Zero))
      strategy2.nodesToDown(decision2) should ===(side1Nodes)
    }

    "down indirectly connected: {(A, B), C} => {C}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB, memberC)
        indirectlyConnected = List(memberA -> memberB, memberB -> memberA)
      }
      import setup._
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(AcquireLeaseAndDownIndirectlyConnected(Duration.Zero))
      strategy1.nodesToDown(decision1) should ===(Set(memberA.uniqueAddress, memberB.uniqueAddress))
      val reverseDecision1 = strategy1.reverseDecision(decision1.asInstanceOf[AcquireLeaseDecision])
      reverseDecision1 should ===(ReverseDownIndirectlyConnected)
      strategy1.nodesToDown(reverseDecision1) should ===(side1Nodes)
    }

    "down indirectly connected when combined with clean partition: {A, (B, C)} | {D, E} => {A}" in {
      val setup = new Setup2(role = None) {
        side1 = Set(memberA, memberB, memberC)
        side2 = Set(memberD, memberE)
        indirectlyConnected = List(memberB -> memberC, memberC -> memberB)
      }
      import setup._

      // from side1 of the partition
      // keep fully connected memberA
      // note that memberD and memberE on the other side of the partition are also downed
      val strategy1 = strategy(side1)
      val decision1 = strategy1.decide()
      decision1 should ===(AcquireLeaseAndDownIndirectlyConnected(Duration.Zero))
      strategy1.nodesToDown(decision1) should ===(Set(memberB, memberC, memberD, memberE).map(_.uniqueAddress))
      val reverseDecision1 = strategy1.reverseDecision(decision1.asInstanceOf[AcquireLeaseDecision])
      reverseDecision1 should ===(ReverseDownIndirectlyConnected)
      strategy1.nodesToDown(reverseDecision1) should ===(side1Nodes)

      // from side2 of the partition
      // indirectly connected not seen from this side, if clean partition happened first
      indirectlyConnected = Nil
      // Note that memberC is not downed, as on the other side, because those indirectly connected
      // not seen from this side. That outcome is OK.
      val strategy2 = strategy(side2)
      val decision2 = strategy2.decide()
      decision2 should ===(AcquireLeaseAndDownUnreachable(acquireLeaseDelayForMinority))
      strategy2.nodesToDown(decision2) should ===(side1Nodes)
      val reverseDecision2 = strategy2.reverseDecision(decision2.asInstanceOf[AcquireLeaseDecision])
      reverseDecision2 should ===(DownReachable)
      strategy2.nodesToDown(reverseDecision2) should ===(side2Nodes)

      // alternative scenario from side2 of the partition
      // indirectly connected on side1 happens before the clean partition
      indirectlyConnected = List(memberB -> memberC, memberC -> memberB)
      val strategy3 = strategy(side2)
      val decision3 = strategy3.decide()
      decision3 should ===(AcquireLeaseAndDownIndirectlyConnected(Duration.Zero))
      strategy3.nodesToDown(decision3) should ===(side1Nodes)
      val reverseDecision3 = strategy3.reverseDecision(decision3.asInstanceOf[AcquireLeaseDecision])
      reverseDecision3 should ===(ReverseDownIndirectlyConnected)
      strategy3.nodesToDown(reverseDecision3) should ===(Set(memberB, memberC, memberD, memberE).map(_.uniqueAddress))
    }

    "down indirectly connected to already downed node during partition: {A, B, C, D} | {(E, F)} => {A, B, C, D}" in new Setup2(
      role = None) {
      val memberELeaving = leaving(memberE)
      val memberFDown = downed(memberF)
      side1 = Set(memberA, memberB, memberC, memberD)
      side2 = Set(memberELeaving, memberFDown)
      // trouble when indirectly connected happens before clean partition
      indirectlyConnected = List(memberELeaving -> memberFDown)

      // from side1 of the partition, majority
      assertDowningSide(side1, Set(memberELeaving))

      // from side2 of the partition, minority
      assertDowningSide(side2, Set(memberA, memberB, memberC, memberD, memberELeaving))
    }
  }

  "Strategy" must {

    class MajoritySetup(role: Option[String] = None) extends StrategySetup {
      override def createStrategy() = new KeepMajority(selfDc, role, selfUniqueAddress)
    }

    class OldestSetup(role: Option[String] = None) extends StrategySetup {
      override def createStrategy() = new KeepOldest(selfDc, downIfAlone = true, role, selfUniqueAddress)
    }

    "add and remove members with default Member ordering" in {
      val setup = new MajoritySetup(role = None) {
        side1 = Set.empty
        side2 = Set.empty
      }
      import setup._
      val strategy1 = strategy(side1)
      testAddRemove(strategy1)
    }

    "add and remove members with oldest Member ordering" in {
      val setup = new OldestSetup(role = None) {
        side1 = Set.empty
        side2 = Set.empty
      }
      testAddRemove(setup.strategy(setup.side1))
    }

    def testAddRemove(strategy: DowningStrategy) = {
      strategy.add(joining(memberA))
      strategy.add(joining(memberB))
      strategy.allMembersInDC.size should ===(2)
      strategy.allMembersInDC.foreach {
        _.status should ===(MemberStatus.Joining)
      }
      strategy.add(memberA)
      strategy.add(memberB)
      strategy.allMembersInDC.size should ===(2)
      strategy.allMembersInDC.foreach {
        _.status should ===(MemberStatus.Up)
      }
      strategy.add(leaving(memberB))
      strategy.allMembersInDC.size should ===(2)
      strategy.allMembersInDC.toList.map(_.status).toSet should ===(Set(MemberStatus.Up, MemberStatus.Leaving))
      strategy.add(exiting(memberB))
      strategy.allMembersInDC.size should ===(2)
      strategy.allMembersInDC.toList.map(_.status).toSet should ===(Set(MemberStatus.Up, MemberStatus.Exiting))
      strategy.remove(memberA)
      strategy.allMembersInDC.size should ===(1)
      strategy.allMembersInDC.head.status should ===(MemberStatus.Exiting)
    }

    "collect and filter members with default Member ordering" in {
      val setup = new MajoritySetup(role = None) {
        side1 = Set.empty
        side2 = Set.empty
      }

      testCollectAndFilter(setup)
    }

    "collect and filter members with oldest Member ordering" in {
      val setup = new OldestSetup(role = None) {
        side1 = Set.empty
        side2 = Set.empty
      }

      testCollectAndFilter(setup)
    }

    def testCollectAndFilter(setup: StrategySetup): Unit = {
      import setup._

      side1 = Set(memberAWeaklyUp, memberB, joining(memberC))
      side2 = Set(memberD, leaving(memberE), downed(memberF), exiting(memberG))

      val strategy1 = strategy(side1)

      strategy1.membersWithRole should ===(Set(memberB, memberD, leaving(memberE)))
      strategy1.membersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC), memberD, leaving(memberE)))
      strategy1.membersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(
        Set(memberB, memberD))
      strategy1.membersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC), memberD))

      strategy1.reachableMembersWithRole should ===(Set(memberB))
      strategy1.reachableMembersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC)))
      strategy1.reachableMembersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(
        Set(memberB))
      strategy1.reachableMembersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC)))

      strategy1.unreachableMembersWithRole should ===(Set(memberD, leaving(memberE)))
      strategy1.unreachableMembers(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberD, leaving(memberE)))
      strategy1.unreachableMembers(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(
        Set(memberD))
      strategy1.unreachableMembers(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(Set(memberD))

      strategy1.unreachable(memberAWeaklyUp) should ===(false)
      strategy1.unreachable(memberB) should ===(false)
      strategy1.unreachable(memberD) should ===(true)
      strategy1.unreachable(leaving(memberE)) should ===(true)
      strategy1.unreachable(downed(memberF)) should ===(true)
      strategy1.joining should ===(Set(memberAWeaklyUp, joining(memberC)))

      val strategy2 = strategy(side2)

      strategy2.membersWithRole should ===(Set(memberB, memberD, leaving(memberE)))
      strategy2.membersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC), memberD, leaving(memberE)))
      strategy2.membersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(
        Set(memberB, memberD))
      strategy2.membersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC), memberD))

      strategy2.unreachableMembersWithRole should ===(Set(memberB))
      strategy2.unreachableMembersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC)))
      strategy2.unreachableMembersWithRole(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(
        Set(memberB))
      strategy2.unreachableMembersWithRole(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(
        Set(memberAWeaklyUp, memberB, joining(memberC)))

      strategy2.reachableMembersWithRole should ===(Set(memberD, leaving(memberE)))
      strategy2.reachableMembers(includingPossiblyUp = true, excludingPossiblyExiting = false) should ===(
        Set(memberD, leaving(memberE)))
      strategy2.reachableMembers(includingPossiblyUp = false, excludingPossiblyExiting = true) should ===(Set(memberD))
      strategy2.reachableMembers(includingPossiblyUp = true, excludingPossiblyExiting = true) should ===(Set(memberD))

      strategy2.unreachable(memberAWeaklyUp) should ===(true)
      strategy2.unreachable(memberB) should ===(true)
      strategy2.unreachable(memberD) should ===(false)
      strategy2.unreachable(leaving(memberE)) should ===(false)
      strategy2.unreachable(downed(memberF)) should ===(false)
      strategy2.joining should ===(Set(memberAWeaklyUp, joining(memberC)))
    }
  }

  "Split Brain Resolver" must {

    class SetupKeepMajority(
        stableAfter: FiniteDuration,
        selfUniqueAddress: UniqueAddress,
        role: Option[String],
        downAllWhenUnstable: FiniteDuration = Duration.Zero,
        tickInterval: FiniteDuration = Duration.Zero)
        extends Setup(
          stableAfter,
          new KeepMajority(selfDc, role, selfUniqueAddress),
          selfUniqueAddress,
          downAllWhenUnstable,
          tickInterval)

    class SetupKeepOldest(
        stableAfter: FiniteDuration,
        selfUniqueAddress: UniqueAddress,
        downIfAlone: Boolean,
        role: Option[String])
        extends Setup(stableAfter, new KeepOldest(selfDc, downIfAlone, role, selfUniqueAddress), selfUniqueAddress)

    class SetupStaticQuorum(
        stableAfter: FiniteDuration,
        selfUniqueAddress: UniqueAddress,
        size: Int,
        role: Option[String])
        extends Setup(stableAfter, new StaticQuorum(selfDc, size, role, selfUniqueAddress), selfUniqueAddress)

    class SetupDownAllNodes(stableAfter: FiniteDuration, selfUniqueAddress: UniqueAddress)
        extends Setup(stableAfter, new DownAllNodes(selfDc, selfUniqueAddress), selfUniqueAddress)

    class SetupLeaseMajority(
        stableAfter: FiniteDuration,
        selfUniqueAddress: UniqueAddress,
        role: Option[String],
        val testLease: TestLease,
        downAllWhenUnstable: FiniteDuration = Duration.Zero,
        tickInterval: FiniteDuration = Duration.Zero)
        extends Setup(
          stableAfter,
          new LeaseMajority(
            selfDc,
            role,
            testLease,
            acquireLeaseDelayForMinority = 20.millis,
            releaseAfter = 10.seconds,
            selfUniqueAddress),
          selfUniqueAddress,
          downAllWhenUnstable,
          tickInterval)

    abstract class Setup(
        stableAfter: FiniteDuration,
        strategy: DowningStrategy,
        selfUniqueAddress: UniqueAddress,
        downAllWhenUnstable: FiniteDuration = Duration.Zero,
        tickInterval: FiniteDuration = Duration.Zero) {

      val a = system.actorOf(
        DowningTestActor
          .props(stableAfter, strategy, testActor, selfUniqueAddress, selfDc, downAllWhenUnstable, tickInterval))

      def memberUp(members: Member*): Unit =
        members.foreach(m => a ! MemberUp(m))

      def memberWeaklyUp(members: Member*): Unit =
        members.foreach(m => a ! MemberWeaklyUp(m))

      def leader(member: Member): Unit =
        a ! LeaderChanged(Some(member.address))

      def unreachable(members: Member*): Unit =
        members.foreach(m => a ! UnreachableMember(m))

      def reachabilityChanged(unreachability: (Member, Member)*): Unit = {
        unreachable(unreachability.map { case (_, to) => to }: _*)

        val r = createReachability(unreachability)
        a ! ReachabilityChanged(r)
      }

      def remove(members: Member*): Unit =
        members.foreach(m => a ! MemberRemoved(m.copy(Removed), previousStatus = Exiting))

      def dcUnreachable(members: Member*): Unit =
        members.map(_.dataCenter).toSet[DataCenter].foreach(dc => a ! UnreachableDataCenter(dc))

      def dcReachable(members: Member*): Unit =
        members.map(_.dataCenter).toSet[DataCenter].foreach(dc => a ! ReachableDataCenter(dc))

      def reachable(members: Member*): Unit =
        members.foreach(m => a ! ReachableMember(m))

      def tick(): Unit = a ! SplitBrainResolver.Tick

      def expectDownCalled(members: Member*): Unit =
        receiveN(members.length).toSet should be(members.map(m => DownCalled(m.address)).toSet)

      def expectNoDecision(max: FiniteDuration): Unit =
        expectNoMessage(max)

      def stop(): Unit = {
        system.stop(a)
        expectNoMessage(100.millis)
      }
    }

    "have downRemovalMargin equal to stable-after" in {
      val cluster = Cluster(system)
      val sbrSettings = new SplitBrainResolverSettings(system.settings.config)
      cluster.downingProvider.downRemovalMargin should be(sbrSettings.DowningStableAfter)
    }

    "down unreachable when leader" in new SetupKeepMajority(Duration.Zero, memberA.uniqueAddress, role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      tick()
      expectDownCalled(memberB)
      stop()
    }

    "not down unreachable when not leader" in new SetupKeepMajority(Duration.Zero, memberB.uniqueAddress, role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberC)
      tick()
      expectNoMessage(500.millis)
      stop()
    }

    "down unreachable when becoming leader" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberB)
      unreachable(memberC)
      leader(memberA)
      tick()
      expectDownCalled(memberC)
      stop()
    }

    "down unreachable after specified duration" in new SetupKeepMajority(
      stableAfter = 2.seconds,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      expectNoMessage(1.second)
      expectDownCalled(memberB)
      stop()
    }

    "down unreachable when becoming leader inbetween detection and specified duration" in new SetupKeepMajority(
      stableAfter = 2.seconds,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberB)
      unreachable(memberC)
      leader(memberA)
      tick()
      expectNoMessage(1.second)
      expectDownCalled(memberC)
      stop()
    }

    "not down unreachable when loosing leadership inbetween detection and specified duration" in new SetupKeepMajority(
      stableAfter = 1.seconds,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberC)
      leader(memberB)
      tick()
      expectNoMessage(1500.millis)
      stop()
    }

    // reproducer of issue #436
    "down when becoming Weakly-Up leader" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberAWeaklyUp.uniqueAddress,
      role = None) {
      memberUp(memberC)
      memberWeaklyUp(memberAWeaklyUp, memberBWeaklyUp)
      unreachable(memberC)
      leader(memberAWeaklyUp)
      tick()
      expectDownCalled(memberAWeaklyUp, memberBWeaklyUp)
      stop()
    }

    "not down when unreachable become reachable inbetween detection and specified duration" in new SetupKeepMajority(
      stableAfter = 1.seconds,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      reachable(memberB)
      tick()
      expectNoMessage(1500.millis)
      stop()
    }

    "not down when unreachable is removed inbetween detection and specified duration" in new SetupKeepMajority(
      stableAfter = 1.seconds,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      a ! MemberRemoved(memberB.copy(Removed), previousStatus = Exiting)
      tick()
      expectNoMessage(1500.millis)
      stop()
    }

    "not down when unreachable is already Down" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB.copy(Down))
      tick()
      expectNoMessage(1000.millis)
      stop()
    }

    "down minority partition" in new SetupKeepMajority(stableAfter = Duration.Zero, memberA.uniqueAddress, role = None) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberC -> memberD)
      tick()
      expectDownCalled(memberB, memberD)
      stop()
    }

    "keep partition with oldest" in new SetupKeepOldest(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      downIfAlone = true,
      role = None) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberA -> memberC, memberE -> memberD)
      tick()
      expectDownCalled(memberB, memberC, memberD)
      stop()
    }

    "log warning if N > static-quorum.size * 2 - 1" in new SetupStaticQuorum(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      size = 2,
      role = None) {
      EventFilter.warning(pattern = "cluster size is \\[4\\].*not add more than \\[3\\]", occurrences = 1).intercept {
        memberUp(memberA, memberB, memberC, memberD)
      }
      leader(memberA)
      unreachable(memberC, memberD)
      tick()
      // down all
      expectDownCalled(memberA, memberB, memberC, memberD)
      stop()
    }

    "not care about partition across data centers" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(dataCenter(selfDc, memberA, memberB, memberC).toList: _*)
      memberUp(dataCenter("other", memberD, memberE).toList: _*)
      leader(memberA)
      unreachable(memberB)
      dcUnreachable(dataCenter("other", memberD, memberE).toList: _*)
      tick()
      expectDownCalled(memberB)
      stop()
    }

    "not count members from other data centers" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(dataCenter(selfDc, memberA, memberB, memberC).toList: _*)
      memberUp(dataCenter("other", memberD, memberE).toList: _*)
      leader(memberA)
      unreachable(memberB, memberC)
      tick()
      // if memberD and memberE would be counted then memberA would be in majority side
      expectDownCalled(memberA)
      stop()
    }

    "keep oldest in self data centers" in new SetupKeepOldest(
      // note that this is now on B
      stableAfter = Duration.Zero,
      memberB.uniqueAddress,
      downIfAlone = true,
      role = None) {
      memberUp(dataCenter(selfDc, memberA, memberB, memberC).toList: _*)
      // D and E have lower upNumber (older), but not in self DC
      memberUp(dataCenter("other", memberD, memberE).toList: _*)
      leader(memberB)
      unreachable(memberA, memberC)
      tick()
      // if memberD and memberE would be counted then memberC would not oldest
      // C is oldest in selfDc, so keep C and B and down self
      expectDownCalled(memberB)
      stop()
    }

    "log warning for data center unreachability" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(dataCenter(selfDc, memberA, memberB, memberC).toList: _*)
      memberUp(dataCenter("other", memberD, memberE).toList: _*)
      leader(memberA)
      EventFilter.warning(start = "Data center [other] observed as unreachable", occurrences = 1).intercept {
        dcUnreachable(dataCenter("other", memberD, memberE).toList: _*)
      }
      stop()
    }

    "down indirectly connected: {(A, B), C} => {C}" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberB -> memberA)
      tick()
      // keep fully connected memberC
      expectDownCalled(memberA, memberB)
      stop()
    }

    "down indirectly connected when combined with crashed: {(A, B), D, E} | {C} => {D, E}" in new SetupKeepMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberB -> memberA, memberB -> memberC)
      tick()
      // keep fully connected memberD, memberE
      // note that crashed memberC is also downed
      expectDownCalled(memberA, memberB, memberC)
      stop()
    }

    "down indirectly connected when combined with clean partition: {A, (B, C)} | {D, E} => {A}" in {
      // from left side of the partition, memberA, memberB, memberC
      new SetupKeepMajority(stableAfter = Duration.Zero, memberA.uniqueAddress, role = None) {
        memberUp(memberA, memberB, memberC, memberD, memberE)
        leader(memberA)
        // indirectly connected: memberB, memberC
        // clean partition: memberA, memberB, memberC | memeberD, memberE
        reachabilityChanged(
          memberB -> memberC,
          memberC -> memberB,
          memberA -> memberD,
          memberB -> memberD,
          memberB -> memberE,
          memberC -> memberE)
        tick()
        // keep fully connected memberA
        // note that memberD and memberE on the other side of the partition are also downed
        expectDownCalled(memberB, memberC, memberD, memberE)
        stop()
      }

      // from right side of the partition, memberD, memberE
      new SetupKeepMajority(stableAfter = Duration.Zero, memberD.uniqueAddress, role = None) {
        memberUp(memberA, memberB, memberC, memberD, memberE)
        leader(memberD)
        // indirectly connected not seen from this side
        // clean partition: memberA, memberB, memberC | memeberD, memberE
        reachabilityChanged(memberD -> memberA, memberD -> memberB, memberE -> memberB, memberE -> memberC)
        tick()
        // Note that memberC is not downed, as on the other side, because those indirectly connected
        // not seen from this side. That outcome is OK.
        expectDownCalled(memberD, memberE)
        stop()
      }

    }

    "down indirectly connected when combined with partition and exiting: {A, B, C, D} | {E, F-exiting} => {A, B, C, D}" in {
      new SetupKeepMajority(stableAfter = Duration.Zero, memberA.uniqueAddress, role = None) {
        memberUp(memberA, memberB, memberC, memberD, memberE, memberF)
        val memberFExiting = exiting(memberF)
        a ! MemberExited(memberFExiting)
        leader(memberA)
        // indirectly connected: memberF
        // partition: memberA, memberB, memberC, memberD | memberE, memberF
        reachabilityChanged(
          memberA -> memberE,
          memberA -> memberFExiting,
          memberB -> memberE,
          memberB -> memberFExiting,
          memberC -> memberE,
          memberC -> memberFExiting,
          memberD -> memberE,
          memberD -> memberFExiting,
          memberE -> memberFExiting)
        tick()
        // keep fully connected members
        expectDownCalled(memberE)
        stop()
      }
    }

    "down indirectly connected when combined with partition and exiting: {A, B, C, D} | {E-exiting, F} => {A, B, C, D}" in {
      new SetupKeepMajority(stableAfter = Duration.Zero, memberA.uniqueAddress, role = None) {
        memberUp(memberA, memberB, memberC, memberD, memberE, memberF)
        val memberEExiting = exiting(memberE)
        a ! MemberExited(memberEExiting)
        leader(memberA)
        // indirectly connected: memberF
        // partition: memberA, memberB, memberC, memberD | memberE, memberF
        reachabilityChanged(
          memberA -> memberEExiting,
          memberA -> memberF,
          memberB -> memberEExiting,
          memberB -> memberF,
          memberC -> memberEExiting,
          memberC -> memberF,
          memberD -> memberEExiting,
          memberD -> memberF,
          memberE -> memberF)
        tick()
        // keep fully connected members
        expectDownCalled(memberF)
        stop()
      }
    }

    "down all in self data centers" in new SetupDownAllNodes(stableAfter = Duration.Zero, memberA.uniqueAddress) {
      memberUp(dataCenter(selfDc, memberA, memberB, memberC).toList: _*)
      // D and E not in self DC
      memberUp(dataCenter("other", memberD, memberE).toList: _*)
      leader(memberA)
      unreachable(memberA, memberC)
      tick()
      expectDownCalled(memberA, memberB, memberC)
      stop()
    }

    "down all when unstable, scenario 1" in new SetupKeepMajority(
      stableAfter = 2.seconds,
      downAllWhenUnstable = 1.second,
      selfUniqueAddress = memberA.uniqueAddress,
      role = None,
      tickInterval = 100.seconds) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      reachabilityChanged(memberB -> memberD, memberB -> memberE)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(1000)
      reachabilityChanged(memberB -> memberD)
      reachable(memberE)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(1000)
      reachabilityChanged(memberB -> memberD, memberB -> memberE)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(1000)
      reachabilityChanged(memberB -> memberD)
      reachable(memberE)
      tick()
      expectDownCalled(memberA, memberB, memberC, memberD, memberE)
    }

    "down all when unstable, scenario 2" in new SetupKeepMajority(
      stableAfter = 2.seconds,
      downAllWhenUnstable = 500.millis,
      selfUniqueAddress = memberA.uniqueAddress,
      role = None,
      tickInterval = 100.seconds) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      // E and D are unreachable
      reachabilityChanged(memberA -> memberE, memberB -> memberD, memberC -> memberD)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(500)
      // E and D are still unreachable
      reachabilityChanged(memberA -> memberE, memberB -> memberD)
      tick()
      expectNoDecision(100.millis)
      // 600 ms has elapsed

      Thread.sleep(500)
      reachabilityChanged(memberA -> memberE)
      reachable(memberD) // reset stableDeadline
      tick()
      expectNoDecision(100.millis)
      // 1200 ms has elapsed

      Thread.sleep(500)
      // E and D are unreachable, reset stableDeadline
      reachabilityChanged(memberA -> memberE, memberB -> memberD, memberC -> memberD)
      tick()
      expectNoDecision(100.millis)
      // 1800 ms has elapsed

      Thread.sleep(1000)
      // E and D are still unreachable
      reachabilityChanged(memberA -> memberE, memberB -> memberD)
      tick()
      // 2800 ms has elapsed and still no stability so downing all
      expectDownCalled(memberA, memberB, memberC, memberD, memberE)
    }

    "not down all when becoming stable again" in new SetupKeepMajority(
      stableAfter = 2.seconds,
      downAllWhenUnstable = 1.second,
      selfUniqueAddress = memberA.uniqueAddress,
      role = None,
      tickInterval = 100.seconds) {
      memberUp(memberA, memberB, memberC, memberD, memberE)
      leader(memberA)
      reachabilityChanged(memberB -> memberD, memberB -> memberE)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(1000)
      reachabilityChanged(memberB -> memberD)
      reachable(memberE)
      tick()
      expectNoDecision(100.millis)

      // wait longer than stableAfter
      Thread.sleep(500)
      tick()
      expectNoDecision(100.millis)
      reachabilityChanged()
      reachable(memberD)
      Thread.sleep(500)
      tick()
      expectNoDecision(100.millis)

      Thread.sleep(3000)
      tick()
      expectNoDecision(100.millis)
    }

    "down other side when lease can be acquired" in new SetupLeaseMajority(
      Duration.Zero,
      memberA.uniqueAddress,
      role = None,
      new TestLease(testLeaseSettings, extSystem)) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      testLease.setNextAcquireResult(Future.successful(true))
      tick()
      expectDownCalled(memberB)
      stop()
    }

    "down own side when lease cannot be acquired" in new SetupLeaseMajority(
      Duration.Zero,
      memberA.uniqueAddress,
      role = None,
      new TestLease(testLeaseSettings, extSystem)) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      unreachable(memberB)
      testLease.setNextAcquireResult(Future.successful(false))
      tick()
      expectDownCalled(memberA, memberC)
      stop()
    }

    "down indirectly connected when lease can be acquired: {(A, B), C} => {C}" in new SetupLeaseMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None,
      new TestLease(testLeaseSettings, extSystem)) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberB -> memberA)
      testLease.setNextAcquireResult(Future.successful(true))
      tick()
      // keep fully connected memberC
      expectDownCalled(memberA, memberB)
      stop()
    }

    "down indirectly connected when lease cannot be acquired: {(A, B), C} => {C}" in new SetupLeaseMajority(
      stableAfter = Duration.Zero,
      memberA.uniqueAddress,
      role = None,
      new TestLease(testLeaseSettings, extSystem)) {
      memberUp(memberA, memberB, memberC)
      leader(memberA)
      reachabilityChanged(memberA -> memberB, memberB -> memberA)
      testLease.setNextAcquireResult(Future.successful(false))
      tick()
      // all reachable + all indirectly connected
      expectDownCalled(memberA, memberB, memberC)
      stop()
    }

  }

  "Split Brain Resolver downing provider" must {

    "be loadable through the cluster extension" in {
      Cluster(system).downingProvider shouldBe a[SplitBrainResolverProvider]
    }
  }

}
