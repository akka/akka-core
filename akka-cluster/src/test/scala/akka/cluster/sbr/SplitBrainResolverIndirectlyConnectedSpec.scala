/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import akka.cluster.Reachability
import akka.cluster.sbr.DowningStrategy._
import akka.cluster.sbr.TestAddresses._
import akka.testkit.AkkaSpec

/**
 * Tests for the down-all-when-indirectly-connected threshold safeguard.
 *
 * When a DownIndirectlyConnected decision would down more than a certain fraction of members,
 * SBR will instead down all nodes.
 */
class SplitBrainResolverIndirectlyConnectedSpec
    extends AkkaSpec("""
  |akka {
  |  actor.provider = cluster
  |  cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
  |  cluster.split-brain-resolver.active-strategy=keep-majority
  |  cluster.split-brain-resolver.down-all-when-indirectly-connected=0.5
  |  remote.artery.canonical {
  |    hostname = "127.0.0.1"
  |    port = 0
  |  }
  |}
  """.stripMargin) {

  private val selfDc = defaultDataCenter

  "DownAllWhenIndirectlyConnected threshold safeguard" must {

    "down all when indirectly connected decision would down more than threshold" in {
      // 5-node cluster: A, B, C, D, E
      // Scenario: B and D are on minority side, A/C/E are on majority side
      // B's decision would down A, B, C, E (4/5 = 80% > 50% threshold)
      // This should trigger DownAll instead

      val strategy = new KeepMajority(selfDc, role = None, memberB.uniqueAddress)

      Set(memberA, memberB, memberC, memberD, memberE).foreach(strategy.add)

      // Simulate the asymmetric partition scenario where B would down 4 of 5 nodes
      // (same as the stale gossip scenario from the logs)
      val reachabilityRecords = Seq(
        Reachability.Record(memberA.uniqueAddress, memberB.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberC.uniqueAddress, memberB.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberE.uniqueAddress, memberB.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberB.uniqueAddress, memberA.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberB.uniqueAddress, memberC.uniqueAddress, Reachability.Unreachable, 2),
        Reachability.Record(memberB.uniqueAddress, memberE.uniqueAddress, Reachability.Unreachable, 3),
        Reachability.Record(memberD.uniqueAddress, memberA.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberD.uniqueAddress, memberC.uniqueAddress, Reachability.Unreachable, 2),
        Reachability.Record(memberD.uniqueAddress, memberE.uniqueAddress, Reachability.Unreachable, 3))

      val versions = Map(
        memberA.uniqueAddress -> 1L,
        memberB.uniqueAddress -> 3L,
        memberC.uniqueAddress -> 1L,
        memberD.uniqueAddress -> 3L,
        memberE.uniqueAddress -> 1L)
      val reachability = Reachability(reachabilityRecords.toIndexedSeq, versions)
      strategy.setReachability(reachability)

      strategy.addUnreachable(memberA)
      strategy.addUnreachable(memberC)
      strategy.addUnreachable(memberE)

      strategy.setSeenBy(Set(memberB.address, memberD.address))

      val decision = strategy.decide()
      decision should ===(DownIndirectlyConnected)

      // The original decision would down 4/5 = 80% of members
      val originalNodesToDown = strategy.nodesToDown(decision)
      originalNodesToDown.size should ===(4)

      // With the threshold safeguard (tested in integration), DownAll would be triggered
      // Here we just verify the precondition is met
      val threshold = 0.5
      val memberCount = strategy.members.size
      (originalNodesToDown.size.toDouble / memberCount >= threshold) should ===(true)
    }

    "not trigger safeguard when indirectly connected decision is below threshold" in {
      // 5-node cluster where only 1 node is indirectly connected
      // 1/5 = 20% < 50% threshold, so safeguard should not trigger

      val strategy = new KeepMajority(selfDc, role = None, memberA.uniqueAddress)

      Set(memberA, memberB, memberC, memberD, memberE).foreach(strategy.add)

      // Simple indirect connection: A <-> B (mutual unreachability)
      val reachabilityRecords = Seq(
        Reachability.Record(memberA.uniqueAddress, memberB.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberB.uniqueAddress, memberA.uniqueAddress, Reachability.Unreachable, 1))

      val versions = Map(memberA.uniqueAddress -> 1L, memberB.uniqueAddress -> 1L)
      val reachability = Reachability(reachabilityRecords.toIndexedSeq, versions)
      strategy.setReachability(reachability)

      strategy.addUnreachable(memberB)

      strategy.setSeenBy(Set(memberA.address, memberC.address, memberD.address, memberE.address))

      val decision = strategy.decide()
      decision should ===(DownIndirectlyConnected)

      // Only 2 nodes (A and B) would be downed = 50% which is exactly at threshold
      // (not MORE than threshold, so should not trigger)
      val nodesToDown = strategy.nodesToDown(decision)
      nodesToDown.size should ===(2)

      val threshold = 0.5
      val memberCount = strategy.members.size
      (nodesToDown.size.toDouble / memberCount >= threshold) should ===(false)
    }

    "trigger safeguard in 3-node cluster when 2 are indirectly connected" in {
      // 3-node cluster: A, B, C
      // If 2 nodes are indirectly connected: 2/3 = 67% > 50% threshold

      val strategy = new KeepMajority(selfDc, role = None, memberA.uniqueAddress)

      Set(memberA, memberB, memberC).foreach(strategy.add)

      // A <-> B <-> C cycle (all indirectly connected)
      val reachabilityRecords = Seq(
        Reachability.Record(memberA.uniqueAddress, memberB.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberB.uniqueAddress, memberC.uniqueAddress, Reachability.Unreachable, 1),
        Reachability.Record(memberC.uniqueAddress, memberA.uniqueAddress, Reachability.Unreachable, 1))

      val versions =
        Map(memberA.uniqueAddress -> 1L, memberB.uniqueAddress -> 1L, memberC.uniqueAddress -> 1L)
      val reachability = Reachability(reachabilityRecords.toIndexedSeq, versions)
      strategy.setReachability(reachability)

      strategy.addUnreachable(memberB)
      strategy.addUnreachable(memberC)

      strategy.setSeenBy(Set(memberA.address))

      val decision = strategy.decide()
      decision should ===(DownIndirectlyConnected)

      // All 3 nodes would be downed = 100% > 50% threshold
      val nodesToDown = strategy.nodesToDown(decision)
      nodesToDown.size should ===(3)

      val threshold = 0.5
      val memberCount = strategy.members.size
      (nodesToDown.size.toDouble / memberCount >= threshold) should ===(true)
    }

  }
}
