/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import InternalClusterAction._
import MemberStatus._
import com.typesafe.config.ConfigFactory
import language.implicitConversions

import akka.actor.Address
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object TransitionMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(
        ConfigFactory.parseString("""
      akka.cluster.periodic-tasks-initial-delay = 300 s # turn off all periodic tasks
      akka.cluster.publish-stats-interval = 0 s # always, when it happens
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class TransitionMultiJvmNode1 extends TransitionSpec
class TransitionMultiJvmNode2 extends TransitionSpec
class TransitionMultiJvmNode3 extends TransitionSpec

abstract class TransitionSpec extends MultiNodeClusterSpec(TransitionMultiJvmSpec) with ImplicitSender {

  import TransitionMultiJvmSpec._

  muteMarkingAsUnreachable()

  // sorted in the order used by the cluster
  def leader(roles: RoleName*) = roles.sorted.head
  def nonLeader(roles: RoleName*) = roles.toSeq.sorted.tail

  def memberStatus(address: Address): MemberStatus = {
    val statusOption = clusterView.members.union(clusterView.unreachableMembers).collectFirst {
      case m if m.address == address => m.status
    }
    statusOption.getOrElse(Removed)
  }

  def memberAddresses: Set[Address] = clusterView.members.map(_.address)

  def members: Set[RoleName] = memberAddresses.flatMap(roleName(_))

  def seenLatestGossip: Set[RoleName] = clusterView.seenBy.flatMap(roleName)

  def awaitSeen(addresses: Address*): Unit = awaitAssert {
    (seenLatestGossip.map(address)) should ===(addresses.toSet)
  }

  def awaitMembers(addresses: Address*): Unit = awaitAssert {
    memberAddresses should ===(addresses.toSet)
  }

  def awaitMemberStatus(address: Address, status: MemberStatus): Unit = awaitAssert {
    memberStatus(address) should ===(status)
  }

  def leaderActions(): Unit =
    cluster.clusterCore ! LeaderActionsTick

  def reapUnreachable(): Unit =
    cluster.clusterCore ! ReapUnreachableTick

  // DSL sugar for `role1 gossipTo role2`
  implicit def roleExtras(role: RoleName): RoleWrapper = new RoleWrapper(role)
  var gossipBarrierCounter = 0
  class RoleWrapper(fromRole: RoleName) {
    def gossipTo(toRole: RoleName): Unit = {
      gossipBarrierCounter += 1
      runOn(toRole) {
        val oldCount = clusterView.latestStats.gossipStats.receivedGossipCount
        enterBarrier("before-gossip-" + gossipBarrierCounter)
        awaitCond {
          clusterView.latestStats.gossipStats.receivedGossipCount != oldCount // received gossip
        }
        // gossip chat will synchronize the views
        awaitCond(Set(fromRole, toRole).diff(seenLatestGossip).isEmpty)
        enterBarrier("after-gossip-" + gossipBarrierCounter)
      }
      runOn(fromRole) {
        enterBarrier("before-gossip-" + gossipBarrierCounter)
        // send gossip
        cluster.clusterCore ! InternalClusterAction.SendGossipTo(toRole)
        // gossip chat will synchronize the views
        awaitCond(Set(fromRole, toRole).diff(seenLatestGossip).isEmpty)
        enterBarrier("after-gossip-" + gossipBarrierCounter)
      }
      runOn(roles.filterNot(r => r == fromRole || r == toRole): _*) {
        enterBarrier("before-gossip-" + gossipBarrierCounter)
        enterBarrier("after-gossip-" + gossipBarrierCounter)
      }
    }
  }

  "A Cluster" must {

    "start nodes as singleton clusters" taggedAs LongRunningTest in {

      runOn(first) {
        cluster.join(myself)
        // first joining itself will immediately be moved to Up
        awaitMemberStatus(myself, Up)
        awaitCond(clusterView.isSingletonCluster)
      }

      enterBarrier("after-1")
    }

    "perform correct transitions when second joining first" taggedAs LongRunningTest in {

      runOn(second) {
        cluster.join(first)
      }
      runOn(first, second) {
        // gossip chat from the join will synchronize the views
        awaitMembers(first, second)
        awaitMemberStatus(first, Up)
        awaitMemberStatus(second, Joining)
        awaitAssert(seenLatestGossip should ===(Set(first, second)))
      }
      enterBarrier("convergence-joining-2")

      runOn(first) {
        leaderActions()
        awaitMemberStatus(first, Up)
        awaitMemberStatus(second, Up)
      }
      enterBarrier("leader-actions-2")

      first.gossipTo(second)
      runOn(first, second) {
        // gossip chat will synchronize the views
        awaitMemberStatus(second, Up)
        awaitAssert(seenLatestGossip should ===(Set(first, second)))
        awaitMemberStatus(first, Up)
      }

      enterBarrier("after-2")
    }

    "perform correct transitions when third joins second" taggedAs LongRunningTest in {

      runOn(third) {
        cluster.join(second)
      }
      runOn(second, third) {
        // gossip chat from the join will synchronize the views
        awaitAssert(seenLatestGossip should ===(Set(second, third)))
      }
      enterBarrier("third-joined-second")

      second.gossipTo(first)
      runOn(first, second) {
        // gossip chat will synchronize the views
        awaitMembers(first, second, third)
        awaitMemberStatus(third, Joining)
        awaitMemberStatus(second, Up)
        awaitAssert(seenLatestGossip should ===(Set(first, second, third)))
      }

      first.gossipTo(third)
      runOn(first, second, third) {
        awaitMembers(first, second, third)
        awaitMemberStatus(first, Up)
        awaitMemberStatus(second, Up)
        awaitMemberStatus(third, Joining)
        awaitAssert(seenLatestGossip should ===(Set(first, second, third)))
      }

      enterBarrier("convergence-joining-3")

      val leader12 = leader(first, second)
      val (other1, other2) = { val tmp = roles.filterNot(_ == leader12); (tmp.head, tmp.tail.head) }
      runOn(leader12) {
        leaderActions()
        awaitMemberStatus(first, Up)
        awaitMemberStatus(second, Up)
        awaitMemberStatus(third, Up)
      }
      enterBarrier("leader-actions-3")

      // leader gossipTo first non-leader
      leader12.gossipTo(other1)
      runOn(other1) {
        awaitMemberStatus(third, Up)
        awaitAssert(seenLatestGossip should ===(Set(leader12, myself)))
      }

      // first non-leader gossipTo the other non-leader
      other1.gossipTo(other2)
      runOn(other1) {
        // send gossip
        cluster.clusterCore ! InternalClusterAction.SendGossipTo(other2)
      }
      runOn(other2) {
        awaitMemberStatus(third, Up)
        awaitAssert(seenLatestGossip should ===(Set(first, second, third)))
      }

      // first non-leader gossipTo the leader
      other1.gossipTo(leader12)
      runOn(first, second, third) {
        awaitMemberStatus(first, Up)
        awaitMemberStatus(second, Up)
        awaitMemberStatus(third, Up)
        awaitAssert(seenLatestGossip should ===(Set(first, second, third)))
      }

      enterBarrier("after-3")
    }

    "perform correct transitions when second becomes unavailable" taggedAs LongRunningTest in {
      runOn(third) {
        markNodeAsUnavailable(second)
        reapUnreachable()
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(second)))
        awaitAssert(seenLatestGossip should ===(Set(third)))
      }

      enterBarrier("after-second-unavailable")

      third.gossipTo(first)

      runOn(first, third) {
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(second)))
      }

      runOn(first) {
        cluster.down(second)
      }

      enterBarrier("after-second-down")

      runOn(second) {
        // first should have sent immediate gossip to second when it downed second
        // and second should then shutdown
        awaitAssert(cluster.isTerminated should ===(true))
      }

      enterBarrier("second-received-down")

      first.gossipTo(third)

      runOn(first, third) {
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(second)))
        awaitMemberStatus(second, Down)
        // second will also gossip when it shuts down, so it has seen it
        awaitAssert(seenLatestGossip should ===(Set(first, second, third)))
      }

      enterBarrier("after-6")
    }

  }
}
