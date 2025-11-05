/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import language.postfixOps

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

final case class LeaderElectionMultiNodeConfig(failureDetectorPuppet: Boolean) extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfig(failureDetectorPuppet)))
}

class LeaderElectionWithFailureDetectorPuppetMultiJvmNode1 extends LeaderElectionSpec(failureDetectorPuppet = true)
class LeaderElectionWithFailureDetectorPuppetMultiJvmNode2 extends LeaderElectionSpec(failureDetectorPuppet = true)
class LeaderElectionWithFailureDetectorPuppetMultiJvmNode3 extends LeaderElectionSpec(failureDetectorPuppet = true)
class LeaderElectionWithFailureDetectorPuppetMultiJvmNode4 extends LeaderElectionSpec(failureDetectorPuppet = true)
class LeaderElectionWithFailureDetectorPuppetMultiJvmNode5 extends LeaderElectionSpec(failureDetectorPuppet = true)

class LeaderElectionWithAccrualFailureDetectorMultiJvmNode1 extends LeaderElectionSpec(failureDetectorPuppet = false)
class LeaderElectionWithAccrualFailureDetectorMultiJvmNode2 extends LeaderElectionSpec(failureDetectorPuppet = false)
class LeaderElectionWithAccrualFailureDetectorMultiJvmNode3 extends LeaderElectionSpec(failureDetectorPuppet = false)
class LeaderElectionWithAccrualFailureDetectorMultiJvmNode4 extends LeaderElectionSpec(failureDetectorPuppet = false)
class LeaderElectionWithAccrualFailureDetectorMultiJvmNode5 extends LeaderElectionSpec(failureDetectorPuppet = false)

abstract class LeaderElectionSpec(multiNodeConfig: LeaderElectionMultiNodeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig) {

  def this(failureDetectorPuppet: Boolean) = this(LeaderElectionMultiNodeConfig(failureDetectorPuppet))

  import multiNodeConfig._

  // sorted in the order used by the cluster
  lazy val sortedRoles = List(first, second, third, fourth).sorted

  "A cluster of four nodes" must {

    "be able to 'elect' a single leader" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third, fourth)

      if (myself != controller) {
        clusterView.isLeader should ===(myself == sortedRoles.head)
        assertLeaderIn(sortedRoles)
      }

      enterBarrier("after-1")
    }

    def shutdownLeaderAndVerifyNewLeader(alreadyShutdown: Int): Unit = {
      val currentRoles = sortedRoles.drop(alreadyShutdown)
      currentRoles.size should be >= (2)
      val leader = currentRoles.head
      val aUser = currentRoles.last
      val remainingRoles = currentRoles.tail
      val n = "-" + (alreadyShutdown + 1)

      myself match {

        case `controller` =>
          enterBarrier("before-shutdown" + n)
          testConductor.exit(leader, 0).await
          enterBarrier("after-shutdown" + n, "after-unavailable" + n, "after-down" + n, "completed" + n)

        case `leader` =>
          enterBarrier("before-shutdown" + n, "after-shutdown" + n)
        // this node will be shutdown by the controller and doesn't participate in more barriers

        case `aUser` =>
          val leaderAddress = address(leader)
          enterBarrier("before-shutdown" + n, "after-shutdown" + n)

          // detect failure
          markNodeAsUnavailable(leaderAddress)
          awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(leaderAddress))
          enterBarrier("after-unavailable" + n)

          // user marks the shutdown leader as DOWN
          cluster.down(leaderAddress)
          // removed
          awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain (leaderAddress))
          enterBarrier("after-down" + n, "completed" + n)

        case _ if remainingRoles.contains(myself) =>
          // remaining cluster nodes, not shutdown
          val leaderAddress = address(leader)
          enterBarrier("before-shutdown" + n, "after-shutdown" + n)

          awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(leaderAddress))
          enterBarrier("after-unavailable" + n)

          enterBarrier("after-down" + n)
          awaitMembersUp(currentRoles.size - 1)
          val nextExpectedLeader = remainingRoles.head
          clusterView.isLeader should ===(myself == nextExpectedLeader)
          assertLeaderIn(remainingRoles)

          enterBarrier("completed" + n)

        case _ =>
          fail()
      }
    }

    "be able to 're-elect' a single leader after leader has left" taggedAs LongRunningTest in within(30 seconds) {
      shutdownLeaderAndVerifyNewLeader(alreadyShutdown = 0)
      enterBarrier("after-2")
    }

    "be able to 're-elect' a single leader after leader has left (again)" taggedAs LongRunningTest in within(30 seconds) {
      shutdownLeaderAndVerifyNewLeader(alreadyShutdown = 1)
      enterBarrier("after-3")
    }
  }
}
