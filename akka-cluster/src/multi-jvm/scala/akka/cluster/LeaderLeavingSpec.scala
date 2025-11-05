/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.Deploy
import akka.actor.Props
import akka.cluster.MemberStatus._
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object LeaderLeavingMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
      akka.cluster.testkit.auto-down-unreachable-after = 0s"""))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class LeaderLeavingMultiJvmNode1 extends LeaderLeavingSpec
class LeaderLeavingMultiJvmNode2 extends LeaderLeavingSpec
class LeaderLeavingMultiJvmNode3 extends LeaderLeavingSpec

abstract class LeaderLeavingSpec extends MultiNodeClusterSpec(LeaderLeavingMultiJvmSpec) {

  import ClusterEvent._
  import LeaderLeavingMultiJvmSpec._

  "A LEADER that is LEAVING" must {

    "be moved to LEAVING, then to EXITING, then to REMOVED, then be shut down and then a new LEADER should be elected" taggedAs LongRunningTest in {

      awaitClusterUp(first, second, third)

      val oldLeaderAddress = clusterView.leader.get

      within(30.seconds) {

        if (clusterView.isLeader) {

          enterBarrier("registered-listener")

          cluster.leave(oldLeaderAddress)
          enterBarrier("leader-left")

          // verify that the LEADER is shut down
          awaitCond(cluster.isTerminated)
          enterBarrier("leader-shutdown")

        } else {

          val exitingLatch = TestLatch()

          cluster.subscribe(system.actorOf(Props(new Actor {
            def receive = {
              case state: CurrentClusterState =>
                if (state.members.exists(m => m.address == oldLeaderAddress && m.status == Exiting))
                  exitingLatch.countDown()
              case MemberExited(m) if m.address == oldLeaderAddress => exitingLatch.countDown()
              case _                                                => // ignore
            }
          }).withDeploy(Deploy.local)), classOf[MemberEvent])
          enterBarrier("registered-listener")

          enterBarrier("leader-left")

          // verify that the LEADER is EXITING
          exitingLatch.await

          enterBarrier("leader-shutdown")
          markNodeAsUnavailable(oldLeaderAddress)

          // verify that the LEADER is no longer part of the 'members' set
          awaitAssert(clusterView.members.map(_.address) should not contain (oldLeaderAddress))

          // verify that the LEADER is not part of the 'unreachable' set
          awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain (oldLeaderAddress))

          // verify that we have a new LEADER
          awaitAssert(clusterView.leader should not be (oldLeaderAddress))
        }

        enterBarrier("finished")
      }
    }
  }
}
