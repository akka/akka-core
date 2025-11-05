/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.actor.Actor
import akka.actor.Deploy
import akka.actor.Props
import akka.cluster.MemberStatus._
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object NodeLeavingAndExitingMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class NodeLeavingAndExitingMultiJvmNode1 extends NodeLeavingAndExitingSpec
class NodeLeavingAndExitingMultiJvmNode2 extends NodeLeavingAndExitingSpec
class NodeLeavingAndExitingMultiJvmNode3 extends NodeLeavingAndExitingSpec

abstract class NodeLeavingAndExitingSpec extends MultiNodeClusterSpec(NodeLeavingAndExitingMultiJvmSpec) {

  import ClusterEvent._
  import NodeLeavingAndExitingMultiJvmSpec._

  "A node that is LEAVING a non-singleton cluster" must {

    "be moved to EXITING by the leader" taggedAs LongRunningTest in {

      awaitClusterUp(first, second, third)

      runOn(first, third) {
        val secondAddess = address(second)
        val exitingLatch = TestLatch()
        cluster.subscribe(system.actorOf(Props(new Actor {
          def receive = {
            case state: CurrentClusterState =>
              if (state.members.exists(m => m.address == secondAddess && m.status == Exiting))
                exitingLatch.countDown()
            case MemberExited(m) if m.address == secondAddess => exitingLatch.countDown()
            case _: MemberRemoved                             => // not tested here

          }
        }).withDeploy(Deploy.local)), classOf[MemberEvent])
        enterBarrier("registered-listener")

        runOn(third) {
          cluster.leave(second)
        }
        enterBarrier("second-left")

        // Verify that 'second' node is set to EXITING
        exitingLatch.await
      }

      // node that is leaving
      runOn(second) {
        enterBarrier("registered-listener")
        enterBarrier("second-left")
      }

      enterBarrier("finished")
    }
  }
}
