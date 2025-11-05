/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object NodeDowningAndBeingRemovedMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false).withFallback(
      ConfigFactory
        .parseString("akka.cluster.testkit.auto-down-unreachable-after = off")
        .withFallback(MultiNodeClusterSpec.clusterConfig)))
}

class NodeDowningAndBeingRemovedMultiJvmNode1 extends NodeDowningAndBeingRemovedSpec
class NodeDowningAndBeingRemovedMultiJvmNode2 extends NodeDowningAndBeingRemovedSpec
class NodeDowningAndBeingRemovedMultiJvmNode3 extends NodeDowningAndBeingRemovedSpec

abstract class NodeDowningAndBeingRemovedSpec extends MultiNodeClusterSpec(NodeDowningAndBeingRemovedMultiJvmSpec) {

  import NodeDowningAndBeingRemovedMultiJvmSpec._

  "A node that is downed" must {

    "eventually be removed from membership" taggedAs LongRunningTest in {

      awaitClusterUp(first, second, third)

      within(30.seconds) {
        runOn(first) {
          cluster.down(second)
          cluster.down(third)
        }
        enterBarrier("second-and-third-down")

        runOn(second, third) {
          // verify that the node is shut down
          awaitCond(cluster.isTerminated)
        }
        enterBarrier("second-and-third-shutdown")

        runOn(first) {
          // verify that the nodes are no longer part of the 'members' set
          awaitAssert {
            clusterView.members.map(_.address) should not contain (address(second))
            clusterView.members.map(_.address) should not contain (address(third))
          }
        }
      }

      enterBarrier("finished")
    }
  }
}
