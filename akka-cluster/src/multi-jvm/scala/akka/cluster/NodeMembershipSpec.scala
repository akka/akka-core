/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object NodeMembershipMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class NodeMembershipMultiJvmNode1 extends NodeMembershipSpec
class NodeMembershipMultiJvmNode2 extends NodeMembershipSpec
class NodeMembershipMultiJvmNode3 extends NodeMembershipSpec

abstract class NodeMembershipSpec extends MultiNodeClusterSpec(NodeMembershipMultiJvmSpec) {

  import NodeMembershipMultiJvmSpec._

  "A set of connected cluster systems" must {

    "(when two nodes) start gossiping to each other so that both nodes gets the same gossip info" taggedAs LongRunningTest in {

      // make sure that the node-to-join is started before other join
      runOn(first) {
        startClusterNode()
      }
      enterBarrier("first-started")

      runOn(first, second) {
        cluster.join(first)
        awaitAssert(clusterView.members.size should ===(2))
        assertMembers(clusterView.members, first, second)
        awaitAssert(clusterView.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up)))
      }

      enterBarrier("after-1")
    }

    "(when three nodes) start gossiping to each other so that all nodes gets the same gossip info" taggedAs LongRunningTest in {

      runOn(third) {
        cluster.join(first)
      }

      awaitAssert(clusterView.members.size should ===(3))
      assertMembers(clusterView.members, first, second, third)
      awaitAssert(clusterView.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up)))

      enterBarrier("after-2")
    }

    "correct member age" taggedAs LongRunningTest in {
      val firstMember = clusterView.members.find(_.address == address(first)).get
      val secondMember = clusterView.members.find(_.address == address(second)).get
      val thirdMember = clusterView.members.find(_.address == address(third)).get
      firstMember.isOlderThan(thirdMember) should ===(true)
      thirdMember.isOlderThan(firstMember) should ===(false)
      secondMember.isOlderThan(thirdMember) should ===(true)
      thirdMember.isOlderThan(secondMember) should ===(false)

      enterBarrier("after-3")

    }
  }
}
