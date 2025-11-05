/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object DisallowJoinOfTwoClustersMultiJvmSpec extends MultiNodeConfig {
  val a1 = role("a1")
  val a2 = role("a2")
  val b1 = role("b1")
  val b2 = role("b2")
  val c1 = role("c1")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class DisallowJoinOfTwoClustersMultiJvmNode1 extends DisallowJoinOfTwoClustersSpec
class DisallowJoinOfTwoClustersMultiJvmNode2 extends DisallowJoinOfTwoClustersSpec
class DisallowJoinOfTwoClustersMultiJvmNode3 extends DisallowJoinOfTwoClustersSpec
class DisallowJoinOfTwoClustersMultiJvmNode4 extends DisallowJoinOfTwoClustersSpec
class DisallowJoinOfTwoClustersMultiJvmNode5 extends DisallowJoinOfTwoClustersSpec

abstract class DisallowJoinOfTwoClustersSpec extends MultiNodeClusterSpec(DisallowJoinOfTwoClustersMultiJvmSpec) {

  import DisallowJoinOfTwoClustersMultiJvmSpec._

  "Three different clusters (A, B and C)" must {

    "not be able to join" taggedAs LongRunningTest in {
      // make sure that the node-to-join is started before other join
      runOn(a1, b1, c1) {
        startClusterNode()
      }
      enterBarrier("first-started")

      runOn(a1, a2) {
        cluster.join(a1)
      }
      runOn(b1, b2) {
        cluster.join(b1)
      }
      runOn(c1) {
        cluster.join(c1)
      }

      val expectedSize = if (myself == c1) 1 else 2
      awaitMembersUp(numberOfMembers = expectedSize)

      enterBarrier("two-members")

      runOn(b1) {
        cluster.join(a1)
      }
      runOn(b2) {
        cluster.join(c1)
      }
      runOn(c1) {
        cluster.join(a2)
      }

      // no change expected
      (1 to 5).foreach { _ =>
        clusterView.members.size should ===(expectedSize)
        Thread.sleep(1000)
      }

      enterBarrier("after-1")
    }

  }
}
