/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.MultiNodeClusterSpec
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig

object DownAllWhenIndirectlyConnected5NodeSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")

  commonConfig(ConfigFactory.parseString("""
    akka {
      loglevel = INFO
      cluster {
        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver.active-strategy = keep-majority
        split-brain-resolver.stable-after = 6s
        # default down-all-when-indirectly-connected = 0.5 (50% threshold)

        run-coordinated-shutdown-when-down = off
      }

      actor.provider = cluster

      test.filter-leeway = 10s
    }
  """))

  testTransport(on = true)
}

class DownAllWhenIndirectlyConnected5NodeSpecMultiJvmNode1 extends DownAllWhenIndirectlyConnected5NodeSpec
class DownAllWhenIndirectlyConnected5NodeSpecMultiJvmNode2 extends DownAllWhenIndirectlyConnected5NodeSpec
class DownAllWhenIndirectlyConnected5NodeSpecMultiJvmNode3 extends DownAllWhenIndirectlyConnected5NodeSpec
class DownAllWhenIndirectlyConnected5NodeSpecMultiJvmNode4 extends DownAllWhenIndirectlyConnected5NodeSpec
class DownAllWhenIndirectlyConnected5NodeSpecMultiJvmNode5 extends DownAllWhenIndirectlyConnected5NodeSpec

class DownAllWhenIndirectlyConnected5NodeSpec extends MultiNodeClusterSpec(DownAllWhenIndirectlyConnected5NodeSpec) {
  import DownAllWhenIndirectlyConnected5NodeSpec._

  "A 5-node cluster with down-all-when-indirectly-connected threshold" should {
    "down all when indirectly connected cycle involves 3 of 5 nodes (60% > 50% threshold)" in {
      // This test creates an indirect connection cycle involving nodes 1, 2, 3:
      //   node1 <-> node2: blocked
      //   node2 <-> node3: blocked
      //   node3 <-> node1: blocked
      // Nodes 4 and 5 can still reach everyone, so gossip flows via them.
      //
      // This creates a cycle in the unreachability graph where nodes 1, 2, 3 are both
      // observers and subjects. The DownIndirectlyConnected decision would down all 3,
      // which is 60% of the cluster - exceeding the 50% threshold.
      // Therefore, SBR should down ALL nodes instead.

      val cluster = Cluster(system)

      runOn(node1) {
        cluster.join(cluster.selfAddress)
      }
      enterBarrier("node1 joined")
      runOn(node2, node3, node4, node5) {
        cluster.join(node(node1).address)
      }
      within(10.seconds) {
        awaitAssert {
          cluster.state.members.size should ===(5)
          cluster.state.members.foreach {
            _.status should ===(MemberStatus.Up)
          }
        }
      }
      enterBarrier("Cluster formed")

      // Create indirect connection cycle: node1 <-> node2 <-> node3 <-> node1
      // All connections between these three nodes are blocked, but they can still
      // communicate via node4 and node5
      runOn(node1) {
        testConductor.blackhole(node1, node2, Direction.Both).await
        testConductor.blackhole(node2, node3, Direction.Both).await
        testConductor.blackhole(node3, node1, Direction.Both).await
      }
      enterBarrier("blackholed-indirectly-connected-cycle")

      // All nodes should be downed because the indirectly connected set (3 nodes)
      // exceeds the 50% threshold
      runOn(node1, node2, node3, node4, node5) {
        awaitCond(cluster.isTerminated, max = 15.seconds)
      }

      enterBarrier("done")
    }

  }

}
