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

object IndirectlyConnected5NodeSpec extends MultiNodeConfig {
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

        run-coordinated-shutdown-when-down = off
      }

      actor.provider = cluster

      test.filter-leeway = 10s
    }
  """))

  testTransport(on = true)
}

class IndirectlyConnected5NodeSpecMultiJvmNode1 extends IndirectlyConnected5NodeSpec
class IndirectlyConnected5NodeSpecMultiJvmNode2 extends IndirectlyConnected5NodeSpec
class IndirectlyConnected5NodeSpecMultiJvmNode3 extends IndirectlyConnected5NodeSpec
class IndirectlyConnected5NodeSpecMultiJvmNode4 extends IndirectlyConnected5NodeSpec
class IndirectlyConnected5NodeSpecMultiJvmNode5 extends IndirectlyConnected5NodeSpec

class IndirectlyConnected5NodeSpec extends MultiNodeClusterSpec(IndirectlyConnected5NodeSpec) {
  import IndirectlyConnected5NodeSpec._

  "A 5-node cluster" should {
    "avoid a split brain when indirectly connected combined with clean partition" in {
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

      runOn(node1) {
        for (x <- List(node1, node2, node3); y <- List(node4, node5)) {
          testConductor.blackhole(x, y, Direction.Both).await
        }
      }
      enterBarrier("blackholed-clean-partition")

      runOn(node1) {
        testConductor.blackhole(node2, node3, Direction.Both).await
      }
      enterBarrier("blackholed-indirectly-connected")

      within(10.seconds) {
        awaitAssert {
          runOn(node1) {
            cluster.state.unreachable.map(_.address) should ===(Set(node2, node3, node4, node5).map(node(_).address))
          }
          runOn(node2) {
            cluster.state.unreachable.map(_.address) should ===(Set(node3, node4, node5).map(node(_).address))
          }
          runOn(node3) {
            cluster.state.unreachable.map(_.address) should ===(Set(node2, node4, node5).map(node(_).address))
          }
          runOn(node4, node5) {
            cluster.state.unreachable.map(_.address) should ===(Set(node1, node2, node3).map(node(_).address))
          }
        }
      }
      enterBarrier("unreachable")

      runOn(node1) {
        within(15.seconds) {
          awaitAssert {
            cluster.state.members.map(_.address) should ===(Set(node(node1).address))
            cluster.state.members.foreach {
              _.status should ===(MemberStatus.Up)
            }
          }
        }
      }

      runOn(node2, node3, node4, node5) {
        // downed
        awaitCond(cluster.isTerminated, max = 15.seconds)
      }

      enterBarrier("done")
    }
  }

}
