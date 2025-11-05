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

object IndirectlyConnected3NodeSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

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

class IndirectlyConnected3NodeSpecMultiJvmNode1 extends IndirectlyConnected3NodeSpec
class IndirectlyConnected3NodeSpecMultiJvmNode2 extends IndirectlyConnected3NodeSpec
class IndirectlyConnected3NodeSpecMultiJvmNode3 extends IndirectlyConnected3NodeSpec

class IndirectlyConnected3NodeSpec extends MultiNodeClusterSpec(IndirectlyConnected3NodeSpec) {
  import IndirectlyConnected3NodeSpec._

  "A 3-node cluster" should {
    "avoid a split brain when two unreachable but can talk via third" in {
      val cluster = Cluster(system)

      runOn(node1) {
        cluster.join(cluster.selfAddress)
      }
      enterBarrier("node1 joined")
      runOn(node2, node3) {
        cluster.join(node(node1).address)
      }
      within(10.seconds) {
        awaitAssert {
          cluster.state.members.size should ===(3)
          cluster.state.members.foreach {
            _.status should ===(MemberStatus.Up)
          }
        }
      }
      enterBarrier("Cluster formed")

      runOn(node1) {
        testConductor.blackhole(node2, node3, Direction.Both).await
      }
      enterBarrier("Blackholed")

      within(10.seconds) {
        awaitAssert {
          runOn(node3) {
            cluster.state.unreachable.map(_.address) should ===(Set(node(node2).address))
          }
          runOn(node2) {
            cluster.state.unreachable.map(_.address) should ===(Set(node(node3).address))
          }
          runOn(node1) {
            cluster.state.unreachable.map(_.address) should ===(Set(node(node3).address, node(node2).address))
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

      runOn(node2, node3) {
        // downed
        awaitCond(cluster.isTerminated, max = 15.seconds)
      }

      enterBarrier("done")
    }
  }

}
