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

object DownAllWhenIndirectlyConnected3NodeSpec extends MultiNodeConfig {
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
        split-brain-resolver.down-all-when-indirectly-connected = on

        run-coordinated-shutdown-when-down = off
      }

      actor.provider = cluster

      test.filter-leeway = 10s
    }
  """))

  testTransport(on = true)
}

class DownAllWhenIndirectlyConnected3NodeSpecMultiJvmNode1 extends DownAllWhenIndirectlyConnected3NodeSpec
class DownAllWhenIndirectlyConnected3NodeSpecMultiJvmNode2 extends DownAllWhenIndirectlyConnected3NodeSpec
class DownAllWhenIndirectlyConnected3NodeSpecMultiJvmNode3 extends DownAllWhenIndirectlyConnected3NodeSpec

class DownAllWhenIndirectlyConnected3NodeSpec extends MultiNodeClusterSpec(DownAllWhenIndirectlyConnected3NodeSpec) {
  import DownAllWhenIndirectlyConnected3NodeSpec._

  "A 3-node cluster with down-all-when-indirectly-connected=on" should {
    "down all when two unreachable but can talk via third" in {
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

      // all downed
      awaitCond(cluster.isTerminated, max = 15.seconds)

      enterBarrier("done")
    }
  }

}
