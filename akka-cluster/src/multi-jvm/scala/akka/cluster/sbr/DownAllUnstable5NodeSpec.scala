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

object DownAllUnstable5NodeSpec extends MultiNodeConfig {
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
        failure-detector.acceptable-heartbeat-pause = 3s
        split-brain-resolver.active-strategy = keep-majority
        split-brain-resolver.stable-after = 10s
        split-brain-resolver.down-all-when-unstable = 7s

        run-coordinated-shutdown-when-down = off
      }

      actor.provider = cluster

      test.filter-leeway = 10s
    }
  """))

  testTransport(on = true)
}

class DownAllUnstable5NodeSpecMultiJvmNode1 extends DownAllUnstable5NodeSpec
class DownAllUnstable5NodeSpecMultiJvmNode2 extends DownAllUnstable5NodeSpec
class DownAllUnstable5NodeSpecMultiJvmNode3 extends DownAllUnstable5NodeSpec
class DownAllUnstable5NodeSpecMultiJvmNode4 extends DownAllUnstable5NodeSpec
class DownAllUnstable5NodeSpecMultiJvmNode5 extends DownAllUnstable5NodeSpec

class DownAllUnstable5NodeSpec extends MultiNodeClusterSpec(DownAllUnstable5NodeSpec) {
  import DownAllUnstable5NodeSpec._

  "A 5-node cluster with down-all-when-unstable" should {
    "down all when instability continues" in {
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

      // acceptable-heartbeat-pause = 3s
      // stable-after = 10s
      // down-all-when-unstable = 7s

      runOn(node1) {
        for (x <- List(node1, node2, node3); y <- List(node4, node5)) {
          testConductor.blackhole(x, y, Direction.Both).await
        }
      }
      enterBarrier("blackholed-clean-partition")

      within(10.seconds) {
        awaitAssert {
          runOn(node1, node2, node3) {
            cluster.state.unreachable.map(_.address) should ===(Set(node4, node5).map(node(_).address))
          }
          runOn(node4, node5) {
            cluster.state.unreachable.map(_.address) should ===(Set(node1, node2, node3).map(node(_).address))
          }
        }
      }
      enterBarrier("unreachable-clean-partition")

      // no decision yet
      Thread.sleep(2000)
      cluster.state.members.size should ===(5)
      cluster.state.members.foreach {
        _.status should ===(MemberStatus.Up)
      }

      runOn(node1) {
        testConductor.blackhole(node2, node3, Direction.Both).await
      }
      enterBarrier("blackhole-2")
      // then it takes about 5 seconds for failure detector to observe that
      Thread.sleep(7000)

      runOn(node1) {
        testConductor.passThrough(node2, node3, Direction.Both).await
      }
      enterBarrier("passThrough-2")

      // now it should have been unstable for more than 17 seconds

      // all downed
      awaitCond(cluster.isTerminated, max = 15.seconds)

      enterBarrier("done")
    }

  }

}
