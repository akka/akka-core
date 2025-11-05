/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.concurrent.Future
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.MemberStatus
import akka.cluster.MultiNodeClusterSpec
import akka.coordination.lease.TestLease
import akka.coordination.lease.TestLeaseExt
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig

object LeaseMajority5NodeSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")

  commonConfig(ConfigFactory.parseString(s"""
    akka {
      loglevel = INFO
      cluster {
        gossip-interval                     = 200 ms
        leader-actions-interval             = 200 ms
        periodic-tasks-initial-delay        = 300 ms
        failure-detector.heartbeat-interval = 500 ms
      
        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver {
          active-strategy = lease-majority
          stable-after = 1.5s
          lease-majority {
            lease-implementation = test-lease
            acquire-lease-delay-for-minority = 1s
            release-after = 3s
          }
        }

        run-coordinated-shutdown-when-down = off
      }

      actor.provider = cluster

      test.filter-leeway = 10s
    }

    test-lease {
      lease-class = ${classOf[TestLease].getName}
      heartbeat-interval = 1s
      heartbeat-timeout = 120s
      lease-operation-timeout = 3s
    }
  """))

  testTransport(on = true)
}

class LeaseMajority5NodeSpecMultiJvmNode1 extends LeaseMajority5NodeSpec
class LeaseMajority5NodeSpecMultiJvmNode2 extends LeaseMajority5NodeSpec
class LeaseMajority5NodeSpecMultiJvmNode3 extends LeaseMajority5NodeSpec
class LeaseMajority5NodeSpecMultiJvmNode4 extends LeaseMajority5NodeSpec
class LeaseMajority5NodeSpecMultiJvmNode5 extends LeaseMajority5NodeSpec

class LeaseMajority5NodeSpec extends MultiNodeClusterSpec(LeaseMajority5NodeSpec) {
  import LeaseMajority5NodeSpec._

  private val testLeaseName = "LeaseMajority5NodeSpec-akka-sbr"

  def sortByAddress(roles: RoleName*): List[RoleName] = {

    /**
     * Sort the roles in the address order used by the cluster node ring.
     */
    implicit val clusterOrdering: Ordering[RoleName] = new Ordering[RoleName] {
      import akka.cluster.Member.addressOrdering
      def compare(x: RoleName, y: RoleName): Int = addressOrdering.compare(node(x).address, node(y).address)
    }
    roles.toList.sorted
  }

  def leader(roles: RoleName*): RoleName =
    sortByAddress(roles: _*).head

  "LeaseMajority in a 5-node cluster" should {
    "setup cluster" in {
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
    }

    "keep the side that can acquire the lease" in {
      val lease = TestLeaseExt(system).getTestLease(testLeaseName)
      val leaseProbe = lease.probe

      runOn(node1, node2, node3) {
        lease.setNextAcquireResult(Future.successful(true))
      }
      runOn(node4, node5) {
        lease.setNextAcquireResult(Future.successful(false))
      }
      enterBarrier("lease-in-place")
      runOn(node1) {
        for (x <- List(node1, node2, node3); y <- List(node4, node5)) {
          testConductor.blackhole(x, y, Direction.Both).await
        }
      }
      enterBarrier("blackholed-clean-partition")

      runOn(node1, node2, node3) {
        within(20.seconds) {
          awaitAssert {
            cluster.state.members.size should ===(3)
          }
        }
        runOn(leader(node1, node2, node3)) {
          leaseProbe.expectMsgType[TestLease.AcquireReq]
          // after 2 * stable-after
          leaseProbe.expectMsgType[TestLease.ReleaseReq](14.seconds)
        }
      }
      runOn(node4, node5) {
        within(20.seconds) {
          awaitAssert {
            cluster.isTerminated should ===(true)
          }
          runOn(leader(node4, node5)) {
            leaseProbe.expectMsgType[TestLease.AcquireReq]
          }
        }
      }
      enterBarrier("downed-and-removed")
      leaseProbe.expectNoMessage(1.second)

      enterBarrier("done-1")
    }
  }

  "keep the side that can acquire the lease, round 2" in {
    val lease = TestLeaseExt(system).getTestLease(testLeaseName)

    runOn(node1) {
      lease.setNextAcquireResult(Future.successful(true))
    }
    runOn(node2, node3) {
      lease.setNextAcquireResult(Future.successful(false))
    }
    enterBarrier("lease-in-place-2")
    runOn(node1) {
      for (x <- List(node1); y <- List(node2, node3)) {
        testConductor.blackhole(x, y, Direction.Both).await
      }
    }
    enterBarrier("blackholed-clean-partition-2")

    runOn(node1) {
      within(20.seconds) {
        awaitAssert {
          cluster.state.members.size should ===(1)
        }
      }
    }
    runOn(node2, node3) {
      within(20.seconds) {
        awaitAssert {
          cluster.isTerminated should ===(true)
        }
      }
    }

    enterBarrier("done-2")
  }

}
