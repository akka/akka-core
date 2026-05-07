/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import akka.cluster.MemberStatus.Removed
import akka.remote.testkit.MultiNodeConfig

object ClusterShutdownSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val forth = role("forth")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfig))
}

class ClusterShutdownSpecMultiJvmNode1 extends ClusterShutdownSpec
class ClusterShutdownSpecMultiJvmNode2 extends ClusterShutdownSpec
class ClusterShutdownSpecMultiJvmNode3 extends ClusterShutdownSpec
class ClusterShutdownSpecMultiJvmNode4 extends ClusterShutdownSpec

abstract class ClusterShutdownSpec extends MultiNodeClusterSpec(ClusterShutdownSpec) with Eventually {

  import ClusterShutdownSpec._

  "Cluster shutdown" should {
    "form cluster" in {
      awaitClusterUp(first, second, third)
    }
    enterBarrier("cluster-up")
    "prepare for shutdown" in {
      runOn(first) {
        Cluster(system).prepareForFullClusterShutdown()
      }

      runOn(first, second, third) {
        awaitAssert({
          withClue("members: " + Cluster(system).readView.members) {
            Cluster(system).selfMember.status shouldEqual MemberStatus.ReadyForShutdown
          }
        }, 10.seconds)
      }
    }
    "spread around the cluster" in {
      runOn(first, second, third) {
        awaitAssert {
          Cluster(system).readView.members.unsorted.map(_.status) shouldEqual Set(MemberStatus.ReadyForShutdown)
        }
      }
      enterBarrier("propagation finished")
    }
    "not allow new members to join" in {
      runOn(forth) {
        cluster.join(address(first))
        Thread.sleep(3000)
        // should not be allowed to join the cluster even after some time
        awaitAssert {
          cluster.selfMember.status shouldBe MemberStatus.Removed
        }

      }
      enterBarrier("not-allowed-to-join")
    }
    "be allowed to leave" in {
      runOn(first) {
        Cluster(system).leave(address(first))
      }
      awaitAssert({
        withClue("members: " + Cluster(system).readView.members) {
          runOn(second, third) {
            Cluster(system).readView.members.size shouldEqual 2
          }
          runOn(first) {
            Cluster(system).selfMember.status shouldEqual Removed
          }
        }
      }, 10.seconds)
      enterBarrier("first-gone")
      runOn(second) {
        Cluster(system).leave(address(second))
        Cluster(system).leave(address(third))
      }
      awaitAssert({
        withClue("self member: " + Cluster(system).selfMember) {
          Cluster(system).selfMember.status shouldEqual Removed
        }
      }, 10.seconds)
      enterBarrier("all-gone")
    }
  }
}
