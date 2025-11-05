/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Address
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

final case class SingletonClusterMultiNodeConfig(failureDetectorPuppet: Boolean) extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster {
        downing-provider-class = akka.cluster.testkit.AutoDowning
        testkit.auto-down-unreachable-after = 0s
        failure-detector.threshold = 4
      }
    """))
      .withFallback(MultiNodeClusterSpec.clusterConfig(failureDetectorPuppet)))

}

class SingletonClusterWithFailureDetectorPuppetMultiJvmNode1 extends SingletonClusterSpec(failureDetectorPuppet = true)
class SingletonClusterWithFailureDetectorPuppetMultiJvmNode2 extends SingletonClusterSpec(failureDetectorPuppet = true)

class SingletonClusterWithAccrualFailureDetectorMultiJvmNode1
    extends SingletonClusterSpec(failureDetectorPuppet = false)
class SingletonClusterWithAccrualFailureDetectorMultiJvmNode2
    extends SingletonClusterSpec(failureDetectorPuppet = false)

abstract class SingletonClusterSpec(multiNodeConfig: SingletonClusterMultiNodeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig) {

  def this(failureDetectorPuppet: Boolean) = this(SingletonClusterMultiNodeConfig(failureDetectorPuppet))

  import multiNodeConfig._

  muteMarkingAsUnreachable()

  "A cluster of 2 nodes" must {

    "become singleton cluster when started with seed-nodes" taggedAs LongRunningTest in {
      runOn(first) {
        val nodes: immutable.IndexedSeq[Address] = Vector(first) //
        cluster.joinSeedNodes(nodes)
        awaitMembersUp(1)
        clusterView.isSingletonCluster should ===(true)
      }

      enterBarrier("after-1")
    }

    "not be singleton cluster when joined with other node" taggedAs LongRunningTest in {
      awaitClusterUp(first, second)
      clusterView.isSingletonCluster should ===(false)
      assertLeader(first, second)

      enterBarrier("after-2")
    }

    "become singleton cluster when one node is shutdown" taggedAs LongRunningTest in {
      runOn(first) {
        val secondAddress = address(second)
        testConductor.exit(second, 0).await

        markNodeAsUnavailable(secondAddress)

        awaitMembersUp(numberOfMembers = 1, canNotBePartOfMemberRing = Set(secondAddress), 30.seconds)
        clusterView.isSingletonCluster should ===(true)
        awaitCond(clusterView.isLeader)
      }

      enterBarrier("after-3")
    }

    "leave and shutdown itself when singleton cluster" taggedAs LongRunningTest in {
      runOn(first) {
        cluster.leave(first)
        awaitCond(cluster.isTerminated, 5.seconds)
      }
      enterBarrier("after-4")
    }
  }
}
