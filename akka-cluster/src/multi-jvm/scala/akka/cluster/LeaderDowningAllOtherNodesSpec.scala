/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object LeaderDowningAllOtherNodesMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.failure-detector.monitored-by-nr-of-members = 2
      akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
      akka.cluster.testkit.auto-down-unreachable-after = 1s
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))
}

class LeaderDowningAllOtherNodesMultiJvmNode1 extends LeaderDowningAllOtherNodesSpec
class LeaderDowningAllOtherNodesMultiJvmNode2 extends LeaderDowningAllOtherNodesSpec
class LeaderDowningAllOtherNodesMultiJvmNode3 extends LeaderDowningAllOtherNodesSpec
class LeaderDowningAllOtherNodesMultiJvmNode4 extends LeaderDowningAllOtherNodesSpec
class LeaderDowningAllOtherNodesMultiJvmNode5 extends LeaderDowningAllOtherNodesSpec
class LeaderDowningAllOtherNodesMultiJvmNode6 extends LeaderDowningAllOtherNodesSpec

abstract class LeaderDowningAllOtherNodesSpec extends MultiNodeClusterSpec(LeaderDowningAllOtherNodesMultiJvmSpec) {

  import LeaderDowningAllOtherNodesMultiJvmSpec._

  "A cluster of 6 nodes with monitored-by-nr-of-members=2" must {
    "setup" taggedAs LongRunningTest in {
      // start some
      awaitClusterUp(roles: _*)
      enterBarrier("after-1")
    }

    "remove all shutdown nodes" taggedAs LongRunningTest in {
      val others = roles.drop(1)
      val shutdownAddresses = others.map(address).toSet
      enterBarrier("before-all-other-shutdown")
      runOn(first) {
        for (node <- others)
          testConductor.exit(node, 0).await
      }
      enterBarrier("all-other-shutdown")
      awaitMembersUp(numberOfMembers = 1, canNotBePartOfMemberRing = shutdownAddresses, 30.seconds)
    }

  }
}
