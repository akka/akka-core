/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Address
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object DeterministicOldestWhenJoiningMultiJvmSpec extends MultiNodeConfig {
  val seed1 = role("seed1")
  val seed2 = role("seed2")
  val seed3 = role("seed3")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
    # not too quick to trigger problematic scenario more often
    akka.cluster.leader-actions-interval = 2000 ms
    akka.cluster.gossip-interval = 500 ms
    """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))
}

class DeterministicOldestWhenJoiningMultiJvmNode1 extends DeterministicOldestWhenJoiningSpec
class DeterministicOldestWhenJoiningMultiJvmNode2 extends DeterministicOldestWhenJoiningSpec
class DeterministicOldestWhenJoiningMultiJvmNode3 extends DeterministicOldestWhenJoiningSpec

abstract class DeterministicOldestWhenJoiningSpec
    extends MultiNodeClusterSpec(DeterministicOldestWhenJoiningMultiJvmSpec) {

  import DeterministicOldestWhenJoiningMultiJvmSpec._

  // reverse order because that expose the bug in issue #18554
  def seedNodes: immutable.IndexedSeq[Address] =
    Vector(address(seed1), address(seed2), address(seed3)).sorted(Member.addressOrdering).reverse
  val roleByAddress = Map(address(seed1) -> seed1, address(seed2) -> seed2, address(seed3) -> seed3)

  "Joining a cluster" must {
    "result in deterministic oldest node" taggedAs LongRunningTest in {
      cluster.subscribe(testActor, classOf[MemberUp])
      expectMsgType[CurrentClusterState]

      runOn(roleByAddress(seedNodes.head)) {
        cluster.joinSeedNodes(seedNodes)
      }
      enterBarrier("first-seed-joined")

      runOn(roleByAddress(seedNodes(1)), roleByAddress(roleByAddress(seedNodes(2)))) {
        cluster.joinSeedNodes(seedNodes)
      }

      within(15.seconds) {
        val ups = List(expectMsgType[MemberUp], expectMsgType[MemberUp], expectMsgType[MemberUp])
        ups.map(_.member).sorted(Member.ageOrdering).head.address should ===(seedNodes.head)
      }

      enterBarrier("after-1")
    }

  }
}
