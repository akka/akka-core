/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable

import com.typesafe.config.ConfigFactory

import akka.actor.Address
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._
import akka.util.Version

object JoinSeedNodeMultiJvmSpec extends MultiNodeConfig {
  val seed1 = role("seed1")
  val seed2 = role("seed2")
  val seed3 = role("seed3")
  val ordinary1 = role("ordinary1")
  val ordinary2 = role("ordinary2")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""akka.cluster.app-version="1.0""""))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(ordinary1, ordinary2)(ConfigFactory.parseString("""akka.cluster.app-version="2.0""""))
}

class JoinSeedNodeMultiJvmNode1 extends JoinSeedNodeSpec
class JoinSeedNodeMultiJvmNode2 extends JoinSeedNodeSpec
class JoinSeedNodeMultiJvmNode3 extends JoinSeedNodeSpec
class JoinSeedNodeMultiJvmNode4 extends JoinSeedNodeSpec
class JoinSeedNodeMultiJvmNode5 extends JoinSeedNodeSpec

abstract class JoinSeedNodeSpec extends MultiNodeClusterSpec(JoinSeedNodeMultiJvmSpec) {

  import JoinSeedNodeMultiJvmSpec._

  def seedNodes: immutable.IndexedSeq[Address] = Vector(seed1, seed2, seed3)

  "A cluster with seed nodes" must {
    "be able to start the seed nodes concurrently" taggedAs LongRunningTest in {

      runOn(seed1) {
        // test that first seed doesn't have to be started first
        Thread.sleep(3000)
      }

      runOn(seed1, seed2, seed3) {
        cluster.joinSeedNodes(seedNodes)
        runOn(seed3) {
          // it is allowed to call this several times (verifies ticket #3973)
          cluster.joinSeedNodes(seedNodes)
        }
        awaitMembersUp(3)
      }
      enterBarrier("after-1")
    }

    "join the seed nodes" taggedAs LongRunningTest in {
      runOn(ordinary1, ordinary2) {
        cluster.joinSeedNodes(seedNodes)
      }
      awaitMembersUp(roles.size)

      seedNodes.foreach { a =>
        cluster.state.members.find(_.address == a).get.appVersion should ===(Version("1.0"))
      }
      List(address(ordinary1), address(ordinary2)).foreach { a =>
        cluster.state.members.find(_.address == a).get.appVersion should ===(Version("2.0"))
      }

      enterBarrier("after-2")
    }
  }
}
