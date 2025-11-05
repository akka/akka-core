/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.MemberStatus.Up
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig

class MultiDcSpecConfig(crossDcConnections: Int = 5) extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString(s"""
      akka.cluster.multi-data-center.cross-data-center-connections = $crossDcConnections
    """).withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(first, second)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc1"
    """))

  nodeConfig(third, fourth, fifth)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc2"
    """))

  testTransport(on = true)
}

object MultiDcNormalConfig extends MultiDcSpecConfig()

class MultiDcMultiJvmNode1 extends MultiDcSpec(MultiDcNormalConfig)
class MultiDcMultiJvmNode2 extends MultiDcSpec(MultiDcNormalConfig)
class MultiDcMultiJvmNode3 extends MultiDcSpec(MultiDcNormalConfig)
class MultiDcMultiJvmNode4 extends MultiDcSpec(MultiDcNormalConfig)
class MultiDcMultiJvmNode5 extends MultiDcSpec(MultiDcNormalConfig)

object MultiDcFewCrossDcConnectionsConfig extends MultiDcSpecConfig(1)

class MultiDcFewCrossDcMultiJvmNode1 extends MultiDcSpec(MultiDcFewCrossDcConnectionsConfig)
class MultiDcFewCrossDcMultiJvmNode2 extends MultiDcSpec(MultiDcFewCrossDcConnectionsConfig)
class MultiDcFewCrossDcMultiJvmNode3 extends MultiDcSpec(MultiDcFewCrossDcConnectionsConfig)
class MultiDcFewCrossDcMultiJvmNode4 extends MultiDcSpec(MultiDcFewCrossDcConnectionsConfig)
class MultiDcFewCrossDcMultiJvmNode5 extends MultiDcSpec(MultiDcFewCrossDcConnectionsConfig)

abstract class MultiDcSpec(config: MultiDcSpecConfig) extends MultiNodeClusterSpec(config) {

  import config._

  "A cluster with multiple data centers" must {
    "be able to form" in {

      runOn(first) {
        cluster.join(first)
      }
      runOn(second, third, fourth) {
        cluster.join(first)
      }
      enterBarrier("form-cluster-join-attempt")

      runOn(first, second, third, fourth) {
        within(20.seconds) {
          awaitAssert(clusterView.members.filter(_.status == MemberStatus.Up) should have size 4)
        }
      }

      enterBarrier("cluster started")
    }

    "have a leader per data center" in {
      runOn(first, second) {
        cluster.settings.SelfDataCenter should ===("dc1")
        clusterView.leader shouldBe defined
        val dc1 = Set(address(first), address(second))
        dc1 should contain(clusterView.leader.get)
      }

      runOn(third, fourth) {
        cluster.settings.SelfDataCenter should ===("dc2")
        clusterView.leader shouldBe defined
        val dc2 = Set(address(third), address(fourth))
        dc2 should contain(clusterView.leader.get)
      }

      enterBarrier("leader per data center")
    }

    "be able to have data center member changes while there is inter data center unreachability" in within(20.seconds) {
      runOn(first) {
        testConductor.blackhole(first, third, Direction.Both).await
      }
      enterBarrier("inter-data-center unreachability")

      runOn(fifth) {
        cluster.join(third)
      }

      runOn(third, fourth, fifth) {
        // should be able to join and become up since the
        // unreachable is between dc1 and dc2,
        within(10.seconds) {
          awaitAssert(clusterView.members.filter(_.status == MemberStatus.Up) should have size 5)
        }
      }

      runOn(first) {
        testConductor.passThrough(first, third, Direction.Both).await
      }

      // should be able to join and become up since the
      // unreachable is between dc1 and dc2,
      within(10.seconds) {
        awaitAssert(clusterView.members.filter(_.status == MemberStatus.Up) should have size 5)
      }

      enterBarrier("inter-data-center unreachability end")
    }

    "be able to have data center member changes while there is unreachability in another data center" in within(
      20.seconds) {
      runOn(first) {
        testConductor.blackhole(first, second, Direction.Both).await
      }
      enterBarrier("other-data-center-internal-unreachable")

      runOn(third) {
        // FIXME This is already part of the cluster, is this intended? Joined on line 107
        cluster.join(fifth)
        // should be able to join and leave
        // since the unreachable nodes are inside of dc1
        cluster.leave(fourth)

        awaitAssert(clusterView.members.map(_.address) should not contain address(fourth))
        awaitAssert(
          clusterView.members.collect { case m if m.status == Up => m.address } should contain(address(fifth)))
      }

      enterBarrier("other-data-center-internal-unreachable changed")

      runOn(first) {
        testConductor.passThrough(first, second, Direction.Both).await
      }
      enterBarrier("other-data-center-internal-unreachable end")
    }

    "be able to down a member of another data-center" in within(20.seconds) {
      runOn(fifth) {
        cluster.down(address(second))
      }

      runOn(first, third, fifth) {
        awaitAssert(clusterView.members.map(_.address) should not contain address(second))
      }
      enterBarrier("cross-data-center-downed")
    }
  }
}
