/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.{ MemberStatus, MultiNodeClusterSpec }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }

object MultiDcClusterSingletonSpecConfig extends MultiNodeConfig {
  val first: RoleName = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
        akka.loglevel = DEBUG
      """).withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(first)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc1"
    """))

  nodeConfig(second, third)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc2"
    """))

  testTransport(on = true)
}

class MultiDcClusterSingletonMultiJvmNode1 extends MultiDcClusterSingletonSpec
class MultiDcClusterSingletonMultiJvmNode2 extends MultiDcClusterSingletonSpec
class MultiDcClusterSingletonMultiJvmNode3 extends MultiDcClusterSingletonSpec

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcClusterSingletonSpec
    extends MultiNodeSpec(MultiDcClusterSingletonSpecConfig)
    with MultiNodeTypedClusterSpec {

  import MultiDcClusterSingletonSpecConfig._
  import MultiDcPinger._

  "A cluster with multiple data centers" must {
    "be able to form" in {
      runOn(first) {
        cluster.manager ! Join(cluster.selfMember.address)
      }
      runOn(second, third) {
        cluster.manager ! Join(first)
      }
      enterBarrier("form-cluster-join-attempt")
      runOn(first, second, third) {
        within(20.seconds) {
          awaitAssert(clusterView.members.filter(_.status == MemberStatus.Up) should have size 3)
        }
      }
      enterBarrier("cluster started")
    }

    "be able to create and ping singleton in same DC" in {
      runOn(first) {
        val singleton = ClusterSingleton(typedSystem)
        val pinger = singleton.init(SingletonActor(MultiDcPinger(), "ping").withStopMessage(NoMore))
        val probe = TestProbe[Pong]()
        pinger ! Ping(probe.ref)
        probe.expectMessage(Pong("dc1"))
        enterBarrier("singleton-up")
      }
      runOn(second, third) {
        enterBarrier("singleton-up")
      }
    }

    "be able to ping singleton via proxy in another dc" in {
      runOn(second) {
        val singleton = ClusterSingleton(system.toTyped)
        val pinger = singleton.init(
          SingletonActor(MultiDcPinger(), "ping")
            .withStopMessage(NoMore)
            .withSettings(ClusterSingletonSettings(typedSystem).withDataCenter("dc1")))
        val probe = TestProbe[Pong]()
        pinger ! Ping(probe.ref)
        probe.expectMessage(Pong("dc1"))
      }

      enterBarrier("singleton-pinged")
    }

    "be able to target singleton with the same name in own dc " in {
      runOn(second, third) {
        val singleton = ClusterSingleton(typedSystem)
        val pinger = singleton.init(SingletonActor(MultiDcPinger(), "ping").withStopMessage(NoMore))
        val probe = TestProbe[Pong]()
        pinger ! Ping(probe.ref)
        probe.expectMessage(Pong("dc2"))
      }

      enterBarrier("singleton-pinged-own-dc")
    }
  }
}
