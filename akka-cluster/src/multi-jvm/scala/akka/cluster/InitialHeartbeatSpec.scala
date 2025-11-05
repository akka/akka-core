/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.cluster.ClusterEvent.CurrentClusterState
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object InitialHeartbeatMultiJvmSpec extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")

  commonConfig(
    debugConfig(on = false).withFallback(ConfigFactory.parseString("""
      akka.cluster.failure-detector.threshold = 4""")).withFallback(MultiNodeClusterSpec.clusterConfig))

  testTransport(on = true)
}

class InitialHeartbeatMultiJvmNode1 extends InitialHeartbeatSpec
class InitialHeartbeatMultiJvmNode2 extends InitialHeartbeatSpec
class InitialHeartbeatMultiJvmNode3 extends InitialHeartbeatSpec

abstract class InitialHeartbeatSpec extends MultiNodeClusterSpec(InitialHeartbeatMultiJvmSpec) {

  import InitialHeartbeatMultiJvmSpec._

  muteMarkingAsUnreachable()

  "A member" must {

    "detect failure even though no heartbeats have been received" taggedAs LongRunningTest in {
      val firstAddress = address(first)
      val secondAddress = address(second)
      awaitClusterUp(first)

      runOn(first) {
        within(10 seconds) {
          awaitAssert({
            cluster.sendCurrentClusterState(testActor)
            expectMsgType[CurrentClusterState].members.map(_.address) should contain(secondAddress)
          }, interval = 50.millis)
        }
      }
      runOn(second) {
        cluster.join(first)
        within(10 seconds) {
          awaitAssert({
            cluster.sendCurrentClusterState(testActor)
            expectMsgType[CurrentClusterState].members.map(_.address) should contain(firstAddress)
          }, interval = 50.millis)
        }
      }
      enterBarrier("second-joined")

      runOn(controller) {
        // It is likely that second has not started heartbeating to first yet,
        // and when it does the messages doesn't go through and the first extra heartbeat is triggered.
        // If the first heartbeat arrives, it will detect the failure anyway but not really exercise the
        // part that we are trying to test here.
        testConductor.blackhole(first, second, Direction.Both).await
      }

      runOn(second) {
        within(15 seconds) {
          awaitCond(!cluster.failureDetector.isAvailable(first))
        }
      }

      enterBarrier("after-1")
    }
  }
}
