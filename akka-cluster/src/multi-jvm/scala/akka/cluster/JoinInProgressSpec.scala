/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object JoinInProgressMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(
    debugConfig(on = false).withFallback(ConfigFactory.parseString("""
          akka.cluster {
            # simulate delay in gossip by turning it off
            gossip-interval = 300 s
            failure-detector {
              threshold = 4
              acceptable-heartbeat-pause = 1 second
            }
          }""").withFallback(MultiNodeClusterSpec.clusterConfig)))
}

class JoinInProgressMultiJvmNode1 extends JoinInProgressSpec
class JoinInProgressMultiJvmNode2 extends JoinInProgressSpec

abstract class JoinInProgressSpec extends MultiNodeClusterSpec(JoinInProgressMultiJvmSpec) {

  import JoinInProgressMultiJvmSpec._

  "A cluster node" must {
    "send heartbeats immediately when joining to avoid false failure detection due to delayed gossip" taggedAs LongRunningTest in {

      runOn(first) {
        startClusterNode()
      }

      enterBarrier("first-started")

      runOn(second) {
        cluster.join(first)
      }

      runOn(first) {
        val until = Deadline.now + 5.seconds
        while (!until.isOverdue()) {
          Thread.sleep(200)
          cluster.failureDetector.isAvailable(second) should ===(true)
        }
      }

      enterBarrier("after")
    }
  }
}
