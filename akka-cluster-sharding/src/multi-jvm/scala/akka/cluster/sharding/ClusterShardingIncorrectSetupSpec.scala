/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.testkit._

object ClusterShardingIncorrectSetupSpecConfig
    extends MultiNodeClusterShardingConfig(additionalConfig = "akka.cluster.sharding.waiting-for-state-timeout = 100ms") {

  val first = role("first")
  val second = role("second")

}

class ClusterShardingIncorrectSetupMultiJvmNode1 extends ClusterShardingIncorrectSetupSpec
class ClusterShardingIncorrectSetupMultiJvmNode2 extends ClusterShardingIncorrectSetupSpec

abstract class ClusterShardingIncorrectSetupSpec
    extends MultiNodeClusterShardingSpec(ClusterShardingIncorrectSetupSpecConfig)
    with ImplicitSender {

  import ClusterShardingIncorrectSetupSpecConfig._

  "Cluster sharding" must {
    "log useful error message if sharding not started" in {
      awaitClusterUp(roles: _*)
      enterBarrier("cluster-up")
      runOn(first) {
        EventFilter.error(pattern = """Has ClusterSharding been started on all nodes?""").intercept {
          startSharding(system, typeName = "Entity", entityProps = TestActors.echoActorProps)
        }
      }
      enterBarrier("helpful error message logged")
    }
  }
}
