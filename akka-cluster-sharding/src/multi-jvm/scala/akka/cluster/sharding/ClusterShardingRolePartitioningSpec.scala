/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.cluster.sharding.ShardRegion.{ ClusterShardingStats, GetClusterShardingStats }
import akka.testkit._

// Tests the case where cluster roles are used with cluster.min-nr-of-members, no per role min set
// with 5 node cluster, 2 roles: 3 nodes role R1, 2 nodes role R2
// See https://github.com/akka/akka-core/issues/28177#issuecomment-555013145
object E1 {
  val TypeKey = "Datatype1"
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case id: String => (id, id)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case id: String => id
    case _          => throw new IllegalArgumentException()
  }
}

object E2 {
  val TypeKey = "Datatype2"
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case id: Int => (id.toString, id)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case id: Int => id.toString
    case _       => throw new IllegalArgumentException()
  }
}

abstract class ClusterShardingMinMembersPerRoleConfig extends MultiNodeClusterShardingConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  val r1Config: Config = ConfigFactory.parseString("""akka.cluster.roles = [ "R1" ]""")
  val r2Config: Config = ConfigFactory.parseString("""akka.cluster.roles = [ "R2" ]""")

}

object ClusterShardingMinMembersPerRoleNotConfiguredConfig extends ClusterShardingMinMembersPerRoleConfig {

  val commonRoleConfig: Config = ConfigFactory.parseString("akka.cluster.min-nr-of-members = 2")

  nodeConfig(first, second, third)(r1Config.withFallback(commonRoleConfig))

  nodeConfig(fourth, fifth)(r2Config.withFallback(commonRoleConfig))
}

object ClusterShardingMinMembersPerRoleConfiguredConfig extends ClusterShardingMinMembersPerRoleConfig {

  val commonRoleConfig =
    ConfigFactory.parseString("""
    akka.cluster.min-nr-of-members = 3                                                  
    akka.cluster.role.R1.min-nr-of-members = 3
    akka.cluster.role.R2.min-nr-of-members = 2
    """)

  nodeConfig(first, second, third)(r1Config.withFallback(commonRoleConfig))

  nodeConfig(fourth, fifth)(r2Config.withFallback(commonRoleConfig))
}

abstract class ClusterShardingMinMembersPerRoleNotConfiguredSpec
    extends ClusterShardingRolePartitioningSpec(ClusterShardingMinMembersPerRoleNotConfiguredConfig)

class ClusterShardingMinMembersPerRoleNotConfiguredMultiJvmNode1
    extends ClusterShardingMinMembersPerRoleNotConfiguredSpec
class ClusterShardingMinMembersPerRoleNotConfiguredMultiJvmNode2
    extends ClusterShardingMinMembersPerRoleNotConfiguredSpec
class ClusterShardingMinMembersPerRoleNotConfiguredMultiJvmNode3
    extends ClusterShardingMinMembersPerRoleNotConfiguredSpec
class ClusterShardingMinMembersPerRoleNotConfiguredMultiJvmNode4
    extends ClusterShardingMinMembersPerRoleNotConfiguredSpec
class ClusterShardingMinMembersPerRoleNotConfiguredMultiJvmNode5
    extends ClusterShardingMinMembersPerRoleNotConfiguredSpec

abstract class ClusterShardingMinMembersPerRoleSpec
    extends ClusterShardingRolePartitioningSpec(ClusterShardingMinMembersPerRoleConfiguredConfig)

class ClusterShardingMinMembersPerRoleSpecMultiJvmNode1 extends ClusterShardingMinMembersPerRoleSpec
class ClusterShardingMinMembersPerRoleSpecMultiJvmNode2 extends ClusterShardingMinMembersPerRoleSpec
class ClusterShardingMinMembersPerRoleSpecMultiJvmNode3 extends ClusterShardingMinMembersPerRoleSpec
class ClusterShardingMinMembersPerRoleSpecMultiJvmNode4 extends ClusterShardingMinMembersPerRoleSpec
class ClusterShardingMinMembersPerRoleSpecMultiJvmNode5 extends ClusterShardingMinMembersPerRoleSpec

abstract class ClusterShardingRolePartitioningSpec(multiNodeConfig: ClusterShardingMinMembersPerRoleConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import multiNodeConfig._

  private val fourthAddress = node(fourth).address
  private val fifthAddress = node(fifth).address

  "Cluster Sharding with roles" must {

    "start the cluster, await convergence, init sharding on every node: 2 data types, 'akka.cluster.min-nr-of-members=2', partition shard location by 2 roles" in {
      // start sharding early
      startSharding(
        system,
        typeName = E1.TypeKey,
        entityProps = TestActors.echoActorProps,
        // nodes 1,2,3: role R1, shard region E1, proxy region E2
        settings = settings.withRole("R1"),
        extractEntityId = E1.extractEntityId,
        extractShardId = E1.extractShardId)

      // when run on first, second and third (role R1) proxy region is started
      startSharding(
        system,
        typeName = E2.TypeKey,
        entityProps = TestActors.echoActorProps,
        // nodes 4,5: role R2, shard region E2, proxy region E1
        settings = settings.withRole("R2"),
        extractEntityId = E2.extractEntityId,
        extractShardId = E2.extractShardId)

      awaitClusterUp(first, second, third, fourth, fifth)
      enterBarrier(s"${roles.size}-up")
    }

    // https://github.com/akka/akka-core/issues/28177
    "access role R2 (nodes 4,5) from one of the proxy nodes (1,2,3)" in {
      runOn(first) {

        // have first message reach the entity from a proxy with 2 nodes of role R2 and 'min-nr-of-members' set globally versus per role (nodes 4,5, with 1,2,3 proxying)
        // RegisterProxy messages from nodes 1,2,3 are deadlettered
        // Register messages sent are eventually successful on the fifth node, once coordinator moves to active state
        val region = ClusterSharding(system).shardRegion(E2.TypeKey)
        (1 to 20).foreach { n =>
          region ! n
          expectMsg(n) // R2 entity received, does not timeout
        }

        region ! GetClusterShardingStats(10.seconds)
        val stats = expectMsgType[ClusterShardingStats]

        withClue(stats) {
          stats.regions.keySet shouldEqual Set(fourthAddress, fifthAddress)
          stats.regions.valuesIterator.flatMap(_.stats.valuesIterator).sum shouldEqual 20
        }
      }
      enterBarrier("proxy-node-other-role-to-shard")

    }
  }
}
