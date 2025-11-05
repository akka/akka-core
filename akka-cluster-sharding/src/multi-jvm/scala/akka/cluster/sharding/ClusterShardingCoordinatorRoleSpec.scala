/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.sharding.MultiNodeClusterShardingSpec.EntityActor
import akka.testkit._

class ClusterShardingCoordinatorRoleSpecConfig(
    mode: String,
    rememberEntities: Boolean,
    rememberEntitiesStore: String = ClusterShardingSettings.RememberEntitiesStoreDData)
    extends MultiNodeClusterShardingConfig(
      mode,
      rememberEntities,
      rememberEntitiesStore = rememberEntitiesStore,
      additionalConfig = """
  akka.cluster.sharding {
    role = shard
    coordinator-singleton-role-override = off
    coordinator-singleton.role = coordinator
    entity-restart-backoff = 100 ms
  }  
  """) {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  val coordinatorConfig: Config = ConfigFactory.parseString("""akka.cluster.roles = [ "coordinator" ]""")
  val shardConfig: Config = ConfigFactory.parseString("""akka.cluster.roles = [ "shard" ]""")

  nodeConfig(first, second)(coordinatorConfig)

  nodeConfig(third, fourth)(shardConfig)

}

class DDataClusterShardingCoordinatorRoleSpec
    extends ClusterShardingCoordinatorRoleSpec(
      new ClusterShardingCoordinatorRoleSpecConfig(
        ClusterShardingSettings.StateStoreModeDData,
        rememberEntities = false))
class DDataClusterShardingCoordinatorRoleSpecMultiJvmNode1 extends DDataClusterShardingCoordinatorRoleSpec
class DDataClusterShardingCoordinatorRoleSpecMultiJvmNode2 extends DDataClusterShardingCoordinatorRoleSpec
class DDataClusterShardingCoordinatorRoleSpecMultiJvmNode3 extends DDataClusterShardingCoordinatorRoleSpec
class DDataClusterShardingCoordinatorRoleSpecMultiJvmNode4 extends DDataClusterShardingCoordinatorRoleSpec

class DDataRememberClusterShardingCoordinatorRoleSpec
    extends ClusterShardingCoordinatorRoleSpec(
      new ClusterShardingCoordinatorRoleSpecConfig(
        ClusterShardingSettings.StateStoreModeDData,
        rememberEntities = true))
class DDataRememberClusterShardingCoordinatorRoleSpecMultiJvmNode1
    extends DDataRememberClusterShardingCoordinatorRoleSpec
class DDataRememberClusterShardingCoordinatorRoleSpecMultiJvmNode2
    extends DDataRememberClusterShardingCoordinatorRoleSpec
class DDataRememberClusterShardingCoordinatorRoleSpecMultiJvmNode3
    extends DDataRememberClusterShardingCoordinatorRoleSpec
class DDataRememberClusterShardingCoordinatorRoleSpecMultiJvmNode4
    extends DDataRememberClusterShardingCoordinatorRoleSpec

abstract class ClusterShardingCoordinatorRoleSpec(multiNodeConfig: ClusterShardingCoordinatorRoleSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {

  import multiNodeConfig._

  def startSharding(probe: ActorRef): ActorRef = {
    startSharding(
      system,
      typeName = "Entity",
      entityProps = Props(new EntityActor(probe)),
      settings = ClusterShardingSettings(system).withRememberEntities(multiNodeConfig.rememberEntities),
      extractEntityId = MultiNodeClusterShardingSpec.intExtractEntityId,
      extractShardId = MultiNodeClusterShardingSpec.intExtractShardId)
  }

  private lazy val region = ClusterSharding(system).shardRegion("Entity")

  private val entityProbe = TestProbe()

  s"Cluster sharding (${multiNodeConfig.mode}, remember ${multiNodeConfig.rememberEntities}) with separate coordinator role" must {

    "use nodes with shard role" in within(30.seconds) {
      startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second, third))

      join(first, first)
      join(second, first)
      join(third, first)
      startSharding(entityProbe.ref)

      runOn(third) {
        region ! 1
        expectMsg(1)
        lastSender.path.address.hasLocalScope should ===(true)
        entityProbe.expectMsg(EntityActor.Started(lastSender))
      }
      runOn(first, second) {
        region ! 1
        expectMsg(1)
        lastSender.path should be(node(third) / "system" / "sharding" / "Entity" / "1" / "1")
        entityProbe.expectNoMessage()
      }

      enterBarrier("first-ok")

      if (rememberEntities) {
        runOn(third) {
          region ! 2
          expectMsg(2)
          watch(lastSender)
          val ref = lastSender
          ref ! PoisonPill
          expectTerminated(ref)
          entityProbe.expectMsg(EntityActor.Started(ref))
          // and then started again by remember entities
          // not same ActorRef, but same path
          entityProbe.expectMsgType[EntityActor.Started].ref.path should be(ref.path)
        }
        runOn(first, second) {
          entityProbe.expectNoMessage()
        }

      }

      enterBarrier("after-1")
    }

    if (rememberEntities) {
      "restart remembered entities" in {
        join(fourth, second)

        runOn(first) {
          cluster.leave(third)
          cluster.leave(first)
        }
        enterBarrier("first left")

        runOn(fourth) {
          // started by remember entities on new node
          entityProbe.expectMsgType[EntityActor.Started](20.seconds)
        }
        runOn(second) {
          entityProbe.expectNoMessage()
        }

        enterBarrier("after-2")
      }
    }

  }
}
