/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Address
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus.Up
import akka.serialization.jackson.CborSerializable
import akka.testkit.ImplicitSender

object RollingUpdateShardAllocationSpecConfig
    extends MultiNodeClusterShardingConfig(
      additionalConfig = """
      akka.cluster.sharding {
        # speed up forming and handovers a bit
        retry-interval = 500ms
        waiting-for-state-timeout = 500ms
        rebalance-interval = 1s
        # we are leaving cluster nodes but they need to stay in test
        akka.coordinated-shutdown.terminate-actor-system = off
        # use the new LeastShardAllocationStrategy
        akka.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit = 1
      }
     """) {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  nodeConfig(first, second)(ConfigFactory.parseString("""
      akka.cluster.app-version = 1.0.0
      """))

  nodeConfig(third, fourth)(ConfigFactory.parseString("""
      akka.cluster.app-version = 1.0.1
      """))

}

object RollingUpdateShardAllocationSpec {
  object GiveMeYourHome {
    case class Get(id: String) extends CborSerializable
    case class Home(address: Address) extends CborSerializable

    val extractEntityId: ShardRegion.ExtractEntityId = {
      case g @ Get(id) => (id, g)
    }

    // shard == id to make testing easier
    val extractShardId: ShardRegion.ExtractShardId = {
      case Get(id) => id
      case _       => throw new IllegalArgumentException()
    }
  }

  class GiveMeYourHome extends Actor with ActorLogging {
    import GiveMeYourHome._

    val selfAddress = Cluster(context.system).selfAddress

    log.info("Started on {}", selfAddress)

    override def receive: Receive = {
      case Get(_) =>
        sender() ! Home(selfAddress)
    }
  }
}

class RollingUpdateShardAllocationSpecMultiJvmNode1 extends RollingUpdateShardAllocationSpec
class RollingUpdateShardAllocationSpecMultiJvmNode2 extends RollingUpdateShardAllocationSpec
class RollingUpdateShardAllocationSpecMultiJvmNode3 extends RollingUpdateShardAllocationSpec
class RollingUpdateShardAllocationSpecMultiJvmNode4 extends RollingUpdateShardAllocationSpec

abstract class RollingUpdateShardAllocationSpec
    extends MultiNodeClusterShardingSpec(RollingUpdateShardAllocationSpecConfig)
    with ImplicitSender {

  import RollingUpdateShardAllocationSpec._
  import RollingUpdateShardAllocationSpecConfig._

  val typeName = "home"

  def upMembers = cluster.state.members.filter(_.status == Up)

  "Cluster sharding" must {

    "form cluster" in {
      awaitClusterUp(first, second)
      enterBarrier("cluster-started")
    }
    lazy val shardRegion = startSharding(
      system,
      typeName = typeName,
      entityProps = Props[GiveMeYourHome](),
      extractEntityId = GiveMeYourHome.extractEntityId,
      extractShardId = GiveMeYourHome.extractShardId)

    "start cluster sharding on first" in {
      runOn(first, second) {

        // make sure both regions have completed registration before triggering entity allocation
        // so the folloing allocations end up as one on each node
        awaitAssert {
          shardRegion ! ShardRegion.GetCurrentRegions
          expectMsgType[ShardRegion.CurrentRegions].regions should have size (2)
        }

        shardRegion ! GiveMeYourHome.Get("id1")
        // started on either of the nodes
        val address1 = expectMsgType[GiveMeYourHome.Home].address

        shardRegion ! GiveMeYourHome.Get("id2")
        // started on the other of the nodes (because least
        val address2 = expectMsgType[GiveMeYourHome.Home].address

        // one on each node
        Set(address1, address2) should have size (2)
      }
      enterBarrier("first-version-started")
    }
    "start a rolling upgrade" in {
      join(third, first)

      runOn(first, second, third) {
        shardRegion

        // new shards should now go on third since that is the highest version,
        // however there is a race where the shard has not yet completed registration
        // with the coordinator and shards will be allocated on the old nodes, so we need
        // to make sure the third region has completed registration before trying
        // if we didn't the strategy will default it back to the old nodes
        awaitAssert {
          shardRegion ! ShardRegion.GetCurrentRegions
          expectMsgType[ShardRegion.CurrentRegions].regions should have size (3)
        }
      }
      enterBarrier("third-region-registered")
      runOn(first, second) {
        shardRegion ! GiveMeYourHome.Get("id3")
        expectMsgType[GiveMeYourHome.Home]
      }
      runOn(third) {
        // now third region should be only option as the other two are old versions
        // but first new allocated shard would anyway go there because of balance, so we
        // need to do more than one
        (3 to 5).foreach { n =>
          shardRegion ! GiveMeYourHome.Get(s"id$n")
          expectMsgType[GiveMeYourHome.Home].address should ===(Cluster(system).selfAddress)
        }
      }
      enterBarrier("rolling-upgrade-in-progress")
    }
    "complete a rolling upgrade" in {
      join(fourth, first)

      runOn(first) {
        val cluster = Cluster(system)
        cluster.leave(cluster.selfAddress)
      }
      runOn(second, third, fourth) {
        awaitAssert(upMembers.size should ===(3))
      }
      enterBarrier("first-left")

      runOn(second, third, fourth) {
        awaitAssert({
          shardRegion ! ShardRegion.GetCurrentRegions
          expectMsgType[ShardRegion.CurrentRegions].regions should have size (3)
        }, 30.seconds)
      }
      enterBarrier("sharding-handed-off")

      // trigger allocation (no verification because we don't know which id was on node 1)
      runOn(second, third, fourth) {
        awaitAssert {
          shardRegion ! GiveMeYourHome.Get("id1")
          expectMsgType[GiveMeYourHome.Home]

          shardRegion ! GiveMeYourHome.Get("id2")
          expectMsgType[GiveMeYourHome.Home]
        }
      }
      enterBarrier("first-allocated")

      runOn(second) {
        val cluster = Cluster(system)
        cluster.leave(cluster.selfAddress)
      }
      runOn(third, fourth) {
        // make sure coordinator has noticed there are only two regions
        awaitAssert({
          shardRegion ! ShardRegion.GetCurrentRegions
          expectMsgType[ShardRegion.CurrentRegions].regions should have size (2)
        }, 30.seconds)
      }
      enterBarrier("second-left")

      // trigger allocation and verify where each was started
      runOn(third, fourth) {
        awaitAssert {
          shardRegion ! GiveMeYourHome.Get("id1")
          val address1 = expectMsgType[GiveMeYourHome.Home].address
          upMembers.map(_.address) should contain(address1)

          shardRegion ! GiveMeYourHome.Get("id2")
          val address2 = expectMsgType[GiveMeYourHome.Home].address
          upMembers.map(_.address) should contain(address2)
        }
      }
      enterBarrier("completo")
    }

  }

}
