/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorRef, Address, Props }
import akka.cluster.{ Cluster, MemberStatus }
import akka.cluster.sharding.ShardRegion.{ CurrentRegions, GetCurrentRegions }
import akka.remote.testconductor.RoleName
import akka.serialization.jackson.CborSerializable
import akka.testkit._

object MultiDcClusterShardingSpec {
  sealed trait EntityMsg extends CborSerializable {
    def id: String
  }
  final case class Ping(id: String) extends EntityMsg
  final case class GetCount(id: String) extends EntityMsg

  class Entity extends Actor {
    var count = 0
    def receive = {
      case Ping(_) =>
        count += 1
        sender() ! self
      case GetCount(_) =>
        sender() ! count
    }
  }

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case m: EntityMsg => (m.id, m)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case m: EntityMsg => m.id.charAt(0).toString
    case _            => throw new IllegalArgumentException()
  }
}

object MultiDcClusterShardingSpecConfig
    extends MultiNodeClusterShardingConfig(
      loglevel = "DEBUG", //issue #23741
      additionalConfig = """
    akka.cluster {
      debug.verbose-heartbeat-logging = on
      debug.verbose-gossip-logging = on
      sharding.retry-interval = 200ms
    }
    """) {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  nodeConfig(first, second) {
    ConfigFactory.parseString("akka.cluster.multi-data-center.self-data-center = DC1")
  }

  nodeConfig(third, fourth) {
    ConfigFactory.parseString("akka.cluster.multi-data-center.self-data-center = DC2")
  }
}

class MultiDcClusterShardingSpecMultiJvmNode1 extends MultiDcClusterShardingSpec
class MultiDcClusterShardingSpecMultiJvmNode2 extends MultiDcClusterShardingSpec
class MultiDcClusterShardingSpecMultiJvmNode3 extends MultiDcClusterShardingSpec
class MultiDcClusterShardingSpecMultiJvmNode4 extends MultiDcClusterShardingSpec

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcClusterShardingSpec
    extends MultiNodeClusterShardingSpec(MultiDcClusterShardingSpecConfig)
    with ImplicitSender {
  import MultiDcClusterShardingSpec._
  import MultiDcClusterShardingSpecConfig._

  def join(from: RoleName, to: RoleName): Unit = {
    join(
      from,
      to, {
        startSharding()
        withClue(
          s"Failed waiting for ${cluster.selfUniqueAddress} to be up. Current state: ${cluster.state}" + cluster.state) {
          within(15.seconds) {
            awaitAssert(cluster.state.members.exists { m =>
              m.uniqueAddress == cluster.selfUniqueAddress && m.status == MemberStatus.Up
            } should be(true))
          }
        }
      })
  }

  def startSharding(): Unit = {
    startSharding(
      system,
      typeName = "Entity",
      entityProps = Props[Entity](),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId)
  }

  lazy val region = ClusterSharding(system).shardRegion("Entity")

  private def fillAddress(a: Address): Address =
    if (a.hasLocalScope) Cluster(system).selfAddress else a

  private def assertCurrentRegions(expected: Set[Address]): Unit = {
    awaitAssert({
      val p = TestProbe()
      region.tell(GetCurrentRegions, p.ref)
      p.expectMsg(CurrentRegions(expected))
    }, 10.seconds)
  }

  "Cluster sharding in multi data center cluster" must {
    "join cluster" in within(20.seconds) {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      awaitAssert({
        withClue(s"Members: ${Cluster(system).state}") {
          Cluster(system).state.members.size should ===(4)
          Cluster(system).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }
      }, 10.seconds)

      runOn(first, second) {
        assertCurrentRegions(Set(first, second).map(r => node(r).address))
      }
      runOn(third, fourth) {
        assertCurrentRegions(Set(third, fourth).map(r => node(r).address))
      }

      enterBarrier("after-1")
    }

    "initialize shards" in {
      runOn(first) {
        val locations = (for (n <- 1 to 10) yield {
          val id = n.toString
          region ! Ping(id)
          id -> expectMsgType[ActorRef]
        }).toMap
        val firstAddress = node(first).address
        val secondAddress = node(second).address
        val hosts = locations.values.map(ref => fillAddress(ref.path.address)).toSet
        hosts should ===(Set(firstAddress, secondAddress))
      }
      runOn(third) {
        val locations = (for (n <- 1 to 10) yield {
          val id = n.toString
          region ! Ping(id)
          val ref1 = expectMsgType[ActorRef]
          region ! Ping(id)
          val ref2 = expectMsgType[ActorRef]
          ref1 should ===(ref2)
          id -> ref1
        }).toMap
        val thirdAddress = node(third).address
        val fourthAddress = node(fourth).address
        val hosts = locations.values.map(ref => fillAddress(ref.path.address)).toSet
        hosts should ===(Set(thirdAddress, fourthAddress))
      }
      enterBarrier("after-2")
    }

    "not mix entities in different data centers" in {
      runOn(second) {
        region ! GetCount("5")
        expectMsg(1)
      }
      runOn(fourth) {
        region ! GetCount("5")
        expectMsg(2)
      }
      enterBarrier("after-3")
    }

    "allow proxy within same data center" in {
      runOn(second) {
        val proxy = ClusterSharding(system).startProxy(
          typeName = "Entity",
          role = None,
          dataCenter = None, // by default use own DC
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)
        proxy ! GetCount("5")
        expectMsg(1)
      }
      enterBarrier("after-4")
    }

    "allow proxy across different data centers" in {
      runOn(second) {
        val proxy = ClusterSharding(system).startProxy(
          typeName = "Entity",
          role = None,
          dataCenter = Some("DC2"), // proxy to other DC
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)

        proxy ! GetCount("5")
        expectMsg(2)
      }
      enterBarrier("after-5")
    }

  }
}
