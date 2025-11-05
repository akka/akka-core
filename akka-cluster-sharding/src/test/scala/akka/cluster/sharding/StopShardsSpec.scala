/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardCoordinator.Internal.ShardStopped
import akka.cluster.sharding.ShardRegion.CurrentRegions
import akka.cluster.sharding.ShardRegion.GetCurrentRegions
import akka.testkit.AkkaSpec
import akka.testkit.DeadLettersFilter
import akka.testkit.TestEvent.Mute
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object StopShardsSpec {

  def config =
    ConfigFactory.parseString("""
        akka.loglevel = DEBUG
        akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
        akka.actor.provider = "cluster"
        akka.remote.artery.canonical.port = 0
        akka.test.single-expect-default = 5 s
        akka.cluster.sharding.distributed-data.durable.keys = []
        akka.cluster.sharding.remember-entities = off
        akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
        akka.cluster.jmx.enabled = off
        akka.cluster.sharding.verbose-debug-logging = on
        akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
        akka.remote.artery.canonical.hostname = "127.0.0.1"
        """)

  val shardTypeName = "stopping-entities"

  val numberOfShards = 3

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
    case _        => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int                    => (msg % 10).toString
    case ShardRegion.StartEntity(id) => (id.toLong % numberOfShards).toString
    case _                           => throw new IllegalArgumentException()
  }

  case class Pong(actorRef: ActorRef)
  class EntityActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case _ =>
        log.debug("ping")
        sender() ! context.self
    }
  }
}

class StopShardsSpec extends AkkaSpec(StopShardsSpec.config) with WithLogCapturing {
  import StopShardsSpec._

  // mute logging of deadLetters
  system.eventStream.publish(Mute(DeadLettersFilter[Any]))

  private val sysA = system
  private val sysB = ActorSystem(system.name, system.settings.config)

  private val pA = TestProbe()(sysA)
  private val pB = TestProbe()(sysB)

  private val regionA = startShardregion(sysA)
  private val regionB = startShardregion(sysB)

  "The StopShards command" must {

    "form a cluster" in {
      Cluster(sysA).join(Cluster(sysA).selfAddress) // coordinator on A
      awaitAssert(Cluster(sysA).selfMember.status shouldEqual MemberStatus.Up, 3.seconds)

      Cluster(sysB).join(Cluster(sysA).selfAddress)
      awaitAssert(Cluster(sysB).selfMember.status shouldEqual MemberStatus.Up, 3.seconds)

      // wait for all regions to be registered
      pA.awaitAssert({
        regionA.tell(GetCurrentRegions, pA.ref)
        pA.expectMsgType[CurrentRegions].regions should have size (2)
      }, 10.seconds)
    }

    "start entities in a few shards, then stop the shards" in {

      val allShards = (1 to 10).map { i =>
        regionA.tell(i, pA.ref)
        val entityRef = pA.expectMsgType[ActorRef]
        pA.watch(entityRef) // so we can verify terminated later
        extractShardId(i)
      }.toSet

      regionB.tell(ShardCoordinator.Internal.StopShards(allShards), pB.ref)
      (1 to allShards.size).foreach { _ =>
        // one ack for each shard stopped
        pB.expectMsgType[ShardStopped].shard
      }

      // all entities stopped
      (1 to 10).foreach(_ => pA.expectMsgType[Terminated])
    }
  }

  def startShardregion(sys: ActorSystem): ActorRef =
    ClusterSharding(sys).start(
      shardTypeName,
      Props[EntityActor](),
      ClusterShardingSettings(system),
      extractEntityId,
      extractShardId)

}
