/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.Shard.GetShardStats
import akka.cluster.sharding.Shard.ShardStats
import akka.cluster.sharding.ShardRegion.StartEntity
import akka.cluster.sharding.ShardRegion.StartEntityAck
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.WithLogCapturing

object RememberEntitiesAndStartEntitySpec {
  class EntityActor extends Actor {
    override def receive: Receive = {
      case "give-me-shard" => sender() ! context.parent
      case msg             => sender() ! msg
    }
  }

  case class EntityEnvelope(entityId: Int, msg: Any)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case EntityEnvelope(id, payload) => (id.toString, payload)
    case _                           => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntityEnvelope(id, _) => (id % 10).toString
    case StartEntity(id)       => (id.toInt % 10).toString
    case _                     => throw new IllegalArgumentException()
  }

  val config = ConfigFactory.parseString("""
      akka.loglevel=DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.verbose-debug-logging = on
      akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
      # no leaks between test runs thank you
      akka.cluster.sharding.distributed-data.durable.keys = []
    """.stripMargin)
}

// this test covers remember entities + StartEntity
class RememberEntitiesAndStartEntitySpec
    extends AkkaSpec(RememberEntitiesAndStartEntitySpec.config)
    with AnyWordSpecLike
    with ImplicitSender
    with WithLogCapturing {

  import RememberEntitiesAndStartEntitySpec._

  override def atStartup(): Unit = {
    // Form a one node cluster
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert(cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1))
  }

  "Sharding" must {

    "remember entities started with StartEntity" in {
      val sharding = ClusterSharding(system).start(
        s"startEntity",
        Props[EntityActor](),
        ClusterShardingSettings(system).withRememberEntities(true),
        extractEntityId,
        extractShardId)

      sharding ! StartEntity("1")
      expectMsg(StartEntityAck("1", "1"))
      val shard = lastSender

      watch(shard)
      shard ! PoisonPill
      expectTerminated(shard)

      // trigger shard start by messaging other actor in it
      system.log.info("Starting shard again")
      // race condition between this message and region getting the termination message, we may need to retry
      val secondShardIncarnation = awaitAssert {
        sharding ! EntityEnvelope(11, "give-me-shard")
        expectMsgType[ActorRef](1.second) // short timeout, retry via awaitAssert
      }

      awaitAssert {
        secondShardIncarnation ! GetShardStats
        // the remembered 1 and 11 which we just triggered start of
        expectMsg(1.second, ShardStats("1", 2)) // short timeout, retry via awaitAssert
      }
    }
  }

}
