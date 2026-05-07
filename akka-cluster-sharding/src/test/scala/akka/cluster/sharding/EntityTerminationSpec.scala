/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.WithLogCapturing

/**
 * Verifies that the automatic restart on terminate/crash that is in place for remember entities does not apply
 * when remember entities is not enabled
 */
object EntityTerminationSpec {

  final case class EntityEnvelope(id: String, msg: Any)

  def config = ConfigFactory.parseString("""
      akka.loglevel=DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.state-store-mode = ddata
      # no leaks between test runs thank you
      akka.cluster.sharding.distributed-data.durable.keys = []
      akka.cluster.sharding.verbose-debug-logging = on
      akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
      akka.cluster.sharding.entity-restart-backoff = 250ms
    """.stripMargin)

  object StoppingActor {
    def props(): Props = Props(new StoppingActor)
  }
  class StoppingActor extends Actor {
    private var counter = 0
    def receive = {
      case "stop" => context.stop(self)
      case "ping" =>
        counter += 1
        sender() ! s"pong-$counter"
      case "passivate" => context.parent ! ShardRegion.Passivate("stop")
    }
  }
}

class EntityTerminationSpec extends AkkaSpec(EntityTerminationSpec.config) with ImplicitSender with WithLogCapturing {

  import EntityTerminationSpec._

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case EntityEnvelope(id, payload) => (id.toString, payload)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntityEnvelope(_, _)       => "1" // single shard for all entities
    case ShardRegion.StartEntity(_) => "1"
    case _                          => throw new IllegalArgumentException()
  }

  override def atStartup(): Unit = {
    // Form a one node cluster
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert(cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1))
  }

  "Sharding, when an entity terminates" must {

    "allow stop without passivation if not remembering entities" in {
      val sharding = ClusterSharding(system).start(
        "regular",
        StoppingActor.props(),
        ClusterShardingSettings(system),
        extractEntityId,
        extractShardId)

      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong-1")
      val entity = lastSender

      sharding ! EntityEnvelope("2", "ping")
      expectMsg("pong-1")

      watch(entity)
      sharding ! EntityEnvelope("1", "stop")
      expectTerminated(entity)

      Thread.sleep(400) // restart backoff is 250 ms
      sharding ! ShardRegion.GetShardRegionState
      val regionState = expectMsgType[ShardRegion.CurrentShardRegionState]
      regionState.shards should have size (1)
      regionState.shards.head.entityIds should be(Set("2"))

      // make sure the shard didn't crash (coverage for regression bug #29383)
      sharding ! EntityEnvelope("2", "ping")
      expectMsg("pong-2") // if it lost state we know it restarted
    }

    "automatically restart a terminating entity (not passivating) if remembering entities" in {
      val sharding = ClusterSharding(system).start(
        "remembering",
        StoppingActor.props(),
        ClusterShardingSettings(system).withRememberEntities(true),
        extractEntityId,
        extractShardId)

      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong-1")
      val entity = lastSender
      watch(entity)

      sharding ! EntityEnvelope("1", "stop")
      expectTerminated(entity)

      Thread.sleep(400) // restart backoff is 250 ms
      awaitAssert({
        sharding ! ShardRegion.GetShardRegionState
        val regionState = expectMsgType[ShardRegion.CurrentShardRegionState]
        regionState.shards should have size (1)
        regionState.shards.head.entityIds should have size (1)
      }, 2.seconds)
    }

    "allow terminating entity to passivate if remembering entities" in {
      val sharding = ClusterSharding(system).start(
        "remembering",
        StoppingActor.props(),
        ClusterShardingSettings(system).withRememberEntities(true),
        extractEntityId,
        extractShardId)

      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong-1")
      val entity = lastSender
      watch(entity)

      sharding ! EntityEnvelope("1", "passivate")
      expectTerminated(entity)
      Thread.sleep(400) // restart backoff is 250 ms

      sharding ! ShardRegion.GetShardRegionState
      val regionState = expectMsgType[ShardRegion.CurrentShardRegionState]
      regionState.shards should have size (1)
      regionState.shards.head.entityIds should have size (0)

    }

  }

}
