/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.WithLogCapturing

/**
 * Covers some corner cases around sending triggering an entity with StartEntity
 */
object StartEntitySpec {

  final case class EntityEnvelope(id: String, msg: Any)

  def config = ConfigFactory.parseString("""
      akka.loglevel=DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.state-store-mode = ddata
      akka.cluster.sharding.remember-entities = on
      # no leaks between test runs thank you
      akka.cluster.sharding.distributed-data.durable.keys = []
      akka.cluster.sharding.verbose-debug-logging = on
      akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    """.stripMargin)

  object EntityActor {
    def props(): Props = Props(new EntityActor)
  }
  class EntityActor extends Actor {
    private var waitingForPassivateAck: Option[ActorRef] = None
    override def receive: Receive = {
      case "ping" =>
        sender() ! "pong"
      case "passivate" =>
        context.parent ! ShardRegion.Passivate("complete-passivation")
        waitingForPassivateAck = Some(sender())
      case "simulate-slow-passivate" =>
        context.parent ! ShardRegion.Passivate("slow-passivate-stop")
        waitingForPassivateAck = Some(sender())
      case "slow-passivate-stop" =>
        // actually, we just don't stop, keeping the passivation state forever for this test
        waitingForPassivateAck.foreach(_ ! "slow-passivate-ack")
        waitingForPassivateAck = None
      case "complete-passivation" | "just-stop" =>
        context.stop(self)
    }
  }

}

class StartEntitySpec extends AkkaSpec(StartEntitySpec.config) with ImplicitSender with WithLogCapturing {
  import StartEntitySpec._

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case EntityEnvelope(id, payload) => (id.toString, payload)
    case _                           => throw new IllegalArgumentException()
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

  "StartEntity while entity is passivating" should {
    "start it again when the entity terminates" in {
      val sharding = ClusterSharding(system).start(
        "start-entity-1",
        EntityActor.props(),
        ClusterShardingSettings(system),
        extractEntityId,
        extractShardId)

      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong")
      val entity = lastSender

      sharding ! EntityEnvelope("1", "simulate-slow-passivate")
      expectMsg("slow-passivate-ack")

      // entity is now in passivating state in shard
      // bypass region and send start entity directly to shard
      system.actorSelection(entity.path.parent) ! ShardRegion.StartEntity("1")
      // bypass sharding and tell entity to complete passivation
      entity ! "complete-passivation"

      // should trigger start of entity again, and an ack
      expectMsg(ShardRegion.StartEntityAck("1", "1"))
      awaitAssert({
        sharding ! ShardRegion.GetShardRegionState
        val state = expectMsgType[ShardRegion.CurrentShardRegionState]
        state.shards should have size (1)
        state.shards.head.entityIds should ===(Set("1"))
      })
    }
  }

  // entity crashed and before restart-backoff hit we sent it a StartEntity
  "StartEntity while the entity is waiting for restart" should {
    "restart it immediately" in {
      val sharding = ClusterSharding(system).start(
        "start-entity-2",
        EntityActor.props(),
        ClusterShardingSettings(system),
        extractEntityId,
        extractShardId)
      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong")
      val entity = lastSender

      // stop without passivation
      entity ! "just-stop"

      // Make sure the shard has processed the termination
      awaitAssert({
        sharding ! ShardRegion.GetShardRegionState
        val state = expectMsgType[ShardRegion.CurrentShardRegionState]
        state.shards should have size (1)
        state.shards.head.entityIds should ===(Set.empty[String])
      })

      // the backoff is 10s by default, so plenty time to
      // bypass region and send start entity directly to shard
      system.actorSelection(entity.path.parent) ! ShardRegion.StartEntity("1")
      expectMsg(ShardRegion.StartEntityAck("1", "1"))
      awaitAssert({
        sharding ! ShardRegion.GetShardRegionState
        val state = expectMsgType[ShardRegion.CurrentShardRegionState]
        state.shards should have size (1)
        state.shards.head.entityIds should ===(Set("1"))
      })
    }
  }

  "StartEntity while the entity is queued remember stop" should {
    "start it again when that is done" in {
      // this is hard to do deterministically
      val sharding = ClusterSharding(system).start(
        "start-entity-3",
        EntityActor.props(),
        ClusterShardingSettings(system),
        extractEntityId,
        extractShardId)
      sharding ! EntityEnvelope("1", "ping")
      expectMsg("pong")
      val entity = lastSender
      watch(entity)

      // resolve before passivation to save some time
      val shard = system.actorSelection(entity.path.parent).resolveOne(3.seconds).futureValue

      // stop passivation
      entity ! "passivate"
      // store of stop happens after passivation when entity has terminated
      expectTerminated(entity)
      shard ! ShardRegion.StartEntity("1") // if we are lucky this happens while remember stop is in progress

      // regardless we should get an ack and the entity should be alive
      expectMsg(ShardRegion.StartEntityAck("1", "1"))
      awaitAssert({
        sharding ! ShardRegion.GetShardRegionState
        val state = expectMsgType[ShardRegion.CurrentShardRegionState]
        state.shards should have size (1)
        state.shards.head.entityIds should ===(Set("1"))
      })

    }
  }

}
