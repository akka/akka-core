/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, ExtendedActorSystem, NoSerializationVerificationNeeded, PoisonPill, Props }
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.sharding.ShardCoordinator.Internal.ShardStopped
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.{ ExtractEntityId, ExtractShardId, HandOffStopper, Msg }
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.testkit.WithLogCapturing

object ClusterShardingInternalsSpec {
  case class HandOffStopMessage() extends NoSerializationVerificationNeeded
  class EmptyHandlerActor extends Actor {
    override def receive: Receive = {
      case _ =>
    }

    override def postStop(): Unit = {
      super.postStop()
    }
  }
}

class ClusterShardingInternalsSpec extends AkkaSpec("""
    |akka.actor.provider = cluster
    |akka.remote.artery.canonical.port = 0
    |akka.loglevel = DEBUG
    |akka.cluster.sharding.verbose-debug-logging = on
    |akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    |akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    |""".stripMargin) with WithLogCapturing {
  import ClusterShardingInternalsSpec._

  case class StartingProxy(
      typeName: String,
      role: Option[String],
      dataCenter: Option[DataCenter],
      extractEntityId: ExtractEntityId,
      extractShardId: ExtractShardId)

  val probe = TestProbe()

  val clusterSharding = new ClusterSharding(system.asInstanceOf[ExtendedActorSystem]) {
    override def startProxy(
        typeName: String,
        role: Option[String],
        dataCenter: Option[DataCenter],
        extractEntityId: ExtractEntityId,
        extractShardId: ExtractShardId): ActorRef = {
      probe.ref ! StartingProxy(typeName, role, dataCenter, extractEntityId, extractShardId)
      ActorRef.noSender
    }
  }

  "ClusterSharding" must {

    "start a region in proxy mode in case of node role mismatch" in {

      val settingsWithRole = ClusterShardingSettings(system).withRole("nonExistingRole")
      val typeName = "typeName"
      val extractEntityId: ExtractEntityId = { case msg: Msg => ("42", msg) }
      val extractShardId: ExtractShardId = _ => "37"

      clusterSharding.start(
        typeName = typeName,
        entityProps = Props.empty,
        settings = settingsWithRole,
        extractEntityId = extractEntityId,
        extractShardId = extractShardId,
        allocationStrategy = ShardAllocationStrategy.leastShardAllocationStrategy(3, 0.1),
        handOffStopMessage = PoisonPill)

      probe.expectMsg(StartingProxy(typeName, settingsWithRole.role, None, extractEntityId, extractShardId))
    }

    "stop entities from HandOffStopper even if the entity doesn't handle handOffStopMessage" in {
      val probe = TestProbe()
      val typeName = "typeName"
      val shard = "7"
      val emptyHandlerActor = system.actorOf(Props(new EmptyHandlerActor))
      val handOffStopper = system.actorOf(
        Props(new HandOffStopper(typeName, shard, probe.ref, Set(emptyHandlerActor), HandOffStopMessage, 10.millis)))

      watch(emptyHandlerActor)
      expectTerminated(emptyHandlerActor, 1.seconds)

      probe.expectMsg(1.seconds, ShardStopped(shard))
      probe.lastSender shouldEqual handOffStopper

      watch(handOffStopper)
      expectTerminated(handOffStopper, 1.seconds)
    }
  }
}
