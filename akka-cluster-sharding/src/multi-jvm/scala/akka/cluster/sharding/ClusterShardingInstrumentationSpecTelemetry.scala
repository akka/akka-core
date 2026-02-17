/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.cluster.sharding.internal.ClusterShardingInstrumentation

class ClusterShardingInstrumentationSpecTelemetry(@nowarn("msg=never used") system: ExtendedActorSystem)
    extends ClusterShardingInstrumentation {

  val shardRegionBufferSizeCounter = new AtomicInteger(0)
  val beginShardHandoffDurationCounter = new AtomicInteger(0)
  val finishShardHandoffDurationCounter = new AtomicInteger(0)
  val shardHomeRequests = new AtomicInteger(0)
  val shardHomeResponses = new AtomicInteger(0)
  val dropMessageCounter = new AtomicInteger(0)

  override def shardRegionBufferSize(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      size: Int): Unit = {
    shardRegionBufferSizeCounter.set(size)
  }

  override def shardRegionBufferSizeIncremented(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String): Unit = {
    shardRegionBufferSizeCounter.incrementAndGet()
  }

  override def shardHandoffStarted(
      selfAddress: Address,
      shardCoordinatorActor: ActorRef,
      typeName: String,
      shard: String): Unit = beginShardHandoffDurationCounter.incrementAndGet()

  override def shardHandoffFinished(
      selfAddress: Address,
      shardCoordinatorActor: ActorRef,
      typeName: String,
      shard: String,
      ok: Boolean): Unit = finishShardHandoffDurationCounter.incrementAndGet()

  override def regionRequestedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit =
    shardHomeRequests.incrementAndGet()

  override def receivedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit = shardHomeResponses.incrementAndGet()

  override def messageDropped(selfAddress: Address, self: ActorRef, typeName: String): Unit =
    dropMessageCounter.incrementAndGet()

  override def dependencies: Seq[String] = Nil
}
