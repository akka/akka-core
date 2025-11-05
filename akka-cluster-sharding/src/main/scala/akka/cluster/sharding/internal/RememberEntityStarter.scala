/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.concurrent.ExecutionContext

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.Timers
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.Shard
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.EntityId
import akka.cluster.sharding.ShardRegion.ShardId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object RememberEntityStarterManager {
  def props(region: ActorRef, settings: ClusterShardingSettings) =
    Props(new RememberEntityStarterManager(region, settings))

  final case class StartEntities(shard: ActorRef, shardId: ShardRegion.ShardId, ids: Set[ShardRegion.EntityId])
      extends NoSerializationVerificationNeeded

  private case object ContinueAfterDelay extends NoSerializationVerificationNeeded
}

/**
 * INTERNAL API: Actor responsible for starting entities when rememberEntities is enabled
 */
@InternalApi
private[akka] final class RememberEntityStarterManager(region: ActorRef, settings: ClusterShardingSettings)
    extends Actor
    with ActorLogging
    with Timers {
  import RememberEntityStarterManager.ContinueAfterDelay
  import RememberEntityStarterManager.StartEntities

  private val delay = settings.tuningParameters.entityRecoveryConstantRateStrategyFrequency

  override def receive: Receive =
    settings.tuningParameters.entityRecoveryStrategy match {
      case "all"      => allStrategy
      case "constant" => constantStrategyIdle
      case other      => throw new IllegalArgumentException(s"Unknown entityRecoveryStrategy [$other]")
    }

  private val allStrategy: Receive = {
    case s: StartEntities => start(s, isConstantStrategy = false)
    case _: Terminated    => // RememberEntityStarter was done
  }

  private val constantStrategyIdle: Receive = {
    case s: StartEntities =>
      start(s, isConstantStrategy = true)
      context.become(constantStrategyWaiting(Vector.empty))
  }

  private def constantStrategyWaiting(workQueue: Vector[StartEntities]): Receive = {
    case s: StartEntities =>
      context.become(constantStrategyWaiting(workQueue :+ s))

    case _: Terminated =>
      // RememberEntityStarter was done
      timers.startSingleTimer(ContinueAfterDelay, ContinueAfterDelay, delay)

    case ContinueAfterDelay =>
      if (workQueue.isEmpty)
        context.become(constantStrategyIdle)
      else {
        start(workQueue.head, isConstantStrategy = true)
        context.become(constantStrategyWaiting(workQueue.tail))
      }
  }

  private def start(s: StartEntities, isConstantStrategy: Boolean): Unit = {
    context.watch(
      context.actorOf(RememberEntityStarter.props(region, s.shard, s.shardId, s.ids, isConstantStrategy, settings)))
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object RememberEntityStarter {
  def props(
      region: ActorRef,
      shard: ActorRef,
      shardId: ShardRegion.ShardId,
      ids: Set[ShardRegion.EntityId],
      isConstantStrategy: Boolean,
      settings: ClusterShardingSettings) =
    Props(new RememberEntityStarter(region, shard, shardId, ids, isConstantStrategy, settings))

  private final case class StartBatch(batchSize: Int) extends NoSerializationVerificationNeeded
  private case object ResendUnAcked extends NoSerializationVerificationNeeded
}

/**
 * INTERNAL API: Actor responsible for starting entities when rememberEntities is enabled
 */
@InternalApi
private[akka] final class RememberEntityStarter(
    region: ActorRef,
    shard: ActorRef,
    shardId: ShardRegion.ShardId,
    ids: Set[ShardRegion.EntityId],
    constantStrategy: Boolean,
    settings: ClusterShardingSettings)
    extends Actor
    with ActorLogging
    with Timers {

  implicit val ec: ExecutionContext = context.dispatcher
  import RememberEntityStarter._

  require(ids.nonEmpty)

  private var idsLeftToStart = Set.empty[EntityId]
  private var waitingForAck = Set.empty[EntityId]
  private var entitiesMoved = Set.empty[EntityId]

  log.debug(
    "Shard [{}] starting [{}] remembered entities using strategy [{}]",
    shardId,
    ids.size,
    settings.tuningParameters.entityRecoveryStrategy)

  if (constantStrategy) {
    import settings.tuningParameters
    idsLeftToStart = ids
    timers.startTimerWithFixedDelay(
      "constant",
      StartBatch(tuningParameters.entityRecoveryConstantRateStrategyNumberOfEntities),
      tuningParameters.entityRecoveryConstantRateStrategyFrequency)
    startBatch(tuningParameters.entityRecoveryConstantRateStrategyNumberOfEntities)
  } else {
    idsLeftToStart = Set.empty
    startBatch(ids)
  }
  timers.startTimerWithFixedDelay("retry", ResendUnAcked, settings.tuningParameters.retryInterval)

  override def receive: Receive = {
    case StartBatch(batchSize)                                => startBatch(batchSize)
    case ShardRegion.StartEntityAck(entityId, ackFromShardId) => onAck(entityId, ackFromShardId)
    case ResendUnAcked                                        => retryUnacked()
  }

  private def onAck(entityId: EntityId, ackFromShardId: ShardId): Unit = {
    idsLeftToStart -= entityId
    waitingForAck -= entityId
    if (shardId != ackFromShardId) entitiesMoved += entityId
    if (waitingForAck.isEmpty && idsLeftToStart.isEmpty) {
      if (entitiesMoved.nonEmpty) {
        log.info("Found [{}] entities moved to new shard(s)", entitiesMoved.size)
        shard ! Shard.EntitiesMovedToOtherShard(entitiesMoved)
      }
      context.stop(self)
    }
  }

  private def startBatch(batchSize: Int): Unit = {
    log.debug("Starting batch of [{}] remembered entities", batchSize)
    val (batch, newIdsLeftToStart) = idsLeftToStart.splitAt(batchSize)
    idsLeftToStart = newIdsLeftToStart
    startBatch(batch)
  }

  private def startBatch(entityIds: Set[EntityId]): Unit = {
    // these go through the region rather the directly to the shard
    // so that shard id extractor changes make them start on the right shard
    waitingForAck = waitingForAck.union(entityIds)
    entityIds.foreach(entityId => region ! ShardRegion.StartEntity(entityId))
  }

  private def retryUnacked(): Unit = {
    if (waitingForAck.nonEmpty) {
      log.debug("Found [{}] remembered entities waiting for StartEntityAck, retrying", waitingForAck.size)
      waitingForAck.foreach { id =>
        // for now we just retry all (as that was the existing behavior spread out over starter and shard)
        // but in the future it could perhaps make sense to batch also the retries to avoid thundering herd
        region ! ShardRegion.StartEntity(id)
      }
    }
  }

}
