/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.ddata.GSet
import akka.cluster.ddata.GSetKey
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.Replicator.ReadAll
import akka.cluster.ddata.Replicator.ReadMajorityPlus
import akka.cluster.ddata.Replicator.WriteAll
import akka.cluster.ddata.Replicator.WriteMajorityPlus
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.ShardId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object DDataRememberEntitiesCoordinatorStore {
  def props(typeName: String, settings: ClusterShardingSettings, replicator: ActorRef, majorityMinCap: Int): Props =
    Props(new DDataRememberEntitiesCoordinatorStore(typeName, settings, replicator, majorityMinCap))
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class DDataRememberEntitiesCoordinatorStore(
    typeName: String,
    settings: ClusterShardingSettings,
    replicator: ActorRef,
    majorityMinCap: Int)
    extends Actor
    with ActorLogging {

  implicit val node: Cluster = Cluster(context.system)
  implicit val selfUniqueAddress: SelfUniqueAddress = SelfUniqueAddress(node.selfUniqueAddress)

  private val readConsistency = settings.tuningParameters.coordinatorStateReadMajorityPlus match {
    case Int.MaxValue => ReadAll(settings.tuningParameters.waitingForStateTimeout)
    case additional   => ReadMajorityPlus(settings.tuningParameters.waitingForStateTimeout, additional, majorityMinCap)
  }
  private val writeConsistency = settings.tuningParameters.coordinatorStateWriteMajorityPlus match {
    case Int.MaxValue => WriteAll(settings.tuningParameters.updatingStateTimeout)
    case additional   => WriteMajorityPlus(settings.tuningParameters.updatingStateTimeout, additional, majorityMinCap)
  }

  private val AllShardsKey = GSetKey[String](s"shard-${typeName}-all")
  private var retryGetCounter = 0
  private var allShards: Option[Set[ShardId]] = None
  private var coordinatorWaitingForShards: Option[ActorRef] = None

  // eager load of remembered shard ids
  def getAllShards(): Unit = {
    replicator ! Replicator.Get(AllShardsKey, readConsistency)
  }
  getAllShards()

  override def receive: Receive = {
    case RememberEntitiesCoordinatorStore.GetShards =>
      allShards match {
        case Some(shardIds) =>
          coordinatorWaitingForShards = Some(sender())
          onGotAllShards(shardIds);
        case None =>
          // reply when we get them, since there is only ever one coordinator communicating with us
          // and it may retry we can just keep the latest sender
          coordinatorWaitingForShards = Some(sender())
      }

    case g @ Replicator.GetSuccess(AllShardsKey, _) =>
      onGotAllShards(g.get(AllShardsKey).elements)

    case Replicator.NotFound(AllShardsKey, _) =>
      onGotAllShards(Set.empty)

    case Replicator.GetFailure(AllShardsKey, _) =>
      retryGetCounter += 1
      val template =
        "Remember entities coordinator store unable to get initial shards within 'waiting-for-state-timeout': {} millis (retrying)"
      if (retryGetCounter < 5)
        log.warning(template, readConsistency.timeout.toMillis)
      else
        log.error(template, readConsistency.timeout.toMillis)
      // repeat until GetSuccess
      getAllShards()

    case RememberEntitiesCoordinatorStore.AddShard(shardId) =>
      replicator ! Replicator.Update(AllShardsKey, GSet.empty[String], writeConsistency, Some((sender(), shardId)))(
        _ + shardId)

    case Replicator.UpdateSuccess(AllShardsKey, Some((replyTo: ActorRef, shardId: ShardId))) =>
      log.debug("Remember entities coordinator store shards successfully updated with {}", shardId)
      replyTo ! RememberEntitiesCoordinatorStore.UpdateDone(shardId)

    case Replicator.UpdateTimeout(AllShardsKey, Some((replyTo: ActorRef, shardId: ShardId))) =>
      log.error(
        "Remember entities coordinator store unable to update shards state within 'updating-state-timeout': {} millis (retrying), adding shard={}",
        writeConsistency.timeout.toMillis,
        shardId)
      replyTo ! RememberEntitiesCoordinatorStore.UpdateFailed(shardId)

    case Replicator.ModifyFailure(key, error, cause, Some((replyTo: ActorRef, shardId: ShardId))) =>
      log.error(
        cause,
        "Remember entities coordinator store was unable to add shard [{}] (key [{}], failed with error: {})",
        shardId,
        key,
        error)
      replyTo ! RememberEntitiesCoordinatorStore.UpdateFailed(shardId)
  }

  def onGotAllShards(shardIds: Set[ShardId]): Unit = {
    retryGetCounter = 0
    coordinatorWaitingForShards match {
      case Some(coordinator) =>
        coordinator ! RememberEntitiesCoordinatorStore.RememberedShards(shardIds)
        coordinatorWaitingForShards = None
        // clear the shards out now that we have sent them to coordinator, to save some memory
        allShards = None
      case None =>
        // wait for coordinator to ask
        allShards = Some(shardIds)
    }
  }

}
