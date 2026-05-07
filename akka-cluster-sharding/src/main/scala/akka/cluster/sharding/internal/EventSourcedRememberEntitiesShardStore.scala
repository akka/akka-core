/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.ActorLogging
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSerializable
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.EntityId
import akka.persistence.DeleteMessagesFailure
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.DeleteSnapshotsFailure
import akka.persistence.DeleteSnapshotsSuccess
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotOffer
import akka.persistence.SnapshotSelectionCriteria

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventSourcedRememberEntitiesShardStore {

  /**
   * A case class which represents a state change for the Shard
   */
  sealed trait StateChange extends ClusterShardingSerializable

  /**
   * Persistent state of the Shard.
   */
  final case class State private[akka] (entities: Set[EntityId] = Set.empty) extends ClusterShardingSerializable

  /**
   * `State` change for starting a set of entities in this `Shard`
   */
  final case class EntitiesStarted(entities: Set[EntityId]) extends StateChange

  case object StartedAck

  /**
   * `State` change for an entity which has terminated.
   */
  final case class EntitiesStopped(entities: Set[EntityId]) extends StateChange

  def props(typeName: String, shardId: ShardRegion.ShardId, settings: ClusterShardingSettings): Props =
    Props(new EventSourcedRememberEntitiesShardStore(typeName, shardId, settings))

  def createEntityEvents(
      entityIds: Set[EntityId],
      eventConstructor: Set[EntityId] => StateChange,
      batchSize: Int): List[StateChange] = {
    if (entityIds.size <= batchSize)
      // optimized when entity count is small
      eventConstructor(entityIds) :: Nil
    else {
      // split up in several writes so we don't hit journal limit
      entityIds.grouped(batchSize).map(eventConstructor).toList
    }
  }
}

/**
 * INTERNAL API
 *
 * Persistent actor keeping the state for Akka Persistence backed remember entities (enabled through `state-store-mode=persistence`).
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
@InternalApi
private[akka] final class EventSourcedRememberEntitiesShardStore(
    typeName: String,
    shardId: ShardRegion.ShardId,
    settings: ClusterShardingSettings)
    extends PersistentActor
    with ActorLogging {

  import EventSourcedRememberEntitiesShardStore._
  import settings.tuningParameters._

  private val maxUpdatesPerWrite = context.system.settings.config
    .getInt("akka.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write")

  log.debug("Starting up EventSourcedRememberEntitiesStore [{}], using journal [{}]", persistenceId, journalPluginId)
  private var state = State()
  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")

  override def persistenceId = s"/sharding/${typeName}Shard/$shardId"
  override def journalPluginId: String = settings.journalPluginId
  override def snapshotPluginId: String = settings.snapshotPluginId

  override def receiveRecover: Receive = {
    case EntitiesStarted(ids)              => state = state.copy(state.entities.union(ids))
    case EntitiesStopped(ids)              => state = state.copy(state.entities.diff(ids))
    case SnapshotOffer(_, snapshot: State) => state = snapshot
    case RecoveryCompleted =>
      log.debug("Recovery completed for shard [{}] with [{}] entities", shardId, state.entities.size)
  }

  override def receiveCommand: Receive = {

    case RememberEntitiesShardStore.Update(started, stopped) =>
      if (verboseDebug)
        log.debug("ES remember entities store asked to store started [{}] and stopped [{}]", started, stopped)

      val events =
        (if (started.nonEmpty) createEntityEvents(started, EntitiesStarted.apply _, maxUpdatesPerWrite) else Nil) :::
        (if (stopped.nonEmpty) createEntityEvents(stopped, EntitiesStopped.apply _, maxUpdatesPerWrite) else Nil)
      var left = events.size
      var saveSnap = false
      persistAll(events) { _ =>
        left -= 1
        saveSnap = saveSnap || isSnapshotNeeded
        if (left == 0) {
          if (verboseDebug)
            log.debug("Persisting remembered entities update done, started [{}], stopped [{}]", started, stopped)
          sender() ! RememberEntitiesShardStore.UpdateDone(started, stopped)
          state = state.copy(state.entities.union(started).diff(stopped))
          if (saveSnap) {
            saveSnapshot()
          }
        }
      }

    case RememberEntitiesShardStore.GetEntities =>
      sender() ! RememberEntitiesShardStore.RememberedEntities(state.entities)

    case e: SaveSnapshotSuccess =>
      log.debug("Snapshot saved successfully")
      internalDeleteMessagesBeforeSnapshot(e, keepNrOfBatches, snapshotAfter)

    case SaveSnapshotFailure(_, reason) =>
      log.warning("Snapshot failure: [{}]", reason.getMessage)

    case DeleteMessagesSuccess(toSequenceNr) =>
      val deleteTo = toSequenceNr - 1
      // keeping one additional batch of messages in case snapshotAfter has been delayed to the end of a processed batch
      val keepNrOfBatchesWithSafetyBatch = if (keepNrOfBatches == 0) 0 else keepNrOfBatches + 1
      val deleteFrom = math.max(0, deleteTo - (keepNrOfBatchesWithSafetyBatch * snapshotAfter))
      log.debug(
        "Messages to [{}] deleted successfully. Deleting snapshots from [{}] to [{}]",
        toSequenceNr,
        deleteFrom,
        deleteTo)
      deleteSnapshots(SnapshotSelectionCriteria(minSequenceNr = deleteFrom, maxSequenceNr = deleteTo))

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.warning("Messages to [{}] deletion failure: [{}]", toSequenceNr, reason.getMessage)

    case DeleteSnapshotsSuccess(m) =>
      log.debug("Snapshots matching [{}] deleted successfully", m)

    case DeleteSnapshotsFailure(m, reason) =>
      log.warning("Snapshots matching [{}] deletion failure: [{}]", m, reason.getMessage)

  }

  override def postStop(): Unit = {
    super.postStop()
    log.debug("Store stopping")
  }

  def saveSnapshotWhenNeeded(): Unit = {
    if (isSnapshotNeeded) {
      saveSnapshot()
    }
  }

  private def saveSnapshot(): Unit = {
    log.debug("Saving snapshot, sequence number [{}]", snapshotSequenceNr)
    saveSnapshot(state)
  }

  private def isSnapshotNeeded = {
    lastSequenceNr % snapshotAfter == 0 && lastSequenceNr != 0
  }
}
