/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.collection.mutable

import akka.actor.ActorLogging
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSerializable
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardCoordinator.Internal
import akka.cluster.sharding.ShardRegion.ShardId
import akka.persistence._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventSourcedRememberEntitiesCoordinatorStore {
  def props(typeName: String, settings: ClusterShardingSettings): Props =
    Props(new EventSourcedRememberEntitiesCoordinatorStore(typeName, settings))

  case class State(shards: Set[ShardId], writtenMigrationMarker: Boolean = false) extends ClusterShardingSerializable

  case object MigrationMarker extends ClusterShardingSerializable
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class EventSourcedRememberEntitiesCoordinatorStore(
    typeName: String,
    settings: ClusterShardingSettings)
    extends PersistentActor
    with ActorLogging {

  import EventSourcedRememberEntitiesCoordinatorStore._

  // Uses the same persistence id as the old persistent coordinator so that the old data can be migrated
  // without any user action
  override def persistenceId = s"/sharding/${typeName}Coordinator"
  override def journalPluginId: String = settings.journalPluginId
  override def snapshotPluginId: String = settings.snapshotPluginId

  private val shards = mutable.Set.empty[ShardId]
  private var writtenMarker = false

  override def receiveRecover: Receive = {
    case shardId: ShardId =>
      shards.add(shardId)
    case SnapshotOffer(_, state: Internal.State) =>
      shards ++= (state.shards.keys ++ state.unallocatedShards)
    case SnapshotOffer(_, State(shardIds, marker)) =>
      shards ++= shardIds
      writtenMarker = marker
    case RecoveryCompleted =>
      log.debug("Recovery complete. Current shards {}. Written Marker {}", shards, writtenMarker)
      if (!writtenMarker) {
        persist(MigrationMarker) { _ =>
          log.debug("Written migration marker")
          writtenMarker = true
        }
      }
    case MigrationMarker =>
      writtenMarker = true
    case other =>
      log.error(
        "Unexpected message type [{}]. Are you migrating from persistent coordinator state store? If so you must add the migration event adapter. Shards will not be restarted.",
        other.getClass)
  }

  override def receiveCommand: Receive = {
    case RememberEntitiesCoordinatorStore.GetShards =>
      sender() ! RememberEntitiesCoordinatorStore.RememberedShards(shards.toSet)

    case RememberEntitiesCoordinatorStore.AddShard(shardId: ShardId) =>
      persistAsync(shardId) { shardId =>
        shards.add(shardId)
        sender() ! RememberEntitiesCoordinatorStore.UpdateDone(shardId)
        saveSnapshotWhenNeeded()
      }

    case e: SaveSnapshotSuccess =>
      log.debug("Snapshot saved successfully")
      internalDeleteMessagesBeforeSnapshot(
        e,
        settings.tuningParameters.keepNrOfBatches,
        settings.tuningParameters.snapshotAfter)

    case SaveSnapshotFailure(_, reason) =>
      log.warning("Snapshot failure: [{}]", reason.getMessage)

    case DeleteMessagesSuccess(toSequenceNr) =>
      val deleteTo = toSequenceNr - 1
      val deleteFrom =
        math.max(0, deleteTo - (settings.tuningParameters.keepNrOfBatches * settings.tuningParameters.snapshotAfter))
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

  def saveSnapshotWhenNeeded(): Unit = {
    if (lastSequenceNr % settings.tuningParameters.snapshotAfter == 0 && lastSequenceNr != 0) {
      log.debug("Saving snapshot, sequence number [{}]", snapshotSequenceNr)
      saveSnapshot(State(shards.toSet, writtenMarker))
    }
  }
}
