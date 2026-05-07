/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor._
import akka.persistence.SnapshotProtocol._

/**
 * Snapshot API on top of the internal snapshot protocol.
 */
trait Snapshotter extends Actor {

  /** Snapshot store plugin actor. */
  private[persistence] def snapshotStore: ActorRef

  /**
   * Snapshotter id.
   */
  def snapshotterId: String

  /**
   * Sequence number to use when taking a snapshot.
   */
  def snapshotSequenceNr: Long

  /**
   * Instructs the snapshot store to load the specified snapshot and send it via an [[SnapshotOffer]]
   * to the running [[PersistentActor]].
   */
  def loadSnapshot(persistenceId: String, criteria: SnapshotSelectionCriteria, toSequenceNr: Long): Unit =
    snapshotStore ! LoadSnapshot(persistenceId, criteria, toSequenceNr)

  /**
   * Saves a `snapshot` of this snapshotter's state.
   *
   * The [[PersistentActor]] will be notified about the success or failure of this
   * via an [[SaveSnapshotSuccess]] or [[SaveSnapshotFailure]] message.
   */
  def saveSnapshot(snapshot: Any): Unit = {
    snapshotStore ! SaveSnapshot(SnapshotMetadata(snapshotterId, snapshotSequenceNr), snapshot)
  }

  /**
   * Deletes the snapshot identified by `sequenceNr`.
   *
   * The [[PersistentActor]] will be notified about the status of the deletion
   * via an [[DeleteSnapshotSuccess]] or [[DeleteSnapshotFailure]] message.
   */
  def deleteSnapshot(sequenceNr: Long): Unit = {
    snapshotStore ! DeleteSnapshot(SnapshotMetadata(snapshotterId, sequenceNr))
  }

  /**
   * Deletes all snapshots matching `criteria`.
   *
   * The [[PersistentActor]] will be notified about the status of the deletion
   * via an [[DeleteSnapshotsSuccess]] or [[DeleteSnapshotsFailure]] message.
   */
  def deleteSnapshots(criteria: SnapshotSelectionCriteria): Unit = {
    snapshotStore ! DeleteSnapshots(snapshotterId, criteria)
  }

}
