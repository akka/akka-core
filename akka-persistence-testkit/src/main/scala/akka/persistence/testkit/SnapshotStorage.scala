/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import scala.util.Success

import akka.actor.Extension
import akka.annotation.InternalApi
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.testkit.ProcessingPolicy.DefaultPolicies
import akka.persistence.testkit.internal.TestKitStorage

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] trait SnapshotStorage
    extends TestKitStorage[SnapshotOperation, (SnapshotMetadata, Any)]
    with Extension {

  import SnapshotStorage._

  override def reprToSeqNum(repr: (SnapshotMetadata, Any)): Long =
    repr._1.sequenceNr

  override protected val DefaultPolicy = SnapshotPolicies.PassAll

  def tryAdd(meta: SnapshotMetadata, payload: Any): Unit = {
    currentPolicy.tryProcess(meta.persistenceId, WriteSnapshot(SnapshotMeta(meta.sequenceNr, meta.timestamp), payload)) match {
      case ProcessingSuccess =>
        add(meta.persistenceId, (meta, payload))
        Success(())
      case f: ProcessingFailure => throw f.error

    }
  }

  def tryRead(persistenceId: String, criteria: SnapshotSelectionCriteria): Option[SelectedSnapshot] = {
    val selectedSnapshot =
      read(persistenceId).flatMap(
        _.reverseIterator.find(v => criteria.matches(v._1)).map(v => SelectedSnapshot(v._1, v._2)))
    currentPolicy.tryProcess(persistenceId, ReadSnapshot(criteria, selectedSnapshot.map(_.snapshot))) match {
      case ProcessingSuccess    => selectedSnapshot
      case f: ProcessingFailure => throw f.error
    }
  }

  def tryDelete(persistenceId: String, selectionCriteria: SnapshotSelectionCriteria): Unit = {
    currentPolicy.tryProcess(persistenceId, DeleteSnapshotsByCriteria(selectionCriteria)) match {
      case ProcessingSuccess =>
        delete(persistenceId, v => selectionCriteria.matches(v._1))
      case f: ProcessingFailure => throw f.error
    }
  }

  def tryDelete(meta: SnapshotMetadata): Unit = {
    currentPolicy.tryProcess(meta.persistenceId, DeleteSnapshotByMeta(SnapshotMeta(meta.sequenceNr, meta.timestamp))) match {
      case ProcessingSuccess =>
        delete(meta.persistenceId, _._1.sequenceNr == meta.sequenceNr)
      case f: ProcessingFailure => throw f.error
    }
  }

}

object SnapshotStorage {

  object SnapshotPolicies extends DefaultPolicies[SnapshotOperation]

}

/**
 * Snapshot metainformation.
 */
final case class SnapshotMeta(sequenceNr: Long, timestamp: Long = 0L) {

  def getSequenceNr() = sequenceNr

  def getTimestamp() = timestamp

}

case object SnapshotMeta {

  def create(sequenceNr: Long, timestamp: Long) =
    SnapshotMeta(sequenceNr, timestamp)

  def create(sequenceNr: Long) = SnapshotMeta(sequenceNr)

}

/**
 * INTERNAL API
 * Operations supported by snapshot plugin
 */
@InternalApi
sealed trait SnapshotOperation

/**
 *
 * Storage read operation for recovery of the persistent actor.
 *
 * @param criteria criteria with which snapshot is searched
 * @param snapshot snapshot found by criteria
 */
final case class ReadSnapshot(criteria: SnapshotSelectionCriteria, snapshot: Option[Any]) extends SnapshotOperation {

  def getSnapshotSelectionCriteria() = criteria

  def getSnapshot(): java.util.Optional[Any] =
    snapshot.map(java.util.Optional.of[Any]).getOrElse(java.util.Optional.empty[Any]())

}

/**
 * Storage write operation to persist snapshot in the storage.
 *
 * @param metadata snapshot metadata
 * @param snapshot snapshot payload
 */
final case class WriteSnapshot(metadata: SnapshotMeta, snapshot: Any) extends SnapshotOperation {

  def getMetadata() = metadata

  def getSnapshot() = snapshot

}

/**
 * INTERNAL API
 */
@InternalApi
sealed abstract class DeleteSnapshot extends SnapshotOperation

/**
 * Delete snapshots from storage by criteria.
 */
final case class DeleteSnapshotsByCriteria(criteria: SnapshotSelectionCriteria) extends DeleteSnapshot {

  def getCriteria() = criteria

}

/**
 * Delete particular snapshot from storage by its metadata.
 */
final case class DeleteSnapshotByMeta(metadata: SnapshotMeta) extends DeleteSnapshot {

  def getMetadata() = metadata

}
