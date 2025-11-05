/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import akka.actor.typed.Signal
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

/**
 * Supertype for all Akka Persistence Typed specific signals
 *
 * Not for user extension
 */
@DoNotInherit
sealed trait EventSourcedSignal extends Signal

@DoNotInherit sealed abstract class RecoveryCompleted extends EventSourcedSignal
case object RecoveryCompleted extends RecoveryCompleted {
  def instance: RecoveryCompleted = this
}

final case class RecoveryFailed(failure: Throwable) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getFailure(): Throwable = failure
}

final case class SnapshotCompleted(metadata: SnapshotMetadata) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getSnapshotMetadata(): SnapshotMetadata = metadata
}

final case class SnapshotFailed(metadata: SnapshotMetadata, failure: Throwable) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getFailure(): Throwable = failure

  /**
   * Java API
   */
  def getSnapshotMetadata(): SnapshotMetadata = metadata
}

object SnapshotMetadata {

  /**
   * @param persistenceId id of persistent actor from which the snapshot was taken.
   * @param sequenceNr sequence number at which the snapshot was taken.
   * @param timestamp time at which the snapshot was saved, defaults to 0 when unknown.
   *                  in milliseconds from the epoch of 1970-01-01T00:00:00Z.
   */
  def apply(persistenceId: String, sequenceNr: Long, timestamp: Long): SnapshotMetadata =
    new SnapshotMetadata(persistenceId, sequenceNr, timestamp)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def fromClassic(metadata: akka.persistence.SnapshotMetadata): SnapshotMetadata =
    new SnapshotMetadata(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp)
}

/**
 * Snapshot metadata.
 *
 * @param persistenceId id of persistent actor from which the snapshot was taken.
 * @param sequenceNr sequence number at which the snapshot was taken.
 * @param timestamp time at which the snapshot was saved, defaults to 0 when unknown.
 *                  in milliseconds from the epoch of 1970-01-01T00:00:00Z.
 */
final class SnapshotMetadata(val persistenceId: String, val sequenceNr: Long, val timestamp: Long) {
  override def toString: String =
    s"SnapshotMetadata($persistenceId,$sequenceNr,$timestamp)"
}

final case class DeleteSnapshotsCompleted(target: DeletionTarget) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getTarget(): DeletionTarget = target
}

final case class DeleteSnapshotsFailed(target: DeletionTarget, failure: Throwable) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getFailure(): Throwable = failure

  /**
   * Java API
   */
  def getTarget(): DeletionTarget = target
}

final case class DeleteEventsCompleted(toSequenceNr: Long) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getToSequenceNr(): Long = toSequenceNr
}

final case class DeleteEventsFailed(toSequenceNr: Long, failure: Throwable) extends EventSourcedSignal {

  /**
   * Java API
   */
  def getFailure(): Throwable = failure

  /**
   * Java API
   */
  def getToSequenceNr(): Long = toSequenceNr
}

/**
 * Not for user extension
 */
@DoNotInherit
sealed trait DeletionTarget
object DeletionTarget {
  final case class Individual(metadata: SnapshotMetadata) extends DeletionTarget {

    /**
     * Java API
     */
    def getSnapshotMetadata(): SnapshotMetadata = metadata
  }
  final case class Criteria(selection: SnapshotSelectionCriteria) extends DeletionTarget {

    /**
     * Java API
     */
    def getSnapshotSelection(): SnapshotSelectionCriteria = selection
  }
}
