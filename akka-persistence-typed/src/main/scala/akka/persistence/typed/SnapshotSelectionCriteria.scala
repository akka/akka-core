/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import akka.annotation.InternalApi
import akka.persistence.{ SnapshotSelectionCriteria => ClassicSnapshotSelectionCriteria }
import akka.util.HashCode

object SnapshotSelectionCriteria {

  /**
   * The latest saved snapshot.
   */
  val latest: SnapshotSelectionCriteria =
    new SnapshotSelectionCriteria(
      maxSequenceNr = Long.MaxValue,
      maxTimestamp = Long.MaxValue,
      minSequenceNr = 0L,
      minTimestamp = 0L)

  /**
   * No saved snapshot matches.
   */
  val none: SnapshotSelectionCriteria =
    new SnapshotSelectionCriteria(maxSequenceNr = 0L, maxTimestamp = 0L, minSequenceNr = 0L, minTimestamp = 0L)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def fromClassic(c: ClassicSnapshotSelectionCriteria): SnapshotSelectionCriteria =
    new SnapshotSelectionCriteria(c.maxSequenceNr, c.maxTimestamp, c.minSequenceNr, c.minTimestamp)

}

/**
 * Selection criteria for loading and deleting snapshots.
 */
final class SnapshotSelectionCriteria @InternalApi private[akka] (
    val maxSequenceNr: Long,
    val maxTimestamp: Long,
    val minSequenceNr: Long,
    val minTimestamp: Long) {

  /**
   * upper bound for a selected snapshot's sequence number
   */
  def withMaxSequenceNr(newMaxSequenceNr: Long): SnapshotSelectionCriteria =
    copy(maxSequenceNr = newMaxSequenceNr)

  /**
   * upper bound for a selected snapshot's timestamp, in milliseconds from the epoch
   * of 1970-01-01T00:00:00Z.
   */
  def withMaxTimestamp(newMaxTimestamp: Long): SnapshotSelectionCriteria =
    copy(maxTimestamp = newMaxTimestamp)

  /**
   * lower bound for a selected snapshot's sequence number
   */
  def withMinSequenceNr(newMinSequenceNr: Long): SnapshotSelectionCriteria =
    copy(minSequenceNr = newMinSequenceNr)

  /**
   * lower bound for a selected snapshot's timestamp, in milliseconds from the epoch
   * of 1970-01-01T00:00:00Z.
   */
  def withMinTimestamp(newMinTimestamp: Long): SnapshotSelectionCriteria =
    copy(minTimestamp = newMinTimestamp)

  private def copy(
      maxSequenceNr: Long = maxSequenceNr,
      maxTimestamp: Long = maxTimestamp,
      minSequenceNr: Long = minSequenceNr,
      minTimestamp: Long = minTimestamp): SnapshotSelectionCriteria =
    new SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp)

  override def toString: String =
    s"SnapshotSelectionCriteria($maxSequenceNr,$maxTimestamp,$minSequenceNr,$minTimestamp)"

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def toClassic: akka.persistence.SnapshotSelectionCriteria =
    akka.persistence.SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp)

  override def equals(other: Any): Boolean = other match {
    case that: SnapshotSelectionCriteria =>
      maxSequenceNr == that.maxSequenceNr &&
      maxTimestamp == that.maxTimestamp &&
      minSequenceNr == that.minSequenceNr &&
      minTimestamp == that.minTimestamp
    case _ => false
  }

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, maxSequenceNr)
    result = HashCode.hash(result, maxTimestamp)
    result = HashCode.hash(result, minSequenceNr)
    result = HashCode.hash(result, minTimestamp)
    result
  }
}
