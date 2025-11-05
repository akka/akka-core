/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.annotation.InternalApi
import akka.persistence.typed.javadsl
import akka.persistence.typed.scaladsl

/**
 * INTERNAL API
 *
 * Note that `keepNSnapshots` should not be used when `BehaviorSetup.isOnlyOneSnapshot` is true.
 */
@InternalApi private[akka] final case class SnapshotCountRetentionCriteriaImpl(
    snapshotEveryNEvents: Int,
    keepNSnapshots: Int,
    deleteEventsOnSnapshot: Boolean)
    extends javadsl.SnapshotCountRetentionCriteria
    with scaladsl.SnapshotCountRetentionCriteria {

  require(snapshotEveryNEvents > 0, s"'snapshotEveryNEvents' must be greater than 0, was [$snapshotEveryNEvents]")
  require(keepNSnapshots > 0, s"'keepNSnapshots' must be greater than 0, was [$keepNSnapshots]")

  def snapshotWhen(currentSequenceNr: Long): Boolean =
    currentSequenceNr % snapshotEveryNEvents == 0

  /**
   * Should only be used when `BehaviorSetup.isOnlyOneSnapshot` is true.
   */
  def deleteUpperSequenceNr(lastSequenceNr: Long): Long = {
    // Delete old events, retain the latest
    math.max(0, lastSequenceNr - (keepNSnapshots.toLong * snapshotEveryNEvents))
  }

  override def withDeleteEventsOnSnapshot: SnapshotCountRetentionCriteriaImpl =
    copy(deleteEventsOnSnapshot = true)

  override def asScala: scaladsl.RetentionCriteria = this

  override def asJava: javadsl.RetentionCriteria = this
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object DisabledRetentionCriteria
    extends javadsl.RetentionCriteria
    with scaladsl.RetentionCriteria {
  override def asScala: scaladsl.RetentionCriteria = this
  override def asJava: javadsl.RetentionCriteria = this
}
