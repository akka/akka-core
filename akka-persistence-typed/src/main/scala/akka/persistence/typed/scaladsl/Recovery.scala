/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import akka.annotation.InternalApi
import akka.persistence.typed.SnapshotSelectionCriteria
import akka.persistence.typed.internal.ReplayOnlyLastRecovery
import akka.persistence.typed.internal.{ DefaultRecovery, DisabledRecovery, RecoveryWithSnapshotSelectionCriteria }

/**
 * Strategy for recovery of snapshots and events.
 */
trait Recovery {
  def asJava: akka.persistence.typed.javadsl.Recovery

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def toClassic: akka.persistence.Recovery

}

/**
 * Strategy for recovery of snapshots and events.
 */
object Recovery {

  /**
   * Snapshots and events are recovered
   */
  val default: Recovery = DefaultRecovery

  /**
   * Neither snapshots nor events are recovered
   */
  val disabled: Recovery = DisabledRecovery

  /**
   * Changes the snapshot selection criteria used for the recovery.
   *
   * By default the most recent snapshot is used, and the remaining state updates are recovered by replaying events
   * from the sequence number up until which the snapshot reached.
   *
   * You may configure the behavior to skip replaying snapshots completely, in which case the recovery will be
   * performed by replaying all events -- which may take a long time.
   */
  def withSnapshotSelectionCriteria(snapshotSelectionCriteria: SnapshotSelectionCriteria) =
    RecoveryWithSnapshotSelectionCriteria(snapshotSelectionCriteria)

  /**
   * Don't load snapshot and replay only last event.
   */
  val replayOnlyLast: Recovery = ReplayOnlyLastRecovery

}
