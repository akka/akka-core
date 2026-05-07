/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import akka.annotation.InternalApi
import akka.util.ConstantFun

/**
 * INTERNAL API
 *
 * Default instance for no snapshots which is the default.
 */
@InternalApi
private[akka] object SnapshotWhenPredicate {
  def noSnapshot[State, Event]: SnapshotWhenPredicate[State, Event] =
    SnapshotWhenPredicate[State, Event](ConstantFun.scalaAnyThreeToFalse, deleteEventsOnSnapshot = false)
}

/**
 * INTERNAL API
 *
 * Wrapper to contain the predicate and deleteEventsOnSnapshot flag
 */
@InternalApi
private[akka] case class SnapshotWhenPredicate[State, Event](
    predicate: (State, Event, Long) => Boolean,
    deleteEventsOnSnapshot: Boolean)
