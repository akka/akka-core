/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.persistence.fsm.PersistentFSM.PersistentFSMSnapshot
import akka.persistence.typed.SnapshotAdapter

/**
 * Helper functions for migration from PersistentFSM to Persistence Typed
 */
object PersistentFSMMigration {

  /**
   * Create a snapshot adapter that will adapt snapshots created by a PersistentFSM into
   * the correct State type of a [[EventSourcedBehavior]]
   * @param adapt Takes in the state identifier, snapshot persisted by the PersistentFSM and the state timeout and
   *              returns the `State` that should be given to the the [[EventSourcedBehavior]]
   * @tparam State State type of the [[EventSourcedBehavior]]
   * @return A [[SnapshotAdapter]] to be used with a [[EventSourcedBehavior]]
   */
  def snapshotAdapter[State](adapt: (String, Any, Option[FiniteDuration]) => State): SnapshotAdapter[State] =
    new SnapshotAdapter[State] {
      override def toJournal(state: State): Any = state
      override def fromJournal(from: Any): State = {
        from match {
          case PersistentFSMSnapshot(stateIdentifier, data, timeout) => adapt(stateIdentifier, data, timeout)
          case data                                                  => data.asInstanceOf[State]
        }
      }
    }
}
