/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import java.time.Duration
import java.util.Optional

import scala.jdk.DurationConverters._
import scala.jdk.OptionConverters._

import akka.japi.function.Function3
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
  def snapshotAdapter[State](adapt: Function3[String, Any, Optional[Duration], State]): SnapshotAdapter[State] =
    akka.persistence.typed.scaladsl.PersistentFSMMigration.snapshotAdapter((stateId, snapshot, timer) =>
      adapt.apply(stateId, snapshot, timer.map(_.toJava).toJava))
}
