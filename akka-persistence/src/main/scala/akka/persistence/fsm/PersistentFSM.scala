/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.fsm

import akka.annotation.InternalApi
import akka.persistence.serialization.Message

import scala.concurrent.duration._

/**
 * Note: Deprecated and removed except for the event and snapshot types which are needed for migrating existing
 * PersistentFSM actors to EventSourcedBehavior using the ```akka.persistence.typed.scaladsl.PersistentFSMMigration```
 * and ```akka.persistence.typed.javadsl.PersistentFSMMigration```
 */
object PersistentFSM {

  sealed trait PersistentFsmEvent extends Message

  /**
   * Persisted on state change
   * Not deprecated as used for users migrating from PersistentFSM to EventSourcedBehavior
   *
   * @param stateIdentifier FSM state identifier
   * @param timeout FSM state timeout
   */
  case class StateChangeEvent(stateIdentifier: String, timeout: Option[FiniteDuration]) extends PersistentFsmEvent

  /**
   * FSM state and data snapshot
   *
   * @param stateIdentifier FSM state identifier
   * @param data FSM state data
   * @param timeout FSM state timeout
   * @tparam D state data type
   */
  @InternalApi
  private[persistence] case class PersistentFSMSnapshot[D](
      stateIdentifier: String,
      data: D,
      timeout: Option[FiniteDuration])
      extends Message

}
