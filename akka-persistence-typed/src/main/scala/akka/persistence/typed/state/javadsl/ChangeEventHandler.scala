/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.javadsl

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

/**
 * API May Change: Implement this interface and use it in [[DurableStateBehavior#withChangeEventHandler]]
 * to store additional change event when the state is updated. The event can be used in Projections.
 *
 * The `updateHandler` and `deleteHandler` are invoked after the ordinary command handler. Be aware of that
 * if the state is mutable and modified by the command handler the previous state parameter of the `updateHandler`
 * will also include the modification, since it's the same instance. If that is a problem you need to use
 * immutable state and create a new state instance when modifying it in the command handler.
 */
@ApiMayChange
trait ChangeEventHandler[Command, State, ChangeEvent] {

  /**
   * Store additional change event when the state is updated. The event can be used in Projections.
   *
   * @param previousState Previous state before the update.
   * @param newState      New state after the update.
   * @return The change event to be stored.
   */
  def updateHandler(previousState: State, newState: State, command: Command): ChangeEvent

  /**
   * Store additional change event when the state is updated. The event can be used in Projections.
   *
   * @param previousState Previous state before the delete.
   * @return The change event to be stored.
   */
  def deleteHandler(previousState: State, command: Command): ChangeEvent

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ChangeEventHandler {
  val Undefined: ChangeEventHandler[Any, Any, Any] = new ChangeEventHandler[Any, Any, Any] {
    override def updateHandler(previousState: Any, newState: Any, command: Any): Any = null
    override def deleteHandler(previousState: Any, command: Any): Any = null
  }

  def undefined[Command, State, ChangeEvent]: ChangeEventHandler[Command, State, ChangeEvent] =
    Undefined.asInstanceOf[ChangeEventHandler[Command, State, ChangeEvent]]

}
