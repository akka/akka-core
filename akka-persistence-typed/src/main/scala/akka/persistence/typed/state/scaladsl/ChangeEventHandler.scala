/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import akka.annotation.ApiMayChange

/**
 * API May Change
 */
@ApiMayChange
object ChangeEventHandler {

  /**
   * Define these handlers in the [[DurableStateBehavior#withChangeEventHandler]] to store additional change event when
   * the state is updated. The event can be used in Projections.
   *
   * The `updateHandler` and `deleteHandler` are invoked after the ordinary command handler. Be aware of that
   * if the state is mutable and modified by the command handler the previous state parameter of the `updateHandler`
   * will also include the modification, since it's the same instance. If that is a problem you need to use
   * immutable state and create a new state instance when modifying it in the command handler.
   *
   * @param updateHandler Function that given the previous and new state creates the change event to be stored
   *                      when the DurableState is updated.
   * @param deleteHandler Function that given the previous state creates the change event to be stored
   *                      when the DurableState is deleted.
   */
  def apply[Command, State, ChangeEvent](
      updateHandler: (State, State, Command) => ChangeEvent,
      deleteHandler: (State, Command) => ChangeEvent): ChangeEventHandler[Command, State, ChangeEvent] =
    new ChangeEventHandler(updateHandler, deleteHandler)
}

/**
 * API May Change: Define these handlers in the [[DurableStateBehavior#withChangeEventHandler]] to store
 * additional change event when the state is updated. The event can be used in Projections.
 */
@ApiMayChange
final class ChangeEventHandler[Command, State, ChangeEvent] private (
    val updateHandler: (State, State, Command) => ChangeEvent,
    val deleteHandler: (State, Command) => ChangeEvent)
