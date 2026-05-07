/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.javadsl

import akka.actor.typed
import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.state.internal
import akka.persistence.typed.state.internal._
import akka.persistence.typed.state.scaladsl

import java.util.Optional

/**
 * A `Behavior` for a persistent actor with durable storage of its state for projects built with Java 17 or newer where message handling can be done
 * * using switch pattern match.
 *
 * For building event sourced actors with Java versions before 17, see [[DurableStateBehavior]]
 *
 * API May Change
 */
@ApiMayChange
abstract class DurableStateOnCommandBehavior[Command, State] private[akka] (
    val persistenceId: PersistenceId,
    onPersistFailure: Optional[BackoffSupervisorStrategy])
    extends DeferredBehavior[Command] {

  /**
   * @param persistenceId stable unique identifier for the `DurableStateBehavior`
   */
  def this(persistenceId: PersistenceId) = {
    this(persistenceId, Optional.empty[BackoffSupervisorStrategy])
  }

  /**
   * If using onPersistFailure the supervision is only around the `DurableStateBehavior` not any outer setup/withTimers
   * block. If using restart any actions e.g. scheduling timers, can be done on the PreRestart signal or on the
   * RecoveryCompleted signal.
   *
   * @param persistenceId stable unique identifier for the `DurableStateBehavior`
   * @param onPersistFailure BackoffSupervisionStrategy for persist failures
   */
  def this(persistenceId: PersistenceId, onPersistFailure: BackoffSupervisorStrategy) = {
    this(persistenceId, Optional.ofNullable(onPersistFailure))
  }

  /**
   * Factory of effects.
   *
   * Return effects from your handlers in order to instruct persistence on how to act on the incoming message (i.e. persist state).
   */
  protected final def Effect: EffectFactories[State] =
    EffectFactories.asInstanceOf[EffectFactories[State]]

  /**
   * Implement by returning the initial empty state object.
   * This object will be passed into this behaviors handlers, until a new state replaces it.
   *
   * Also known as "zero state" or "neutral state".
   */
  protected def emptyState: State

  /**
   * Implement by handling incoming commands and return an `Effect()` to persist or signal other effects
   * of the command handling such as stopping the behavior or others.
   *
   * Use [[DurableStateBehavior#newCommandHandlerBuilder]] to define the command handlers.
   *
   * The command handlers are only invoked when the actor is running (i.e. not recovering).
   * While the actor is persisting state, the incoming messages are stashed and only
   * delivered to the handler once persisting them has completed.
   */
  protected def onCommand(state: State, command: Command): Effect[State]

  /**
   * Override to react on general lifecycle signals and `DurableStateBehavior` specific signals
   * (recovery related). Those are all subtypes of [[akka.persistence.typed.state.DurableStateSignal]].
   *
   * Use [[DurableStateBehavior#newSignalHandlerBuilder]] to define the signal handler.
   */
  protected def signalHandler(): SignalHandler[State] = SignalHandler.empty[State]

  /**
   * @return A new, mutable signal handler builder
   */
  protected final def newSignalHandlerBuilder(): SignalHandlerBuilder[State] =
    SignalHandlerBuilder.builder[State]

  /**
   * API May Change: Override this and implement the [[ChangeEventHandler]] to store additional change event
   * when the state is updated or deleted. The event can be used in Projections.
   */
  @ApiMayChange
  protected def changeEventHandler(): ChangeEventHandler[Command, State, _] =
    ChangeEventHandler.undefined[Command, State, Any]

  /**
   * Override and define the `DurableStateStore` plugin id that this actor should use instead of the default.
   */
  def durableStateStorePluginId: String = ""

  /**
   * The tag that can be used in persistence query.
   */
  def tag: String = ""

  /**
   * Transform the state into another type before giving it to and from the store. Can be used
   * to migrate from different state types e.g. when migration from PersistentFSM to Typed DurableStateBehavior.
   */
  def snapshotAdapter(): SnapshotAdapter[State] = NoOpSnapshotAdapter.instance[State]

  /**
   * INTERNAL API: DeferredBehavior init, not for user extension
   */
  @InternalApi override def apply(context: typed.TypedActorContext[Command]): Behavior[Command] =
    createDurableStateBehavior()

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final def createDurableStateBehavior(): scaladsl.DurableStateBehavior[Command, State] = {

    val behavior = new internal.DurableStateBehaviorImpl[Command, State](
      persistenceId,
      () => emptyState,
      onCommand(_, _).asInstanceOf[EffectImpl[State]],
      getClass).withTag(tag).snapshotAdapter(snapshotAdapter()).withDurableStateStorePluginId(durableStateStorePluginId)

    val behaviorWithSignalHandler = {
      val handler = signalHandler()
      if (handler.isEmpty) behavior
      else behavior.receiveSignal(handler.handler)
    }

    val withSignalHandler =
      if (onPersistFailure.isPresent)
        behaviorWithSignalHandler.onPersistFailure(onPersistFailure.get)
      else
        behaviorWithSignalHandler

    val withChangeEventHandler = changeEventHandler() match {
      case handler if handler eq ChangeEventHandler.Undefined => withSignalHandler
      case handler: ChangeEventHandler[Command, State, _] @unchecked =>
        withSignalHandler.withChangeEventHandler(
          scaladsl.ChangeEventHandler(
            updateHandler = (previousState, newState, command) =>
              handler.updateHandler(previousState, newState, command),
            deleteHandler = (previousState, command) => handler.deleteHandler(previousState, command)))
    }

    if (stashCapacity.isPresent) {
      withChangeEventHandler.withStashCapacity(stashCapacity.get)
    } else {
      withChangeEventHandler
    }
  }

  /**
   * The last sequence number that was persisted, can only be called from inside the handlers of a `DurableStateBehavior`
   */
  final def lastSequenceNumber(ctx: ActorContext[_]): Long = {
    scaladsl.DurableStateBehavior.lastSequenceNumber(ctx.asScala)
  }

  /**
   * Override to define a custom stash capacity per entity.
   * If not defined, the default `akka.persistence.typed.stash-capacity` will be used.
   */
  def stashCapacity: Optional[java.lang.Integer] = Optional.empty()

}
