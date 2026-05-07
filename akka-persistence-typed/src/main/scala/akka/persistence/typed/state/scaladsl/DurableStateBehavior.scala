/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import scala.annotation.tailrec

import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.Signal
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.internal.InterceptorImpl
import akka.actor.typed.internal.LoggerClass
import akka.actor.typed.scaladsl.ActorContext
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.state.internal._

/**
 * API May Change
 */
@ApiMayChange
object DurableStateBehavior {

  /**
   * Type alias for the command handler function that defines how to act on commands.
   *
   * The type alias is not used in API signatures because it's easier to see (in IDE) what is needed
   * when full function type is used. When defining the handler as a separate function value it can
   * be useful to use the alias for shorter type signature.
   */
  type CommandHandler[Command, State] = (State, Command) => Effect[State]

  private val logPrefixSkipList = classOf[DurableStateBehavior[_, _]].getName :: Nil

  /**
   * Create a `Behavior` for a persistent actor with durable storage of its state.
   *
   * This can be used when the state is immutable, but if the state is mutable, it is important to use
   * the `withMutableState` that takes `emptyStateFactory: () => State` parameter.
   *
   * @param persistenceId stable unique identifier for the `DurableStateBehavior`
   * @param emptyState the intial state for the entity before any state has been stored
   * @param commandHandler map commands to effects e.g. persisting state, replying to commands
   */
  def apply[Command, State](
      persistenceId: PersistenceId,
      emptyState: State,
      commandHandler: (State, Command) => Effect[State]): DurableStateBehavior[Command, State] = {
    withMutableState(persistenceId, () => emptyState, commandHandler)
  }

  /**
   * Create a `Behavior` with mutable state for a persistent actor with durable storage of its state.
   *
   * When the state is mutable, it is important to use this variant to make sure that the state instance is
   * recreated in case of failure restarts.
   *
   * @param persistenceId stable unique identifier for the `DurableStateBehavior`
   * @param emptyStateFactory factory function of the intial state for the entity before any state has been stored
   * @param commandHandler map commands to effects e.g. persisting state, replying to commands
   */
  def withMutableState[Command, State](
      persistenceId: PersistenceId,
      emptyStateFactory: () => State,
      commandHandler: (State, Command) => Effect[State]): DurableStateBehavior[Command, State] = {
    val loggerClass = LoggerClass.detectLoggerClassFromStack(classOf[DurableStateBehavior[_, _]], logPrefixSkipList)
    DurableStateBehaviorImpl(persistenceId, emptyStateFactory, commandHandler, loggerClass)
  }

  /**
   * Create a `Behavior` for a persistent actor that is enforcing that replies to commands are not forgotten.
   * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
   * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
   *
   * This can be used when the state is immutable, but if the state is mutable, it is important to use
   * the `withEnforcedRepliesMutableState` that takes `emptyStateFactory: () => State` parameter.
   */
  def withEnforcedReplies[Command, State](
      persistenceId: PersistenceId,
      emptyState: State,
      commandHandler: (State, Command) => ReplyEffect[State]): DurableStateBehavior[Command, State] = {
    withEnforcedRepliesMutableState(persistenceId, () => emptyState, commandHandler)
  }

  /**
   * Create a `Behavior` with mutable state for a persistent actor that is enforcing that replies to commands are not forgotten.
   * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
   * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
   *
   * When the state is mutable, it is important to use this variant to make sure that the state instance is
   * recreated in case of failure restarts.
   */
  def withEnforcedRepliesMutableState[Command, State](
      persistenceId: PersistenceId,
      emptyStateFactory: () => State,
      commandHandler: (State, Command) => ReplyEffect[State]): DurableStateBehavior[Command, State] = {
    val loggerClass = LoggerClass.detectLoggerClassFromStack(classOf[DurableStateBehavior[_, _]], logPrefixSkipList)
    DurableStateBehaviorImpl(persistenceId, emptyStateFactory, commandHandler, loggerClass)
  }

  /**
   * The `CommandHandler` defines how to act on commands. A `CommandHandler` is
   * a function:
   *
   * {{{
   *   (State, Command) => Effect[State]
   * }}}
   *
   * The [[CommandHandler#command]] is useful for simple commands that don't need the state
   * and context.
   */
  object CommandHandler {

    /**
     * Convenience for simple commands that don't need the state and context.
     *
     * @see [[Effect]] for possible effects of a command.
     */
    def command[Command, State](commandHandler: Command => Effect[State]): (State, Command) => Effect[State] =
      (_, cmd) => commandHandler(cmd)

  }

  /**
   * The last sequence number that was persisted, can only be called from inside the handlers of a `DurableStateBehavior`
   */
  def lastSequenceNumber(context: ActorContext[_]): Long = {
    @tailrec
    def extractConcreteBehavior(beh: Behavior[_]): Behavior[_] =
      beh match {
        case interceptor: InterceptorImpl[_, _] => extractConcreteBehavior(interceptor.nestedBehavior)
        case concrete                           => concrete
      }

    extractConcreteBehavior(context.currentBehavior) match {
      case w: Running.WithRevisionAccessible => w.currentRevision
      case s =>
        throw new IllegalStateException(s"Cannot extract the lastSequenceNumber in state ${s.getClass.getName}")
    }
  }

}

/**
 * Further customization of the `DurableStateBehavior` can be done with the methods defined here.
 *
 * Not for user extension
 *
 * API May Change
 */
@ApiMayChange @DoNotInherit trait DurableStateBehavior[Command, State] extends DeferredBehavior[Command] {

  def persistenceId: PersistenceId

  /**
   * Allows the `DurableStateBehavior` to react on signals.
   *
   * The regular lifecycle signals can be handled as well as `DurableStateBehavior` specific signals
   * (recovery related). Those are all subtypes of [[akka.persistence.typed.state.DurableStateSignal]]
   */
  def receiveSignal(signalHandler: PartialFunction[(State, Signal), Unit]): DurableStateBehavior[Command, State]

  /**
   * @return The currently defined signal handler or an empty handler if no custom handler previously defined
   */
  def signalHandler: PartialFunction[(State, Signal), Unit]

  /**
   * Change the `DurableStateStore` plugin id that this actor should use.
   */
  def withDurableStateStorePluginId(id: String): DurableStateBehavior[Command, State]

  /**
   * The tag that can used in persistence query
   */
  def withTag(tag: String): DurableStateBehavior[Command, State]

  /**
   * Transform the state to another type before giving to the store. Can be used to transform older
   * state types into the current state type e.g. when migrating from Persistent FSM to Typed DurableStateBehavior.
   */
  def snapshotAdapter(adapter: SnapshotAdapter[State]): DurableStateBehavior[Command, State]

  /**
   * Back off strategy for persist failures.
   *
   * Specifically BackOff to prevent resume being used. Resume is not allowed as
   * it will be unknown if the state has been persisted.
   *
   * This supervision is only around the `DurableStateBehavior` not any outer setup/withTimers
   * block. If using restart, any actions e.g. scheduling timers, can be done on the PreRestart
   *
   * If not specified the actor will be stopped on failure.
   */
  def onPersistFailure(backoffStrategy: BackoffSupervisorStrategy): DurableStateBehavior[Command, State]

  /**
   * Define a custom stash capacity per entity.
   * If not defined, the default `akka.persistence.typed.stash-capacity` will be used.
   */
  def withStashCapacity(size: Int): DurableStateBehavior[Command, State]

  /**
   * API May Change: Store additional change event when the state is updated or deleted.
   * The event can be used in Projections.
   */
  @ApiMayChange
  def withChangeEventHandler[ChangeEvent](
      handler: ChangeEventHandler[Command, State, ChangeEvent]): DurableStateBehavior[Command, State]

}
