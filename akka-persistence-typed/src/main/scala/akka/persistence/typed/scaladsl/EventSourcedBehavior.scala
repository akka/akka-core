/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import akka.Done
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
import akka.annotation.InternalApi
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.SnapshotSelectionCriteria
import akka.persistence.typed.internal._
import scala.concurrent.Future
import scala.reflect.ClassTag

import akka.annotation.InternalStableApi

object EventSourcedBehavior {

  /**
   * Type alias for the command handler function that defines how to act on commands.
   *
   * The type alias is not used in API signatures because it's easier to see (in IDE) what is needed
   * when full function type is used. When defining the handler as a separate function value it can
   * be useful to use the alias for shorter type signature.
   */
  type CommandHandler[Command, Event, State] = (State, Command) => Effect[Event, State]

  /**
   * Type alias for the event handler function for updating the state based on events having been persisted.
   *
   * The type alias is not used in API signatures because it's easier to see (in IDE) what is needed
   * when full function type is used. When defining the handler as a separate function value it can
   * be useful to use the alias for shorter type signature.
   */
  type EventHandler[State, Event] = (State, Event) => State

  private val logPrefixSkipList = classOf[EventSourcedBehavior[_, _, _]].getName :: Nil

  /**
   * Create a `Behavior` for a persistent actor.
   *
   * This can be used when the state is immutable, but if the state is mutable, it is important to use
   * the `withMutableState` that takes `emptyStateFactory: () => State` parameter.
   *
   * @param persistenceId stable unique identifier for the event sourced behavior
   * @param emptyState the intial state for the entity before any events have been processed
   * @param commandHandler map commands to effects e.g. persisting events, replying to commands
   * @param eventHandler compute the new state given the current state when an event has been persisted
   */
  def apply[Command, Event, State](
      persistenceId: PersistenceId,
      emptyState: State,
      commandHandler: (State, Command) => Effect[Event, State],
      eventHandler: (State, Event) => State): EventSourcedBehavior[Command, Event, State] = {
    withMutableState(persistenceId, () => emptyState, commandHandler, eventHandler)
  }

  /**
   * Create a `Behavior` with mutable state for a persistent actor.
   *
   * When the state is mutable, it is important to use this variant to make sure that the state instance is
   * recreated in case of failure restarts.
   *
   * @param persistenceId stable unique identifier for the event sourced behavior
   * @param emptyStateFactory factory function of the intial state for the entity before any events have been processed
   * @param commandHandler map commands to effects e.g. persisting events, replying to commands
   * @param eventHandler compute the new state given the current state when an event has been persisted
   */
  def withMutableState[Command, Event, State](
      persistenceId: PersistenceId,
      emptyStateFactory: () => State,
      commandHandler: (State, Command) => Effect[Event, State],
      eventHandler: (State, Event) => State): EventSourcedBehavior[Command, Event, State] = {
    val loggerClass = LoggerClass.detectLoggerClassFromStack(classOf[EventSourcedBehavior[_, _, _]], logPrefixSkipList)
    EventSourcedBehaviorImpl(persistenceId, emptyStateFactory, commandHandler, eventHandler, loggerClass)
  }

  /**
   * Create a `Behavior` for a persistent actor that is enforcing that replies to commands are not forgotten.
   * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
   * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
   *
   * This can be used when the state is immutable, but if the state is mutable, it is important to use
   * the `withEnforcedRepliesMutableState` that takes `emptyStateFactory: () => State` parameter.
   */
  def withEnforcedReplies[Command, Event, State](
      persistenceId: PersistenceId,
      emptyState: State,
      commandHandler: (State, Command) => ReplyEffect[Event, State],
      eventHandler: (State, Event) => State): EventSourcedBehavior[Command, Event, State] = {
    withEnforcedRepliesMutableState(persistenceId, () => emptyState, commandHandler, eventHandler)
  }

  /**
   * Create a `Behavior` with mutable state for a persistent actor that is enforcing that replies to commands are not forgotten.
   * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
   * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
   *
   * When the state is mutable, it is important to use this variant to make sure that the state instance is
   * recreated in case of failure restarts.
   */
  def withEnforcedRepliesMutableState[Command, Event, State](
      persistenceId: PersistenceId,
      emptyStateFactory: () => State,
      commandHandler: (State, Command) => ReplyEffect[Event, State],
      eventHandler: (State, Event) => State): EventSourcedBehavior[Command, Event, State] = {
    val loggerClass = LoggerClass.detectLoggerClassFromStack(classOf[EventSourcedBehavior[_, _, _]], logPrefixSkipList)
    EventSourcedBehaviorImpl(persistenceId, emptyStateFactory, commandHandler, eventHandler, loggerClass)
  }

  /**
   * The `CommandHandler` defines how to act on commands. A `CommandHandler` is
   * a function:
   *
   * {{{
   *   (State, Command) => Effect[Event, State]
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
    def command[Command, Event, State](
        commandHandler: Command => Effect[Event, State]): (State, Command) => Effect[Event, State] =
      (_, cmd) => commandHandler(cmd)

  }

  /**
   * The last sequence number that was persisted, can only be called from inside the handlers of an `EventSourcedBehavior`
   */
  def lastSequenceNumber(context: ActorContext[_]): Long = {
    @tailrec
    def extractConcreteBehavior(beh: Behavior[_]): Behavior[_] =
      beh match {
        case interceptor: InterceptorImpl[_, _] => extractConcreteBehavior(interceptor.nestedBehavior)
        case concrete                           => concrete
      }

    extractConcreteBehavior(context.currentBehavior) match {
      case w: EventSourcedBehaviorImpl.WithSeqNrAccessible => w.currentSequenceNumber
      case s =>
        throw new IllegalStateException(s"Cannot extract the lastSequenceNumber in state ${s.getClass.getName}")
    }
  }

  /**
   * The metadata of the given type that was persisted with an event, if any.
   * Can only be called from inside the event handler or `RecoveryCompleted` of an `EventSourcedBehavior`.
   */
  def currentMetadata[M: ClassTag](context: ActorContext[_]): Option[M] = {
    @tailrec
    def extractConcreteBehavior(beh: Behavior[_]): Behavior[_] =
      beh match {
        case interceptor: InterceptorImpl[_, _] => extractConcreteBehavior(interceptor.nestedBehavior)
        case concrete                           => concrete
      }

    extractConcreteBehavior(context.currentBehavior) match {
      case w: EventSourcedBehaviorImpl.WithMetadataAccessible => w.metadata[M]
      case s =>
        throw new IllegalStateException(s"Cannot extract the metadata in state ${s.getClass.getName}")
    }
  }

}

/**
 * Further customization of the `EventSourcedBehavior` can be done with the methods defined here.
 *
 * Not for user extension
 */
@DoNotInherit trait EventSourcedBehavior[Command, Event, State] extends DeferredBehavior[Command] {

  def persistenceId: PersistenceId

  /**
   * Allows the event sourced behavior to react on signals.
   *
   * The regular lifecycle signals can be handled as well as
   * Akka Persistence specific signals (snapshot and recovery related). Those are all subtypes of
   * [[akka.persistence.typed.EventSourcedSignal]]
   */
  def receiveSignal(signalHandler: PartialFunction[(State, Signal), Unit]): EventSourcedBehavior[Command, Event, State]

  /**
   * @return The currently defined signal handler or an empty handler if no custom handler previously defined
   */
  def signalHandler: PartialFunction[(State, Signal), Unit]

  /**
   * Change the journal plugin id that this actor should use.
   */
  def withJournalPluginId(id: String): EventSourcedBehavior[Command, Event, State]

  /**
   * Change the snapshot store plugin id that this actor should use.
   */
  def withSnapshotPluginId(id: String): EventSourcedBehavior[Command, Event, State]

  /**
   * Changes the snapshot selection criteria used by this behavior.
   * By default the most recent snapshot is used, and the remaining state updates are recovered by replaying events
   * from the sequence number up until which the snapshot reached.
   *
   * You may configure the behavior to skip replaying snapshots completely, in which case the recovery will be
   * performed by replaying all events -- which may take a long time.
   */
  @deprecated("use withRecovery(Recovery.withSnapshotSelectionCriteria(...))", "2.6.5")
  def withSnapshotSelectionCriteria(selection: SnapshotSelectionCriteria): EventSourcedBehavior[Command, Event, State]

  /**
   * Initiates a snapshot if the given `predicate` evaluates to true.
   *
   * Decide to store a snapshot based on the State, Event and sequenceNr when the event has
   * been successfully persisted.
   *
   * When persisting multiple events at once the snapshot is triggered after all the events have
   * been persisted.
   *
   * Snapshots triggered by `snapshotWhen` will not trigger deletes of old snapshots and events if
   * [[EventSourcedBehavior.withRetention]] with [[RetentionCriteria.snapshotEvery]] is used together with
   * `snapshotWhen`. Such deletes are only triggered by snapshots matching the `numberOfEvents` in the
   * [[RetentionCriteria]].
   *
   * Events can be deleted if `snapshotWhen(predicate, deleteEventsOnSnapshot = true)` is used.
   */
  def snapshotWhen(predicate: (State, Event, Long) => Boolean): EventSourcedBehavior[Command, Event, State]

  /**
   * Can be used to delete events after `shouldSnapshot`.
   *
   * Can be used in combination with `[[EventSourcedBehavior.retentionCriteria]]` in a way that events are triggered
   * up the the oldest snapshot based on `[[RetentionCriteria.snapshotEvery]]` config.
   */
  def snapshotWhen(
      predicate: (State, Event, Long) => Boolean,
      deleteEventsOnSnapshot: Boolean): EventSourcedBehavior[Command, Event, State]

  /**
   * Criteria for retention/deletion of snapshots and events.
   * By default, retention is disabled and snapshots are not saved and deleted automatically.
   */
  def withRetention(criteria: RetentionCriteria): EventSourcedBehavior[Command, Event, State]

  /**
   * The `tagger` function should give event tags, which will be used in persistence query
   */
  def withTagger(tagger: Event => Set[String]): EventSourcedBehavior[Command, Event, State]

  /**
   * The `tagger` function should give event tags, which will be used in persistence query.
   * The state passed to the tagger allows for toggling a tag with one event but keep all events after it tagged
   * based on a property or the type of the state.
   */
  def withTaggerForState(tagger: (State, Event) => Set[String]): EventSourcedBehavior[Command, Event, State]

  /**
   * Transform the event to another type before giving to the journal. Can be used to wrap events
   * in types Journals understand but is of a different type than `Event`.
   */
  def eventAdapter(adapter: EventAdapter[Event, _]): EventSourcedBehavior[Command, Event, State]

  /**
   * Transform the state to another type before giving to the journal. Can be used to transform older
   * state types into the current state type e.g. when migrating from Persistent FSM to Typed EventSourcedBehavior.
   */
  def snapshotAdapter(adapter: SnapshotAdapter[State]): EventSourcedBehavior[Command, Event, State]

  /**
   * Back off strategy for persist failures.
   *
   * Specifically BackOff to prevent resume being used. Resume is not allowed as
   * it will be unknown if the event has been persisted.
   *
   * This supervision is only around the event sourced behavior not any outer setup/withTimers
   * block. If using restart, any actions e.g. scheduling timers, can be done on the PreRestart
   *
   * If not specified the actor will be stopped on failure.
   */
  def onPersistFailure(backoffStrategy: BackoffSupervisorStrategy): EventSourcedBehavior[Command, Event, State]

  /**
   * Change the recovery strategy.
   * By default, snapshots and events are recovered.
   */
  def withRecovery(recovery: Recovery): EventSourcedBehavior[Command, Event, State]

  /**
   * Publish events to the system event stream as [[akka.persistence.typed.PublishedEvent]] after they have been persisted
   */
  @ApiMayChange
  def withEventPublishing(enabled: Boolean): EventSourcedBehavior[Command, Event, State]

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def withReplication(context: ReplicationContextImpl): EventSourcedBehavior[Command, Event, State]

  /**
   * Define a custom stash capacity per entity.
   * If not defined, the default `akka.persistence.typed.stash-capacity` will be used.
   */
  def withStashCapacity(size: Int): EventSourcedBehavior[Command, Event, State]

  /**
   * Invoke this callback when an event from another replica arrives, delaying persisting the event until the returned
   * future completes, if the future fails the actor is crashed.
   *
   * Only used when the entity is replicated.
   */
  @ApiMayChange
  def withReplicatedEventInterceptor(
      interceptor: ReplicationInterceptor[State, Event]): EventSourcedBehavior[Command, Event, State]

  /**
   * INTERNAL API: Invoke this transformation function when an event from another replica arrives, before persisting the event and
   * before calling the ordinary event handler. The transformation function returns the updated event and optionally
   * additional metadata that will be stored together with the event.
   *
   * Only used when the entity is replicated.
   */
  @ApiMayChange
  @InternalStableApi
  @deprecated("Use withReplicatedEventsTransformation", "2.10.12")
  def withReplicatedEventTransformation(
      f: (State, EventWithMetadata[Event]) => EventWithMetadata[Event]): EventSourcedBehavior[Command, Event, State]

  /**
   * INTERNAL API: Invoke this transformation function when an event from another replica arrives, before persisting the event and
   * before calling the ordinary event handler. The transformation function returns the updated event, and possibly
   * additional events, and optionally additional metadata that will be stored together with the events.
   *
   * Only used when the entity is replicated.
   */
  @ApiMayChange
  @InternalStableApi
  def withReplicatedEventsTransformation(f: (State, EventWithMetadata[Event]) => Seq[EventWithMetadata[Event]])
      : EventSourcedBehavior[Command, Event, State]
}

@FunctionalInterface
trait ReplicationInterceptor[State, Event] {

  /**
   * @param state Current state
   * @param event The replicated event
   * @param originReplica The replica where the event came from
   * @param sequenceNumber The local sequence number the event will get when persisted
   * @return
   */
  def intercept(state: State, event: Event, originReplica: ReplicaId, sequenceNumber: Long): Future[Done]
}
