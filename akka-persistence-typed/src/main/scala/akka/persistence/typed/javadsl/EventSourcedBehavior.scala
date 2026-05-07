/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import akka.Done
import akka.actor.typed
import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.annotation.InternalApi
import akka.persistence.typed.EventAdapter
import akka.persistence.typed._
import akka.persistence.typed.internal._
import java.util.Collections
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.annotation.nowarn
import scala.reflect.ClassTag

/**
 * For projects using Java 17 and newer, also see [[EventSourcedOnCommandBehavior]]
 */
abstract class EventSourcedBehavior[Command, Event, State] private[akka] (
    val persistenceId: PersistenceId,
    onPersistFailure: Optional[BackoffSupervisorStrategy])
    extends DeferredBehavior[Command] {

  /**
   * @param persistenceId stable unique identifier for the event sourced behavior
   */
  def this(persistenceId: PersistenceId) = {
    this(persistenceId, Optional.empty[BackoffSupervisorStrategy])
  }

  /**
   * If using onPersistFailure the supervision is only around the event sourced behavior not any outer setup/withTimers
   * block. If using restart any actions e.g. scheduling timers, can be done on the PreRestart signal or on the
   * RecoveryCompleted signal.
   *
   * @param persistenceId stable unique identifier for the event sourced behavior
   * @param onPersistFailure BackoffSupervisionStrategy for persist failures
   */
  def this(persistenceId: PersistenceId, onPersistFailure: BackoffSupervisorStrategy) = {
    this(persistenceId, Optional.ofNullable(onPersistFailure))
  }

  /**
   * Factory of effects.
   *
   * Return effects from your handlers in order to instruct persistence on how to act on the incoming message (i.e. persist events).
   */
  protected final def Effect: EffectFactories[Event, State] =
    EffectFactories.asInstanceOf[EffectFactories[Event, State]]

  /**
   * Implement by returning the initial empty state object.
   * This object will be passed into this behaviors handlers, until a new state replaces it.
   *
   * Also known as "zero state" or "neutral state".
   *
   * If the state is mutable, it is important that this creates a new State instance each time it is called
   * to ensure that the state is recreated in case of failure restarts.
   */
  protected def emptyState: State

  /**
   * Implement by handling incoming commands and return an `Effect()` to persist or signal other effects
   * of the command handling such as stopping the behavior or others.
   *
   * Use [[EventSourcedBehavior#newCommandHandlerBuilder]] to define the command handlers.
   *
   * The command handlers are only invoked when the actor is running (i.e. not replaying).
   * While the actor is persisting events, the incoming messages are stashed and only
   * delivered to the handler once persisting them has completed.
   */
  protected def commandHandler(): CommandHandler[Command, Event, State]

  /**
   * Implement by applying the event to the current state in order to return a new state.
   *
   * Use [[EventSourcedBehavior#newEventHandlerBuilder]] to define the event handlers.
   *
   * The event handlers are invoked during recovery as well as running operation of this behavior,
   * in order to keep updating the state state.
   *
   * For that reason it is strongly discouraged to perform side-effects in this handler;
   * Side effects should be executed in `thenRun` or `recoveryCompleted` blocks.
   */
  protected def eventHandler(): EventHandler[State, Event]

  /**
   * Override to react on general lifecycle signals and persistence specific signals (subtypes of
   * [[akka.persistence.typed.EventSourcedSignal]]).
   *
   * Use [[EventSourcedBehavior#newSignalHandlerBuilder]] to define the signal handler.
   */
  protected def signalHandler(): SignalHandler[State] = SignalHandler.empty[State]

  /**
   * @return A new, mutable signal handler builder
   */
  protected final def newSignalHandlerBuilder(): SignalHandlerBuilder[State] =
    SignalHandlerBuilder.builder[State]

  /**
   * @return A new, mutable, command handler builder
   */
  protected def newCommandHandlerBuilder(): CommandHandlerBuilder[Command, Event, State] = {
    CommandHandlerBuilder.builder[Command, Event, State]()
  }

  /**
   * @return A new, mutable, event handler builder
   */
  protected final def newEventHandlerBuilder(): EventHandlerBuilder[State, Event] =
    EventHandlerBuilder.builder[State, Event]()

  /**
   * Override and define the journal plugin id that this actor should use instead of the default.
   */
  def journalPluginId: String = ""

  /**
   * Override and define the snapshot store plugin id that this actor should use instead of the default.
   */
  def snapshotPluginId: String = ""

  /**
   * Override and define the snapshot selection criteria used by this actor instead of the default.
   * By default the most recent snapshot is used, and the remaining state updates are recovered by replaying events
   * from the sequence number up until which the snapshot reached.
   *
   * You may configure the behavior to skip replaying snapshots completely, in which case the recovery will be
   * performed by replaying all events -- which may take a long time.
   */
  @deprecated("override recovery instead", "2.6.5")
  def snapshotSelectionCriteria: SnapshotSelectionCriteria = SnapshotSelectionCriteria.latest

  /**
   * Initiates a snapshot if the given predicate evaluates to true.
   *
   * Decide to store a snapshot based on the State, Event and sequenceNr when the event has
   * been successfully persisted.
   *
   * When persisting multiple events at once the snapshot is triggered after all the events have
   * been persisted.
   *
   * Snapshots triggered by `shouldSnapshot` will not trigger deletes of old snapshots and events if
   * [[EventSourcedBehavior.retentionCriteria]] with [[RetentionCriteria.snapshotEvery]] is used together with
   * `shouldSnapshot`. Such deletes are only triggered by snapshots matching the `numberOfEvents` in the
   * [[RetentionCriteria]].
   *
   * Events can be deleted if `deleteEventsOnSnapshot` returns `true`.
   *
   * @return `true` if snapshot should be saved at the given `state`, `event` and `sequenceNr` when the event has
   *         been successfully persisted
   */
  def shouldSnapshot(state: State, event: Event, sequenceNr: Long): Boolean = false

  /**
   * Can be used to delete events after `shouldSnapshot`.
   *
   * Can be used in combination with `[[EventSourcedBehavior.retentionCriteria]]` in a way that events are triggered
   * up the the oldest snapshot based on `[[RetentionCriteria.snapshotEvery]]` config.
   *
   * @return `true` if events should be deleted after `shouldSnapshot` evaluates to true
   */
  def deleteEventsOnSnapshot: Boolean = false

  /**
   * Criteria for retention/deletion of snapshots and events.
   * By default, retention is disabled and snapshots are not saved and deleted automatically.
   */
  def retentionCriteria: RetentionCriteria = RetentionCriteria.disabled

  /**
   * Override to change the strategy for recovery of snapshots and events.
   * By default, snapshots and events are recovered.
   */
  def recovery: Recovery = Recovery.enabled

  /**
   * Return tags to store for the given event, the tags can then be used in persistence query.
   *
   * If [[tagsFor(Event, State)]] is overriden this method is ignored.
   */
  def tagsFor(@nowarn("msg=never used") event: Event): java.util.Set[String] = Collections.emptySet()

  /**
   * Return tags to store for the given event and state, the tags can then be used in persistence query.
   * The state passed to the tagger allows for toggling a tag with one event but keep all events after it tagged
   * based on a property or the type of the state.
   */
  def tagsFor(@nowarn("msg=never used") state: State, event: Event): java.util.Set[String] =
    tagsFor(event)

  /**
   * Transform the event in another type before giving to the journal. Can be used to wrap events
   * in types Journals understand but is of a different type than `Event`.
   */
  def eventAdapter(): EventAdapter[Event, _] = NoOpEventAdapter.instance[Event]

  /**
   * Transform the state into another type before giving it to and from the journal. Can be used
   * to migrate from different state types e.g. when migration from PersistentFSM to Typed EventSourcedBehavior.
   */
  def snapshotAdapter(): SnapshotAdapter[State] = NoOpSnapshotAdapter.instance[State]

  /**
   * INTERNAL API: DeferredBehavior init, not for user extension
   */
  @InternalApi override def apply(context: typed.TypedActorContext[Command]): Behavior[Command] =
    createEventSourcedBehavior()

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createEventSourcedBehavior(): scaladsl.EventSourcedBehavior[Command, Event, State] = {
    val snapshotWhen: (State, Event, Long) => Boolean = (state, event, seqNr) => shouldSnapshot(state, event, seqNr)

    val tagger: (State, Event) => Set[String] = { (state, event) =>
      import scala.jdk.CollectionConverters._
      val tags = tagsFor(state, event)
      if (tags.isEmpty) Set.empty
      else tags.asScala.toSet
    }

    val commandHandlerInstance = commandHandler()
    val eventHandlerInstance = eventHandler()
    var behavior = new internal.EventSourcedBehaviorImpl[Command, Event, State](
      persistenceId,
      () => emptyState,
      (state, cmd) => commandHandlerInstance(state, cmd).asInstanceOf[EffectImpl[Event, State]],
      eventHandlerInstance(_, _),
      getClass)
      .snapshotWhen(snapshotWhen, deleteEventsOnSnapshot)
      .withRetention(retentionCriteria.asScala)
      .withTaggerForState(tagger)
      .eventAdapter(eventAdapter())
      .snapshotAdapter(snapshotAdapter())
      .withJournalPluginId(journalPluginId)
      .withSnapshotPluginId(snapshotPluginId)
      .withRecovery(recovery.asScala)

    val handler = signalHandler()
    if (!handler.isEmpty) behavior = behavior.receiveSignal(handler.handler)
    onPersistFailure.ifPresent(opf => behavior = behavior.onPersistFailure(opf))
    stashCapacity.ifPresent(sc => behavior = behavior.withStashCapacity(sc))
    behavior
  }

  /**
   * The last sequence number that was persisted, can only be called from inside the handlers of an `EventSourcedBehavior`
   */
  final def lastSequenceNumber(ctx: ActorContext[_]): Long = {
    scaladsl.EventSourcedBehavior.lastSequenceNumber(ctx.asScala)
  }

  /**
   * The metadata of the given type that was persisted with an event, if any.
   * Can only be called from inside the event handler or `RecoveryCompleted` of an `EventSourcedBehavior`.
   */
  def getCurrentMetadata[M](ctx: ActorContext[_], metadataType: Class[M]): Optional[M] = {
    import scala.jdk.OptionConverters._
    implicit val ct: ClassTag[M] = ClassTag(metadataType)
    scaladsl.EventSourcedBehavior.currentMetadata(ctx.asScala).toJava
  }

  /**
   * Override to define a custom stash capacity per entity.
   * If not defined, the default `akka.persistence.typed.stash-capacity` will be used.
   */
  def stashCapacity: Optional[java.lang.Integer] = Optional.empty()

}

@FunctionalInterface
trait ReplicationInterceptor[Event, State] {

  /**
   * @param state Current state
   * @param event The replicated event
   * @param originReplica The replica where the event came from
   * @param sequenceNumber The local sequence number the event will get when persisted
   * @return
   */
  def intercept(state: State, event: Event, originReplica: ReplicaId, sequenceNumber: Long): CompletionStage[Done]
}

/**
 * A [[EventSourcedBehavior]] that is enforcing that replies to commands are not forgotten.
 * There will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
 * created with `Effects().reply`, `Effects().noReply`, [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
 */
abstract class EventSourcedBehaviorWithEnforcedReplies[Command, Event, State](
    persistenceId: PersistenceId,
    backoffSupervisorStrategy: Optional[BackoffSupervisorStrategy])
    extends EventSourcedBehavior[Command, Event, State](persistenceId, backoffSupervisorStrategy) {

  def this(persistenceId: PersistenceId) = {
    this(persistenceId, Optional.empty[BackoffSupervisorStrategy])
  }

  def this(persistenceId: PersistenceId, backoffSupervisorStrategy: BackoffSupervisorStrategy) = {
    this(persistenceId, Optional.ofNullable(backoffSupervisorStrategy))
  }

  /**
   * Implement by handling incoming commands and return an `Effect()` to persist or signal other effects
   * of the command handling such as stopping the behavior or others.
   *
   * Use [[EventSourcedBehaviorWithEnforcedReplies#newCommandHandlerWithReplyBuilder]] to define the command handlers.
   *
   * The command handlers are only invoked when the actor is running (i.e. not replaying).
   * While the actor is persisting events, the incoming messages are stashed and only
   * delivered to the handler once persisting them has completed.
   */
  override protected def commandHandler(): CommandHandlerWithReply[Command, Event, State]

  /**
   * @return A new, mutable, command handler builder
   */
  protected def newCommandHandlerWithReplyBuilder(): CommandHandlerWithReplyBuilder[Command, Event, State] = {
    CommandHandlerWithReplyBuilder.builder[Command, Event, State]()
  }

  /**
   * Use [[EventSourcedBehaviorWithEnforcedReplies#newCommandHandlerWithReplyBuilder]] instead, or
   * extend [[EventSourcedBehavior]] instead of [[EventSourcedBehaviorWithEnforcedReplies]].
   *
   * @throws UnsupportedOperationException use newCommandHandlerWithReplyBuilder instead
   */
  override protected def newCommandHandlerBuilder(): CommandHandlerBuilder[Command, Event, State] =
    throw new UnsupportedOperationException("Use newCommandHandlerWithReplyBuilder instead")

}
