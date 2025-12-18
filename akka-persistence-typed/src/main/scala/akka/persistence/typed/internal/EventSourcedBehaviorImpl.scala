/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import org.slf4j.LoggerFactory

import akka.Done
import akka.actor.typed
import akka.actor.typed.ActorRef
import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.BehaviorInterceptor
import akka.actor.typed.PostStop
import akka.actor.typed.Signal
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.internal.ActorContextImpl
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation._
import akka.persistence.JournalProtocol
import akka.persistence.RecoveryPermitter
import akka.persistence.SnapshotProtocol
import akka.persistence.journal.Tagged
import akka.persistence.typed.DeleteEventsCompleted
import akka.persistence.typed.DeleteEventsFailed
import akka.persistence.typed.DeleteSnapshotsCompleted
import akka.persistence.typed.DeleteSnapshotsFailed
import akka.persistence.typed.DeletionTarget
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.NoOpEventAdapter
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.PublishedEvent
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.SnapshotCompleted
import akka.persistence.typed.SnapshotFailed
import akka.persistence.typed.SnapshotSelectionCriteria
import akka.persistence.typed.scaladsl._
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.persistence.typed.telemetry.EventSourcedBehaviorInstrumentationProvider
import akka.util.HashCode

@InternalApi
private[akka] object EventSourcedBehaviorImpl {

  object WriterIdentity {

    // ok to wrap around (2*Int.MaxValue restarts will not happen within a journal roundtrip)
    private[akka] val instanceIdCounter = new AtomicInteger(1)

    def newIdentity(): WriterIdentity = {
      val instanceId: Int = WriterIdentity.instanceIdCounter.getAndIncrement()
      val writerUuid: String = UUID.randomUUID.toString
      WriterIdentity(instanceId, writerUuid)
    }
  }
  final case class WriterIdentity(instanceId: Int, writerUuid: String)

  /**
   * Used by EventSourcedBehaviorTestKit to retrieve the `persistenceId`.
   */
  final case class GetPersistenceId(replyTo: ActorRef[PersistenceId]) extends Signal

  /**
   * Used by EventSourcedBehaviorTestKit to retrieve the state.
   * Can't be a Signal because those are not stashed.
   */
  final case class GetState[State](replyTo: ActorRef[GetStateReply[State]]) extends InternalProtocol

  /**
   * Used to send a state being `null` as an Actor message
   */
  final case class GetStateReply[State](currentState: State)

  /**
   * Used to start the replication stream at the correct sequence number
   */
  final case class GetSeenSequenceNr(replica: ReplicaId, replyTo: ActorRef[Long]) extends InternalProtocol

  /**
   * For testing purposes
   */
  final case class GetVersion(replyTo: ActorRef[VersionVector]) extends InternalProtocol

  trait WithSeqNrAccessible {
    def currentSequenceNumber: Long
  }

  trait WithMetadataAccessible {
    def metadata[M: ClassTag]: Option[M]
  }

  // Don't use it directly, but instead call internalLogger() (see below)
  private val loggerForInternal = LoggerFactory.getLogger(classOf[EventSourcedBehaviorImpl[_, _, _]])

}

@InternalApi
private[akka] final case class EventSourcedBehaviorImpl[Command, Event, State](
    persistenceId: PersistenceId,
    emptyState: () => State,
    commandHandler: EventSourcedBehavior.CommandHandler[Command, Event, State],
    eventHandler: EventSourcedBehavior.EventHandler[State, Event],
    loggerClass: Class[_],
    journalPluginId: Option[String] = None,
    snapshotPluginId: Option[String] = None,
    tagger: (State, Event) => Set[String] = (_: State, _: Event) => Set.empty[String],
    eventAdapter: EventAdapter[Event, Any] = NoOpEventAdapter.instance[Event],
    snapshotAdapter: SnapshotAdapter[State] = NoOpSnapshotAdapter.instance[State],
    snapshotWhen: SnapshotWhenPredicate[State, Event] = SnapshotWhenPredicate.noSnapshot[State, Event],
    recovery: Recovery = Recovery.default,
    retention: RetentionCriteria = RetentionCriteria.disabled,
    supervisionStrategy: SupervisorStrategy = SupervisorStrategy.stop,
    override val signalHandler: PartialFunction[(State, Signal), Unit] = PartialFunction.empty,
    replication: Option[ReplicationSetup] = None,
    publishEvents: Boolean = true,
    customStashCapacity: Option[Int] = None,
    replicatedEventInterceptor: Option[ReplicationInterceptor[State, Event]] = None,
    replicatedEventTransformation: Option[(State, EventWithMetadata[Event]) => Seq[EventWithMetadata[Event]]] = None)
    extends EventSourcedBehavior[Command, Event, State] {

  import EventSourcedBehaviorImpl.WriterIdentity

  if (persistenceId eq null)
    throw new IllegalArgumentException("persistenceId must not be null")

  if (replication.isDefined) {
    retention match {
      case s: SnapshotCountRetentionCriteriaImpl if s.deleteEventsOnSnapshot =>
        throw new IllegalArgumentException(
          "Retention criteria with deleteEventsOnSnapshot must not be used together with replication.")
      case _ => // ok
    }

    if (snapshotWhen.deleteEventsOnSnapshot)
      throw new IllegalArgumentException(
        "snapshotWhen with deleteEventsOnSnapshot must not be used together with replication.")
  }

  override def apply(context: typed.TypedActorContext[Command]): Behavior[Command] = {
    val ctx = context.asScala
    val hasCustomLoggerName = ctx match {
      case internalCtx: ActorContextImpl[_] => internalCtx.hasCustomLoggerName
      case _                                => false
    }
    if (!hasCustomLoggerName) ctx.setLoggerName(loggerClass)
    val settings = EventSourcedSettings(
      ctx.system,
      journalPluginId.getOrElse(""),
      snapshotPluginId.getOrElse(""),
      customStashCapacity)

    // stashState outside supervise because StashState should survive restarts due to persist failures
    val stashState = new StashState(ctx.asInstanceOf[ActorContext[InternalProtocol]], settings)

    // This method ensures that the MDC is set before we use the internal logger
    def internalLogger() = {
      if (settings.useContextLoggerForInternalLogging) ctx.log
      else {
        // MDC is cleared (if used) from aroundReceive in ActorAdapter after processing each message,
        // but important to call `context.log` to mark MDC as used
        ctx.log
        EventSourcedBehaviorImpl.loggerForInternal
      }
    }

    val actualSignalHandler: PartialFunction[(State, Signal), Unit] = signalHandler.orElse {
      // default signal handler is always the fallback
      case (_, SnapshotCompleted(meta)) =>
        internalLogger().debug("Save snapshot successful, snapshot metadata [{}].", meta)
      case (_, SnapshotFailed(meta, failure)) =>
        internalLogger()
          .error(s"Save snapshot failed, snapshot metadata [$meta] due to: ${failure.getMessage}", failure)
      case (_, DeleteSnapshotsCompleted(DeletionTarget.Individual(meta))) =>
        internalLogger().debug("Persistent snapshot [{}] deleted successfully.", meta)
      case (_, DeleteSnapshotsCompleted(DeletionTarget.Criteria(criteria))) =>
        internalLogger().debug("Persistent snapshots given criteria [{}] deleted successfully.", criteria)
      case (_, DeleteSnapshotsFailed(DeletionTarget.Individual(meta), failure)) =>
        internalLogger().warn("Failed to delete snapshot with meta [{}] due to: {}", meta, failure.getMessage)
      case (_, DeleteSnapshotsFailed(DeletionTarget.Criteria(criteria), failure)) =>
        internalLogger().warn("Failed to delete snapshots given criteria [{}] due to: {}", criteria, failure.getMessage)
      case (_, DeleteEventsCompleted(toSequenceNr)) =>
        internalLogger().debug("Events successfully deleted to sequence number [{}].", toSequenceNr)
      case (_, DeleteEventsFailed(toSequenceNr, failure)) =>
        internalLogger().warn(
          "Failed to delete events to sequence number [{}] due to: {}",
          toSequenceNr,
          failure.getMessage)
      case (_, EventSourcedBehaviorImpl.GetPersistenceId(replyTo)) => replyTo ! persistenceId
    }

    val instrumentation = EventSourcedBehaviorInstrumentationProvider(ctx.system).instrumentation
    // do this once, even if the actor is restarted
    instrumentation.actorInitialized(ctx.self)
    initialize(context.asScala)

    Behaviors
      .supervise {
        Behaviors.setup[Command] { _ =>
          val eventSourcedSetup = new BehaviorSetup(
            ctx.asInstanceOf[ActorContext[InternalProtocol]],
            persistenceId,
            emptyState(),
            commandHandler,
            eventHandler,
            WriterIdentity.newIdentity(),
            actualSignalHandler,
            tagger,
            eventAdapter,
            snapshotAdapter,
            snapshotWhen,
            recovery,
            retention,
            holdingRecoveryPermit = false,
            settings = settings,
            stashState = stashState,
            replication = replication,
            publishEvents = publishEvents,
            internalLoggerFactory = () => internalLogger(),
            retentionInProgress = false,
            instrumentation,
            replicatedEventInterceptor,
            replicatedEventTransformation)

          // needs to accept Any since we also can get messages from the journal
          // not part of the user facing Command protocol
          def interceptor: BehaviorInterceptor[Any, InternalProtocol] = new BehaviorInterceptor[Any, InternalProtocol] {

            import BehaviorInterceptor._
            override def aroundReceive(
                ctx: typed.TypedActorContext[Any],
                msg: Any,
                target: ReceiveTarget[InternalProtocol]): Behavior[InternalProtocol] = {
              val innerMsg = msg match {
                case res: JournalProtocol.Response           => InternalProtocol.JournalResponse(res)
                case res: SnapshotProtocol.Response          => InternalProtocol.SnapshotterResponse(res)
                case RecoveryPermitter.RecoveryPermitGranted => InternalProtocol.RecoveryPermitGranted
                case internal: InternalProtocol              => internal // such as RecoveryTickEvent
                case cmd                                     => InternalProtocol.IncomingCommand(cmd.asInstanceOf[Command])
              }
              target(ctx, innerMsg)
            }

            override def aroundSignal(
                ctx: typed.TypedActorContext[Any],
                signal: Signal,
                target: SignalTarget[InternalProtocol]): Behavior[InternalProtocol] = {
              if (signal == PostStop) {
                eventSourcedSetup.cancelRecoveryTimer()
                // clear stash to be GC friendly
                stashState.clearStashBuffers()
              }
              target(ctx, signal)
            }

            override def toString: String = "EventSourcedBehaviorInterceptor"
          }

          Behaviors.intercept(() => interceptor)(RequestingRecoveryPermit(eventSourcedSetup)).narrow
        }

      }
      .onFailure[JournalFailureException](supervisionStrategy)
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def initialize(context: ActorContext[_]): Unit = ()

  override def receiveSignal(
      handler: PartialFunction[(State, Signal), Unit]): EventSourcedBehavior[Command, Event, State] =
    copy(signalHandler = handler)

  override def withJournalPluginId(id: String): EventSourcedBehavior[Command, Event, State] = {
    require(id != null, "journal plugin id must not be null; use empty string for 'default' journal")
    copy(journalPluginId = if (id != "") Some(id) else None)
  }

  override def withSnapshotPluginId(id: String): EventSourcedBehavior[Command, Event, State] = {
    require(id != null, "snapshot plugin id must not be null; use empty string for 'default' snapshot store")
    copy(snapshotPluginId = if (id != "") Some(id) else None)
  }

  override def withSnapshotSelectionCriteria(
      selection: SnapshotSelectionCriteria): EventSourcedBehavior[Command, Event, State] = {
    copy(recovery = Recovery.withSnapshotSelectionCriteria(selection))
  }

  override def snapshotWhen(predicate: (State, Event, Long) => Boolean): EventSourcedBehavior[Command, Event, State] =
    snapshotWhen(predicate, deleteEventsOnSnapshot = false)

  override def snapshotWhen(
      predicate: (State, Event, Long) => Boolean,
      deleteEventsOnSnapshot: Boolean): EventSourcedBehavior[Command, Event, State] = {
    copy(snapshotWhen = SnapshotWhenPredicate(predicate, deleteEventsOnSnapshot))
  }

  override def withRetention(criteria: RetentionCriteria): EventSourcedBehavior[Command, Event, State] =
    copy(retention = criteria)

  override def withTagger(tagger: Event => Set[String]): EventSourcedBehavior[Command, Event, State] =
    copy(tagger = (_, event) => tagger(event))

  override def withTaggerForState(tagger: (State, Event) => Set[String]): EventSourcedBehavior[Command, Event, State] =
    copy(tagger = tagger)

  override def eventAdapter(adapter: EventAdapter[Event, _]): EventSourcedBehavior[Command, Event, State] =
    copy(eventAdapter = adapter.asInstanceOf[EventAdapter[Event, Any]])

  override def snapshotAdapter(adapter: SnapshotAdapter[State]): EventSourcedBehavior[Command, Event, State] =
    copy(snapshotAdapter = adapter)

  override def onPersistFailure(
      backoffStrategy: BackoffSupervisorStrategy): EventSourcedBehavior[Command, Event, State] =
    copy(supervisionStrategy = backoffStrategy)

  override def withRecovery(recovery: Recovery): EventSourcedBehavior[Command, Event, State] = {
    copy(recovery = recovery)
  }

  override def withEventPublishing(enabled: Boolean): EventSourcedBehavior[Command, Event, State] = {
    copy(publishEvents = enabled)
  }

  override private[akka] def withReplication(
      context: ReplicationContextImpl): EventSourcedBehavior[Command, Event, State] = {
    copy(
      replication = Some(ReplicationSetup(context.replicationId.replicaId, context.replicasAndQueryPlugins, context)))
  }

  override def withStashCapacity(size: Int): EventSourcedBehavior[Command, Event, State] =
    copy(customStashCapacity = Some(size))

  override def withReplicatedEventInterceptor(
      interceptor: ReplicationInterceptor[State, Event]): EventSourcedBehavior[Command, Event, State] =
    copy(replicatedEventInterceptor = Some(interceptor))

  override def withReplicatedEventTransformation(
      f: (State, EventWithMetadata[Event]) => EventWithMetadata[Event]): EventSourcedBehavior[Command, Event, State] =
    copy(replicatedEventTransformation = Some((s: State, e: EventWithMetadata[Event]) => f(s, e) :: Nil))

  override def withReplicatedEventsTransformation(f: (State, EventWithMetadata[Event]) => Seq[EventWithMetadata[Event]])
      : EventSourcedBehavior[Command, Event, State] =
    copy(replicatedEventTransformation = Some(f))

}

/** Protocol used internally by the eventsourced behaviors. */
@InternalApi private[akka] sealed trait InternalProtocol
@InternalApi private[akka] object InternalProtocol {
  case object RecoveryPermitGranted extends InternalProtocol
  final case class JournalResponse(msg: akka.persistence.JournalProtocol.Response) extends InternalProtocol
  final case class SnapshotterResponse(msg: akka.persistence.SnapshotProtocol.Response) extends InternalProtocol
  final case class RecoveryTickEvent(snapshot: Boolean) extends InternalProtocol
  final case class IncomingCommand[C](c: C) extends InternalProtocol
  final case class AsyncEffectCompleted[C, E, S](cmd: C, effect: Effect[E, S]) extends InternalProtocol
  final case class AsyncReplicationInterceptCompleted(nextBehaviorF: () => Behavior[InternalProtocol])
      extends InternalProtocol
  case object ContinueUnstash extends InternalProtocol

  final case class ReplicatedEventEnvelope[E](event: ReplicatedEvent[E], ack: ActorRef[ReplicatedEventAck.type])
      extends InternalProtocol

}

object ReplicatedEventMetadata {

  /**
   * For a journal supporting Replicated Event Sourcing needing to add test coverage, use this instance as metadata and defer
   * to the built in serializer for serialization format
   */
  def instanceForJournalTest: Any =
    ReplicatedEventMetadata(ReplicaId("DC-A"), 1L, VersionVector.empty + "DC-A", concurrent = true)
}

/**
 * INTERNAL API
 *
 * @param originReplica Where the event originally was created
 * @param originSequenceNr The original sequenceNr in the origin DC
 * @param version The version with which the event was persisted at the different DC. The same event will have different version vectors
 *                at each location as they are received at different times
 */
@InternalStableApi
final case class ReplicatedEventMetadata(
    originReplica: ReplicaId,
    originSequenceNr: Long,
    version: VersionVector,
    concurrent: Boolean) // whether when the event handler was executed the event was concurrent

object ReplicatedSnapshotMetadata {

  /**
   * For a snapshot store supporting Replicated Event Sourcing needing to add test coverage, use this instance as metadata and defer
   * to the built in serializer for serialization format
   */
  def instanceForSnapshotStoreTest: Any =
    ReplicatedSnapshotMetadata(
      VersionVector.empty + "DC-B" + "DC-A",
      Map(ReplicaId("DC-A") -> 1L, ReplicaId("DC-B") -> 1L))

}

@InternalApi
private[akka] final case class ReplicatedSnapshotMetadata(version: VersionVector, seenPerReplica: Map[ReplicaId, Long])

/**
 * An event replicated from a different replica.
 *
 * The version is for when it was persisted at the other replica. At the current replica it will be
 * merged with the current local version.
 */
@InternalApi
private[akka] final case class ReplicatedEvent[E](
    event: E,
    metadata: Option[Any],
    originReplica: ReplicaId,
    originSequenceNr: Long,
    originVersion: VersionVector)
@InternalApi
private[akka] case object ReplicatedEventAck

final class ReplicatedPublishedEventMetaData(
    val replicaId: ReplicaId,
    private[akka] val version: VersionVector,
    private[akka] val metadata: Option[Any]) {

  @deprecated("Use constructor with metadata parameter", "2.10.1")
  def this(replicaId: ReplicaId, version: VersionVector) = this(replicaId, version, None)

  override def equals(obj: Any): Boolean = obj match {
    case other: ReplicatedPublishedEventMetaData =>
      replicaId == other.replicaId &&
      version == other.version &&
      metadata == other.metadata
    case _ => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, replicaId)
    result = HashCode.hash(result, version)
    result = HashCode.hash(result, metadata)
    result
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] final case class PublishedEventImpl(
    persistenceId: PersistenceId,
    sequenceNumber: Long,
    payload: Any,
    timestamp: Long,
    replicatedMetaData: Option[ReplicatedPublishedEventMetaData],
    replyTo: Option[ActorRef[Done]])
    extends PublishedEvent
    with InternalProtocol {
  import scala.jdk.OptionConverters._

  def tags: Set[String] = payload match {
    case t: Tagged => t.tags
    case _         => Set.empty
  }

  def event: Any = payload match {
    case Tagged(event, _) => event
    case _                => payload
  }

  override def withoutTags: PublishedEvent = payload match {
    case Tagged(event, _) => copy(payload = event)
    case _                => this
  }

  def lossyTransport: Boolean = replyTo.isEmpty

  override def getReplicatedMetaData: Optional[ReplicatedPublishedEventMetaData] = replicatedMetaData.toJava
}
