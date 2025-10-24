/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.UnhandledMessage
import akka.actor.typed.{ Behavior, Signal }
import akka.actor.typed.ActorRef
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors }
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.event.Logging
import akka.persistence.CompositeMetadata
import akka.persistence.DeleteMessagesFailure
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.DeleteSnapshotFailure
import akka.persistence.DeleteSnapshotSuccess
import akka.persistence.DeleteSnapshotsFailure
import akka.persistence.DeleteSnapshotsSuccess
import akka.persistence.JournalProtocol
import akka.persistence.JournalProtocol._
import akka.persistence.PersistentRepr
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotProtocol
import akka.persistence.journal.Tagged
import akka.persistence.query.{ EventEnvelope, PersistenceQuery }
import akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
import akka.persistence.typed.PersistFailed
import akka.persistence.typed.PersistRejected
import akka.persistence.typed.{
  DeleteEventsCompleted,
  DeleteEventsFailed,
  DeleteSnapshotsCompleted,
  DeleteSnapshotsFailed,
  DeletionTarget,
  EventRejectedException,
  PersistenceId,
  SnapshotCompleted,
  SnapshotFailed,
  SnapshotMetadata,
  SnapshotSelectionCriteria
}
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetVersion
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.WithMetadataAccessible
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.{ GetSeenSequenceNr, GetState, GetStateReply }
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.WithSeqNrAccessible
import akka.persistence.typed.internal.InternalProtocol.ReplicatedEventEnvelope
import akka.persistence.typed.internal.JournalInteractions.EventToPersist
import akka.persistence.typed.internal.Running.MaxRecursiveUnstash
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventWithMetadata
import akka.persistence.typed.telemetry.EventSourcedBehaviorInstrumentation
import akka.stream.{ RestartSettings, SystemMaterializer, WatchedActorTerminatedException }
import akka.stream.scaladsl.{ RestartSource, Sink }
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.OptionVal
import akka.util.Timeout

/**
 * INTERNAL API
 *
 * Conceptually fourth (of four) -- also known as 'final' or 'ultimate' -- form of EventSourcedBehavior.
 *
 * In this phase recovery has completed successfully and we continue handling incoming commands,
 * as well as persisting new events as dictated by the user handlers.
 *
 * This behavior operates in three phases (also behaviors):
 * - HandlingCommands - where the command handler is invoked for incoming commands
 * - PersistingEvents - where incoming commands are stashed until persistence completes
 * - storingSnapshot - where incoming commands are stashed until snapshot storage completes
 *
 * This is implemented as such to avoid creating many EventSourced Running instances,
 * which perform the Persistence extension lookup on creation and similar things (config lookup)
 *
 * See previous [[ReplayingEvents]].
 */
@InternalApi
private[akka] object Running {

  private val MaxRecursiveUnstash = 100

  final case class RunningState[State](
      seqNr: Long,
      state: State,
      receivedPoisonPill: Boolean,
      version: VersionVector,
      seenPerReplica: Map[ReplicaId, Long],
      replicationControl: Map[ReplicaId, ReplicationStreamControl],
      instrumentationContexts: Map[Long, EventSourcedBehaviorInstrumentation.Context]) {

    def nextSequenceNr(): RunningState[State] =
      copy(seqNr = seqNr + 1)

    def updateLastSequenceNr(persistent: PersistentRepr): RunningState[State] =
      if (persistent.sequenceNr > seqNr) copy(seqNr = persistent.sequenceNr) else this

    def applyEvent[C, E](setup: BehaviorSetup[C, E, State], event: E): RunningState[State] = {
      val updated = setup.eventHandler(state, event)
      copy(state = updated)
    }

    def getInstrumentationContext(seqNr: Long): EventSourcedBehaviorInstrumentation.Context =
      instrumentationContexts.get(seqNr) match {
        case Some(ctx) => ctx
        case None      => EventSourcedBehaviorInstrumentation.EmptyContext
      }

    def updateInstrumentationContext(
        seqNr: Long,
        instrumentationContext: EventSourcedBehaviorInstrumentation.Context): RunningState[State] = {
      if (instrumentationContext eq EventSourcedBehaviorInstrumentation.EmptyContext)
        this // avoid instance creation for EmptyContext
      else copy(instrumentationContexts = instrumentationContexts.updated(seqNr, instrumentationContext))
    }

    def clearInstrumentationContext: RunningState[State] =
      if (instrumentationContexts.isEmpty) this
      else copy(instrumentationContexts = Map.empty)
  }

  def startReplicationStream[C, E, S](
      setup: BehaviorSetup[C, E, S],
      state: RunningState[S],
      replicationSetup: ReplicationSetup): RunningState[S] = {
    import scala.concurrent.duration._
    val system = setup.context.system
    val ref = setup.context.self

    val query = PersistenceQuery(system)
    replicationSetup.allReplicas.foldLeft(state) { (state, replicaId) =>
      if (replicaId != replicationSetup.replicaId) {
        val pid = ReplicationId(
          replicationSetup.replicationContext.replicationId.typeName,
          replicationSetup.replicationContext.entityId,
          replicaId)
        val queryPluginId = replicationSetup.allReplicasAndQueryPlugins(replicaId)
        val replication = query.readJournalFor[EventsByPersistenceIdQuery](queryPluginId)

        implicit val timeout: Timeout = 30.seconds
        implicit val scheduler = setup.context.system.scheduler
        implicit val ec = setup.context.system.executionContext

        val controlRef = new AtomicReference[ReplicationStreamControl]()

        import akka.actor.typed.scaladsl.AskPattern._
        val source = RestartSource
          .withBackoff(RestartSettings(2.seconds, 10.seconds, randomFactor = 0.2)) { () =>
            Source.futureSource {
              setup.context.self.ask[Long](replyTo => GetSeenSequenceNr(replicaId, replyTo)).map { seqNr =>
                replication
                  .eventsByPersistenceId(pid.persistenceId.id, seqNr + 1, Long.MaxValue)
                  .mapConcat(eventEnvelope =>
                    eventEnvelope.metadata[ReplicatedEventMetadata] match {
                      case Some(replicatedMeta) =>
                        // skip events originating from self replica (break the cycle)
                        if (replicatedMeta.originReplica == replicationSetup.replicaId)
                          Nil
                        else
                          eventEnvelope :: Nil
                      case _ =>
                        // migrated from non-replicated, fill in metadata
                        val metadata = ReplicatedEventMetadata(
                          originReplica = replicaId,
                          originSequenceNr = eventEnvelope.sequenceNr,
                          version = VersionVector(replicaId.id, eventEnvelope.sequenceNr),
                          concurrent = false)
                        eventEnvelope.withMetadata(metadata) :: Nil
                    })
                  .viaMat(new FastForwardingFilter)(Keep.right)
                  .mapMaterializedValue(streamControl => controlRef.set(streamControl))
              }
            }
          }
          // needs to be outside of the restart source so that it actually cancels when terminating the replica
          .via(ActorFlow
            .ask[EventEnvelope, ReplicatedEventEnvelope[E], ReplicatedEventAck.type](ref) { (eventEnvelope, replyTo) =>
              val replicatedEventMetadata = eventEnvelope.metadata[internal.ReplicatedEventMetadata].get

              val re =
                ReplicatedEvent[E](
                  eventEnvelope.event.asInstanceOf[E],
                  eventEnvelope.internalEventMetadata,
                  replicatedEventMetadata.originReplica,
                  replicatedEventMetadata.originSequenceNr,
                  replicatedEventMetadata.version)
              ReplicatedEventEnvelope(re, replyTo)
            }
            .recoverWithRetries(1, {
              // not a failure, the replica is stopping, complete the stream
              case _: WatchedActorTerminatedException =>
                Source.empty
            }))

        source.runWith(Sink.ignore)(SystemMaterializer(system).materializer)

        // TODO support from journal to fast forward https://github.com/akka/akka-core/issues/29311
        state.copy(
          replicationControl =
            state.replicationControl.updated(replicaId, new ReplicationStreamControl {
              override def fastForward(sequenceNumber: Long): Unit = {
                // (logging is safe here since invoked on message receive
                OptionVal(controlRef.get) match {
                  case OptionVal.Some(control) =>
                    if (setup.internalLogger.isDebugEnabled)
                      setup.internalLogger.debug("Fast forward replica [{}] to [{}]", replicaId, sequenceNumber)
                    control.fastForward(sequenceNumber)
                  case _ =>
                    // stream not started yet, ok, fast forward is an optimization
                    if (setup.internalLogger.isDebugEnabled)
                      setup.internalLogger.debug(
                        "Ignoring fast forward replica [{}] to [{}], stream not started yet",
                        replicaId,
                        sequenceNumber)
                }
              }
            }))
      } else {
        state
      }
    }
  }

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val UTC = ZoneId.of("UTC")

  def formatTimestamp(time: Long): String = {
    timestampFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), UTC))
  }
}

// ===============================================

/** INTERNAL API */
@InternalApi private[akka] final class Running[C, E, S](override val setup: BehaviorSetup[C, E, S])
    extends JournalInteractions[C, E, S]
    with SnapshotInteractions[C, E, S]
    with StashManagement[C, E, S] {
  import BehaviorSetup._
  import InternalProtocol._
  import Running.RunningState
  import Running.formatTimestamp

  private var recursiveUnstashOne = 0

  final class HandlingCommands(state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible
      with WithMetadataAccessible {

    setup.currentSequenceNumber = state.seqNr
    setup.currentMetadata = None

    private def alreadySeen(e: ReplicatedEvent[_]): Boolean = {
      e.originSequenceNr <= state.seenPerReplica.getOrElse(e.originReplica, 0L)
    }

    def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = msg match {
      case IncomingCommand(c: C @unchecked)          => onCommand(state, c)
      case re: ReplicatedEventEnvelope[E @unchecked] => onReplicatedEvent(state, re, setup.replication.get)
      case pe: PublishedEventImpl                    => onPublishedEvent(state, pe)
      case JournalResponse(r)                        => onDeleteEventsJournalResponse(r, state.state)
      case SnapshotterResponse(r)                    => onDeleteSnapshotResponse(r, state.state)
      case get: GetState[S @unchecked]               => onGetState(get)
      case get: GetSeenSequenceNr                    => onGetSeenSequenceNr(get)
      case get: GetVersion                           => onGetVersion(get)
      case _                                         => Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        if (isInternalStashEmpty && !isUnstashAllInProgress) Behaviors.stopped
        else new HandlingCommands(state.copy(receivedPoisonPill = true))
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false)) this
        else Behaviors.unhandled
    }

    def onCommand(state: RunningState[S], cmd: C): Behavior[InternalProtocol] = {
      val effect = setup.commandHandler(state.state, cmd)
      runEffect(cmd, effect)
    }

    def runEffect(cmd: C, effect: Effect[E, S]): Behavior[InternalProtocol] = {
      val (next, doUnstash) = applyEffects(cmd, state, effect.asInstanceOf[EffectImpl[E, S]])
      if (doUnstash) tryUnstashOne(next)
      else next

    }

    def onReplicatedEvent(
        state: Running.RunningState[S],
        envelope: ReplicatedEventEnvelope[E],
        replication: ReplicationSetup): Behavior[InternalProtocol] = {
      setup.internalLogger.debug(
        "Replica [{}] received replicated event from [{}], origin seq nr [{}]. Replica seq nrs: {}.",
        replication.replicaId,
        envelope.event.originReplica,
        envelope.event.originSequenceNr,
        state.seenPerReplica)
      envelope.ack ! ReplicatedEventAck
      if (envelope.event.originReplica != replication.replicaId && !alreadySeen(envelope.event)) {
        setup.replicationInterceptor match {
          case Some(interceptor) =>
            val asyncInterceptResult =
              interceptor.intercept(state.state, envelope.event.event, envelope.event.originReplica, state.seqNr + 1)
            asyncInterceptResult.value match {
              case Some(Success(_)) =>
                // optimization for quick successful interceptors
                handleExternalReplicatedEventPersist(replication, envelope.event, None)
              case _ =>
                // failed or not ready
                waitAsyncReplicationIntercept(
                  state,
                  asyncInterceptResult,
                  () => handleExternalReplicatedEventPersist(replication, envelope.event, None))
            }

          case None =>
            setup.internalLogger.debug(
              "Saving event [{}] from [{}] as first time",
              envelope.event.originSequenceNr,
              envelope.event.originReplica)
            handleExternalReplicatedEventPersist(replication, envelope.event, None)
        }
      } else {
        setup.internalLogger.debug(
          "Filtering event [{}] from [{}] as it was already seen",
          envelope.event.originSequenceNr,
          envelope.event.originReplica)
        tryUnstashOne(this)
      }
    }

    def onPublishedEvent(state: Running.RunningState[S], event: PublishedEventImpl): Behavior[InternalProtocol] = {
      var asyncWait: Boolean = false
      val newBehavior: Behavior[InternalProtocol] = setup.replication match {
        case None =>
          setup.internalLogger.warn(
            "Received published event for [{}] but not an Replicated Event Sourcing actor, dropping",
            event.persistenceId)
          this

        case Some(replication) =>
          event.replicatedMetaData match {
            case None =>
              setup.internalLogger.warn(
                "Received published event for [{}] but with no replicated metadata, dropping",
                event.persistenceId)
              this
            case Some(replicatedEventMetaData) =>
              setup.replicationInterceptor match {
                case Some(interceptor) =>
                  val asyncInterceptResult =
                    interceptor.intercept(
                      state.state,
                      event.event.asInstanceOf[E],
                      replicatedEventMetaData.replicaId,
                      state.seqNr + 1)
                  asyncInterceptResult.value match {
                    case Some(Success(_)) =>
                      // optimization for quick successful interceptors
                      onPublishedEvent(state, replication, replicatedEventMetaData, event)
                    case _ =>
                      // failed or not ready
                      asyncWait = true
                      waitAsyncReplicationIntercept(
                        state,
                        asyncInterceptResult,
                        () => onPublishedEvent(state, replication, replicatedEventMetaData, event))

                  }

                case None =>
                  onPublishedEvent(state, replication, replicatedEventMetaData, event)
              }

          }
      }
      if (!asyncWait) tryUnstashOne(newBehavior)
      else newBehavior
    }

    private def onPublishedEvent(
        state: Running.RunningState[S],
        replication: ReplicationSetup,
        replicatedMetadata: ReplicatedPublishedEventMetaData,
        event: PublishedEventImpl): Behavior[InternalProtocol] = {
      val log = setup.internalLogger
      val separatorIndex = event.persistenceId.id.indexOf(PersistenceId.DefaultSeparator)
      val idPrefix = event.persistenceId.id.substring(0, separatorIndex)
      val originReplicaId = replicatedMetadata.replicaId
      if (!setup.persistenceId.id.startsWith(idPrefix)) {
        log.warn("Ignoring published replicated event for the wrong actor [{}]", event.persistenceId)
        this
      } else if (originReplicaId == replication.replicaId) {
        if (log.isDebugEnabled)
          log.debug(
            "Ignoring published replicated event with seqNr [{}] from our own replica id [{}]",
            event.sequenceNumber,
            originReplicaId)
        event.replyTo.foreach(_ ! Done) // probably won't happen
        this
      } else {
        val seenSequenceNr = state.seenPerReplica.getOrElse(originReplicaId, 0L)
        if (seenSequenceNr >= event.sequenceNumber) {
          // already seen/deduplication
          if (log.isDebugEnabled)
            log.debug(
              "Ignoring published replicated event with seqNr [{}] from replica [{}] because it was already seen (version: {})",
              event.sequenceNumber,
              originReplicaId,
              state.seenPerReplica)
          event.replyTo.foreach(_ ! Done)
          this
        } else if (event.lossyTransport && event.sequenceNumber != (seenSequenceNr + 1)) {
          // Lossy transport/opportunistic replication cannot allow gaps in sequence
          // numbers (message lost or query and direct replication out of sync, should heal up by itself
          // once the query catches up)
          if (log.isDebugEnabled) {
            log.debug(
              "Ignoring published replicated event with replication seqNr [{}] from replica [{}] " +
              "because expected replication seqNr was [{}] ",
              event.sequenceNumber,
              originReplicaId,
              seenSequenceNr + 1)
          }
          this
        } else {
          if (log.isTraceEnabled) {
            log.trace(
              "Received published replicated event [{}] with timestamp [{} (UTC)] from replica [{}] seqNr [{}]",
              Logging.simpleName(event.event.getClass),
              formatTimestamp(event.timestamp),
              originReplicaId,
              event.sequenceNumber)
          }

          // fast-forward stream for source replica
          state.replicationControl.get(originReplicaId).foreach(_.fastForward(event.sequenceNumber))

          handleExternalReplicatedEventPersist(
            replication,
            ReplicatedEvent(
              event.event.asInstanceOf[E],
              replicatedMetadata.metadata,
              originReplicaId,
              event.sequenceNumber,
              replicatedMetadata.version),
            event.replyTo)
        }

      }
    }

    // Used by EventSourcedBehaviorTestKit to retrieve the state.
    def onGetState(get: GetState[S]): Behavior[InternalProtocol] = {
      get.replyTo ! GetStateReply(state.state)
      tryUnstashOne(this)
    }

    def onGetSeenSequenceNr(get: GetSeenSequenceNr): Behavior[InternalProtocol] = {
      get.replyTo ! state.seenPerReplica.getOrElse(get.replica, 0L)
      this
    }

    def onGetVersion(get: GetVersion): Behavior[InternalProtocol] = {
      get.replyTo ! state.version
      this
    }

    private def handleExternalReplicatedEventPersist(
        replication: ReplicationSetup,
        event: ReplicatedEvent[E],
        ackToOnPersisted: Option[ActorRef[Done]]): Behavior[InternalProtocol] = {
      setup.currentSequenceNumber = state.seqNr + 1
      setup.currentMetadata = event.metadata
      val isConcurrent: Boolean = event.originVersion <> state.version
      val updatedVersion = event.originVersion.merge(state.version)

      if (setup.internalLogger.isDebugEnabled())
        setup.internalLogger.debug(
          "Processing event [{}] with version [{}]. Local version: {}. Updated version {}. Concurrent? {}",
          Logging.simpleName(event.event.getClass),
          event.originVersion,
          state.version,
          updatedVersion,
          isConcurrent)

      replication.setContext(recoveryRunning = false, event.originReplica, concurrent = isConcurrent)

      val (transformedEvent, additionalMetadata, additionalEvents) = setup.replicatedEventTransformation match {
        case None =>
          (event.event, Nil, Nil)
        case Some(f) =>
          val metadataEntries = event.metadata match {
            case None                             => Nil
            case Some(CompositeMetadata(entries)) => entries
            case Some(meta)                       => meta :: Nil
          }
          val eventWithMetadata = EventWithMetadata(event.event, metadataEntries)
          val newEventsWithMetadata = f(state.state, eventWithMetadata)
          if (newEventsWithMetadata.isEmpty)
            throw new IllegalStateException("At least one event is required in replicatedEventTransformation")
          (newEventsWithMetadata.head.event, newEventsWithMetadata.head.metadataEntries, newEventsWithMetadata.tail)
      }

      val replicatedEventMetadata =
        ReplicatedEventMetadata(event.originReplica, event.originSequenceNr, updatedVersion, isConcurrent)
      val metadataEntriesFromReplicatedEvent =
        event.metadata match {
          case None                             => Nil
          case Some(_: ReplicatedEventMetadata) => Nil
          case Some(CompositeMetadata(entries)) => entries.filterNot(_.isInstanceOf[ReplicatedEventMetadata])
          case Some(meta)                       => meta :: Nil
        }

      val newMetadataEntries = replicatedEventMetadata +: additionalMetadata ++: metadataEntriesFromReplicatedEvent
      val newMetadata = CompositeMetadata.construct(newMetadataEntries)
      setup.currentMetadata = newMetadata // make new metadata visible to event handler

      val stateAfterApply = state.applyEvent(setup, transformedEvent)
      val adaptedEvent = adaptEvent(stateAfterApply.state, transformedEvent)
      val eventAdapterManifest = setup.eventAdapter.manifest(transformedEvent)

      val updatedSeen = stateAfterApply.seenPerReplica.updated(event.originReplica, event.originSequenceNr)

      val sideEffects = ackToOnPersisted match {
        case None => Nil
        case Some(ref) =>
          SideEffect { (_: S) =>
            ref ! Done
          } :: Nil
      }

      if (additionalEvents.isEmpty) {
        replication.clearContext()

        val newState2: RunningState[S] =
          internalPersist(OptionVal.none, stateAfterApply, adaptedEvent, eventAdapterManifest, newMetadata)

        val shouldSnapshotAfterPersist = setup.shouldSnapshot(newState2.state, transformedEvent, newState2.seqNr)

        // one event, this is the normal case
        persistingEvents(
          newState2.copy(seenPerReplica = updatedSeen, version = updatedVersion),
          state,
          command = OptionVal.none[C],
          numberOfEvents = 1,
          shouldSnapshotAfterPersist,
          shouldPublish = false,
          sideEffects)
      } else {
        // additional events, this is similar to the persistAll case

        replication.setContext(recoveryRunning = false, replication.replicaId, concurrent = false) // local events are never concurrent
        val replicatedEventMetadataTemplate = ReplicatedEventMetadata(
          replication.replicaId,
          0L,
          state.version,
          concurrent = false) // we replace it with actual seqnr later

        var currentState = stateAfterApply
        var eventsToPersist
            : List[EventToPersist] = EventToPersist(adaptedEvent, eventAdapterManifest, newMetadata) :: Nil
        var shouldSnapshotAfterPersist =
          setup.shouldSnapshot(stateAfterApply.state, transformedEvent, stateAfterApply.seqNr)

        additionalEvents.foreach { evtWithMeta =>
          val event = evtWithMeta.event
          setup.currentSequenceNumber += 1
          setup.currentMetadata = CompositeMetadata.construct(evtWithMeta.metadataEntries)
          val evtManifest = setup.eventAdapter.manifest(event)
          val metadataEntries = {
            val updatedVersion =
              currentState.version
                .updated(replicatedEventMetadataTemplate.originReplica.id, setup.currentSequenceNumber)
            if (setup.internalLogger.isDebugEnabled)
              setup.internalLogger.trace(
                "Additional event [{}] from replicated event , version vector [{}]",
                Logging.simpleName(event.getClass),
                updatedVersion)
            currentState = currentState.copy(version = updatedVersion)
            replicatedEventMetadataTemplate.copy(
              originSequenceNr = setup.currentSequenceNumber,
              version = updatedVersion) +: evtWithMeta.metadataEntries
          }
          val metadata = CompositeMetadata.construct(metadataEntries)

          currentState = currentState.applyEvent(setup, event)
          if (shouldSnapshotAfterPersist == NoSnapshot)
            shouldSnapshotAfterPersist = setup.shouldSnapshot(currentState.state, event, setup.currentSequenceNumber)

          val adaptedEvent = adaptEvent(currentState.state, event)

          eventsToPersist = EventToPersist(adaptedEvent, evtManifest, metadata) :: eventsToPersist
        }

        replication.clearContext()

        val newState2 =
          internalPersistAll(OptionVal.none, currentState, eventsToPersist.reverse)

        persistingEvents(
          newState2.copy(seenPerReplica = updatedSeen),
          state,
          command = OptionVal.none[C],
          eventsToPersist.size,
          shouldSnapshotAfterPersist,
          shouldPublish = true,
          sideEffects = sideEffects)

      }

    }

    private def handleEventPersist(
        event: E,
        cmd: C,
        metadataEntries: Seq[Any],
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      try {
        // apply the event before persist so that validation exception is handled before persisting
        // the invalid event, in case such validation is implemented in the event handler.
        // also, ensure that there is an event handler for each single event
        setup.currentSequenceNumber = state.seqNr + 1
        val metadata = CompositeMetadata.construct(metadataEntries)
        setup.currentMetadata = metadata

        setup.replication.foreach(r => r.setContext(recoveryRunning = false, r.replicaId, concurrent = false))

        val stateAfterApply = state.applyEvent(setup, event)
        val eventToPersist = adaptEvent(stateAfterApply.state, event)
        val eventAdapterManifest = setup.eventAdapter.manifest(event)

        val newState2 = setup.replication match {
          case Some(replication) =>
            val updatedVersion = stateAfterApply.version.updated(replication.replicaId.id, setup.currentSequenceNumber)
            val replicatedEventMetadata =
              ReplicatedEventMetadata(
                replication.replicaId,
                setup.currentSequenceNumber,
                updatedVersion,
                concurrent = false)
            val metadata2 = CompositeMetadata.construct(replicatedEventMetadata +: metadataEntries)
            val r =
              internalPersist(OptionVal.Some(cmd), stateAfterApply, eventToPersist, eventAdapterManifest, metadata2)
                .copy(version = updatedVersion)

            if (setup.internalLogger.isTraceEnabled())
              setup.internalLogger.trace(
                "Event persisted [{}]. Version vector after: [{}]",
                Logging.simpleName(event.getClass),
                r.version)

            r
          case None =>
            internalPersist(OptionVal.Some(cmd), stateAfterApply, eventToPersist, eventAdapterManifest, metadata)
        }

        val shouldSnapshotAfterPersist = setup.shouldSnapshot(newState2.state, event, newState2.seqNr)
        (
          persistingEvents(
            newState2,
            state,
            command = OptionVal.Some(cmd),
            numberOfEvents = 1,
            shouldSnapshotAfterPersist,
            shouldPublish = true,
            sideEffects),
          false)
      } finally {
        setup.replication.foreach(_.clearContext())
      }
    }

    private def handleEventPersistAll(
        eventsWithMetadata: immutable.Seq[EventWithMetadata[E]],
        cmd: C,
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      if (eventsWithMetadata.nonEmpty) {
        try {
          // apply the event before persist so that validation exception is handled before persisting
          // the invalid event, in case such validation is implemented in the event handler.
          // also, ensure that there is an event handler for each single event
          setup.currentSequenceNumber = state.seqNr

          val replicatedEventMetadataTemplate: Option[ReplicatedEventMetadata] = setup.replication match {
            case Some(replication) =>
              replication.setContext(recoveryRunning = false, replication.replicaId, concurrent = false) // local events are never concurrent
              Some(ReplicatedEventMetadata(replication.replicaId, 0L, state.version, concurrent = false)) // we replace it with actual seqnr later
            case None => None
          }

          var currentState = state
          var shouldSnapshotAfterPersist: SnapshotAfterPersist = NoSnapshot
          var eventsToPersist: List[EventToPersist] = Nil

          eventsWithMetadata.foreach { evtWithMeta =>
            val event = evtWithMeta.event
            setup.currentSequenceNumber += 1
            setup.currentMetadata = CompositeMetadata.construct(evtWithMeta.metadataEntries)
            val evtManifest = setup.eventAdapter.manifest(event)
            val metadataEntries = replicatedEventMetadataTemplate match {
              case Some(template) =>
                val updatedVersion =
                  currentState.version.updated(template.originReplica.id, setup.currentSequenceNumber)
                if (setup.internalLogger.isDebugEnabled)
                  setup.internalLogger.trace(
                    "Processing event [{}] with version vector [{}]",
                    Logging.simpleName(event.getClass),
                    updatedVersion)
                currentState = currentState.copy(version = updatedVersion)
                template.copy(originSequenceNr = setup.currentSequenceNumber, version = updatedVersion) +: evtWithMeta.metadataEntries
              case None => evtWithMeta.metadataEntries
            }
            val metadata = CompositeMetadata.construct(metadataEntries)

            currentState = currentState.applyEvent(setup, event)
            if (shouldSnapshotAfterPersist == NoSnapshot)
              shouldSnapshotAfterPersist = setup.shouldSnapshot(currentState.state, event, setup.currentSequenceNumber)

            val adaptedEvent = adaptEvent(currentState.state, event)

            eventsToPersist = EventToPersist(adaptedEvent, evtManifest, metadata) :: eventsToPersist
          }

          val newState2 =
            internalPersistAll(OptionVal.Some(cmd), currentState, eventsToPersist.reverse)

          (
            persistingEvents(
              newState2,
              state,
              command = OptionVal.Some(cmd),
              eventsWithMetadata.size,
              shouldSnapshotAfterPersist,
              shouldPublish = true,
              sideEffects = sideEffects),
            false)
        } finally {
          setup.replication.foreach(_.clearContext())
        }
      } else {
        // run side-effects even when no events are emitted
        setup.currentMetadata = None
        (applySideEffects(sideEffects, state), true)
      }
    }
    @tailrec def applyEffects(
        msg: C,
        state: RunningState[S],
        effect: Effect[E, S],
        sideEffects: immutable.Seq[SideEffect[S]] = Nil): (Behavior[InternalProtocol], Boolean) = {
      if (setup.internalLogger.isDebugEnabled && !effect.isInstanceOf[CompositeEffect[_, _]])
        setup.internalLogger.debug(
          s"Handled command [{}], resulting effect: [{}], side effects: [{}]",
          msg.getClass.getName,
          effect,
          sideEffects.size)

      effect match {
        case CompositeEffect(eff, currentSideEffects) =>
          // unwrap and accumulate effects
          applyEffects(msg, state, eff, currentSideEffects ++ sideEffects)

        case Persist(event, metadata) =>
          handleEventPersist(event, msg, metadata, sideEffects)

        case persistAll: PersistAll[E, S] @unchecked =>
          handleEventPersistAll(persistAll.eventsWithMetadata, msg, sideEffects)

        case _: PersistNothing.type =>
          (applySideEffects(sideEffects, state), true)

        case AsyncEffect(laterEffect) =>
          (waitAsyncEffect(msg, state, laterEffect), false)

        case _: Unhandled.type =>
          import akka.actor.typed.scaladsl.adapter._
          setup.context.system.toClassic.eventStream
            .publish(UnhandledMessage(msg, setup.context.system.toClassic.deadLetters, setup.context.self.toClassic))
          (applySideEffects(sideEffects, state), true)

        case _: Stash.type =>
          stashUser(IncomingCommand(msg))
          (applySideEffects(sideEffects, state), true)

        case unexpected => throw new IllegalStateException(s"Unexpected retention effect: $unexpected")
      }
    }

    def adaptEvent(state: S, event: E): Any = {
      val tags = setup.tagger(state, event)
      val adaptedEvent = setup.eventAdapter.toJournal(event)
      if (tags.isEmpty)
        adaptedEvent
      else
        Tagged(adaptedEvent, tags)
    }

    // note that this shadows tryUnstashOne in StashManagement from HandlingCommands
    private def tryUnstashOne(behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
      if (isStashEmpty) {
        recursiveUnstashOne = 0
        behavior
      } else {
        recursiveUnstashOne += 1
        if (recursiveUnstashOne >= MaxRecursiveUnstash && behavior.isInstanceOf[HandlingCommands]) {
          // avoid StackOverflow from too many recursive tryUnstashOne (stashed read only commands)
          recursiveUnstashOne = 0
          setup.context.self ! ContinueUnstash
          new WaitingForContinueUnstash(state)
        } else
          Running.this.tryUnstashOne(behavior)
      }
    }

    setup.setMdcPhase(PersistenceMdc.RunningCmds)

    // WithSeqNrAccessible
    override def currentSequenceNumber: Long =
      setup.currentSequenceNumber

    // WithMetadataAccessible
    override def metadata[M: ClassTag]: Option[M] =
      CompositeMetadata.extract[M](setup.currentMetadata)

  }

  // ===============================================

  def persistingEvents(
      state: RunningState[S],
      visibleState: RunningState[S], // previous state until write success
      command: OptionVal[C],
      numberOfEvents: Int,
      shouldSnapshotAfterPersist: SnapshotAfterPersist,
      shouldPublish: Boolean,
      sideEffects: immutable.Seq[SideEffect[S]]): Behavior[InternalProtocol] = {
    setup.setMdcPhase(PersistenceMdc.PersistingEvents)
    recursiveUnstashOne = 0
    new PersistingEvents(
      state,
      visibleState,
      command,
      numberOfEvents,
      shouldSnapshotAfterPersist,
      shouldPublish,
      sideEffects)
  }

  /** INTERNAL API */
  @InternalApi private[akka] class PersistingEvents(
      var state: RunningState[S],
      var visibleState: RunningState[S], // previous state until write success
      command: OptionVal[C],
      numberOfEvents: Int,
      shouldSnapshotAfterPersist: SnapshotAfterPersist,
      shouldPublish: Boolean,
      var sideEffects: immutable.Seq[SideEffect[S]],
      persistStartTime: Long = System.nanoTime())
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible
      with WithMetadataAccessible {

    private var eventCounter = 0

    override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
      msg match {
        case JournalResponse(r)                        => onJournalResponse(r)
        case in: IncomingCommand[C @unchecked]         => onCommand(in)
        case re: ReplicatedEventEnvelope[E @unchecked] => onReplicatedEvent(re)
        case pe: PublishedEventImpl                    => onPublishedEvent(pe)
        case get: GetState[S @unchecked]               => stashInternal(get)
        case getSeqNr: GetSeenSequenceNr               => onGetSeenSequenceNr(getSeqNr)
        case getVersion: GetVersion                    => onGetVersion(getVersion)
        case SnapshotterResponse(r)                    => onDeleteSnapshotResponse(r, visibleState.state)
        case RecoveryTickEvent(_)                      => Behaviors.unhandled
        case RecoveryPermitGranted                     => Behaviors.unhandled
        case ContinueUnstash                           => Behaviors.unhandled
        case _: AsyncEffectCompleted[_, _, _]          => Behaviors.unhandled
        case _: AsyncReplicationInterceptCompleted     => Behaviors.unhandled
      }
    }

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    def onGetSeenSequenceNr(get: GetSeenSequenceNr): PersistingEvents = {
      get.replyTo ! state.seenPerReplica.getOrElse(get.replica, 0L)
      this
    }

    def onGetVersion(get: GetVersion): Behavior[InternalProtocol] = {
      get.replyTo ! state.version
      this
    }

    def onReplicatedEvent(event: InternalProtocol.ReplicatedEventEnvelope[E]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        Behaviors.unhandled
      } else {
        stashInternal(event)
      }
    }

    def onPublishedEvent(event: PublishedEventImpl): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        Behaviors.unhandled
      } else {
        stashInternal(event)
      }
    }

    final def onJournalResponse(response: Response): Behavior[InternalProtocol] = {
      if (setup.internalLogger.isDebugEnabled) {
        setup.internalLogger.debug(
          "Received Journal response: {} after: {} nanos",
          response,
          System.nanoTime() - persistStartTime)
      }

      def onWriteResponse(p: PersistentRepr): Behavior[InternalProtocol] = {
        state = state.updateLastSequenceNr(p)
        eventCounter += 1

        val instrumentationContext2 =
          setup.instrumentation.persistEventWritten(
            setup.context.self,
            p.payload,
            state.getInstrumentationContext(p.sequenceNr))
        val state2 = state.updateInstrumentationContext(p.sequenceNr, instrumentationContext2)
        onWriteSuccess(setup.context, p)

        if (setup.publishEvents && shouldPublish) {
          val meta = setup.replication.map(replication =>
            new ReplicatedPublishedEventMetaData(replication.replicaId, state.version, p.metadata))
          context.system.eventStream ! EventStream.Publish(
            PublishedEventImpl(setup.persistenceId, p.sequenceNr, p.payload, p.timestamp, meta, None))
        }

        // only once all things are applied we can revert back
        if (eventCounter < numberOfEvents) {
          setup.instrumentation.persistEventDone(setup.context.self, instrumentationContext2)
          onWriteDone(setup.context, p)
          this
        } else {
          visibleState = state2
          def skipRetention(): Boolean = {
            // only one retention process at a time
            val inProgress = shouldSnapshotAfterPersist == SnapshotWithRetention && setup.isRetentionInProgress()
            if (inProgress)
              setup.internalLogger.info(
                "Skipping retention at seqNr [{}] because previous retention has not completed yet. " +
                "Next retention will cover skipped retention.",
                state2.seqNr)
            inProgress
          }

          if (shouldSnapshotAfterPersist == NoSnapshot || state2.state == null || skipRetention()) {
            val behavior = applySideEffects(sideEffects, state2.clearInstrumentationContext)
            setup.instrumentation.persistEventDone(setup.context.self, instrumentationContext2)
            onWriteDone(setup.context, p)
            tryUnstashOne(behavior)
          } else {
            setup.instrumentation.persistEventDone(setup.context.self, instrumentationContext2)
            onWriteDone(setup.context, p)
            if (shouldSnapshotAfterPersist == SnapshotWithRetention)
              setup.retentionProgressSaveSnapshotStarted(state2.seqNr)
            internalSaveSnapshot(state2, setup.currentMetadata)
            new StoringSnapshot(state2.clearInstrumentationContext, sideEffects, shouldSnapshotAfterPersist)
          }
        }
      }

      response match {
        case WriteMessageSuccess(p, id) =>
          if (id == setup.writerIdentity.instanceId)
            onWriteResponse(p)
          else this

        case WriteMessageRejected(p, cause, id) =>
          if (id == setup.writerIdentity.instanceId) {
            setup.instrumentation.persistRejected(
              setup.context.self,
              cause,
              p.payload,
              p.sequenceNr,
              state.getInstrumentationContext(p.sequenceNr))
            onWriteRejected(setup.context, cause, p)
            val signal = PersistRejected(cause, command.toOption)
            if (setup.onSignal(state.state, signal, catchAndLog = false)) {
              setup.internalLogger.debug("Emitted signal [{}].", signal)
            }
            throw new EventRejectedException(setup.persistenceId, p.sequenceNr, cause)
          } else this

        case WriteMessageFailure(p, cause, id) =>
          if (id == setup.writerIdentity.instanceId) {
            setup.instrumentation.persistFailed(
              setup.context.self,
              cause,
              p.payload,
              p.sequenceNr,
              state.getInstrumentationContext(p.sequenceNr))
            onWriteFailed(setup.context, cause, p)
            val signal = PersistFailed(cause, command.toOption)
            if (setup.onSignal(state.state, signal, catchAndLog = false)) {
              setup.internalLogger.debug("Emitted signal [{}].", signal)
            }
            throw new JournalFailureException(setup.persistenceId, p.sequenceNr, p.payload.getClass.getName, cause)
          } else this

        case WriteMessagesSuccessful =>
          // ignore
          this

        case WriteMessagesFailed(_, _) =>
          // ignore
          this // it will be stopped by the first WriteMessageFailure message; not applying side effects

        case _ =>
          onDeleteEventsJournalResponse(response, visibleState.state)
      }
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for journal responses before stopping
        state = state.copy(receivedPoisonPill = true)
        this
      case signal =>
        if (setup.onSignal(visibleState.state, signal, catchAndLog = false)) this
        else Behaviors.unhandled
    }

    // WithSeqNrAccessible
    override def currentSequenceNumber: Long =
      setup.currentSequenceNumber

    // WithMetadataAccessible
    override def metadata[M: ClassTag]: Option[M] =
      CompositeMetadata.extract[M](setup.currentMetadata)
  }

  // ===============================================

  def waitAsyncEffect(
      msg: Any,
      state: RunningState[S],
      laterEffect: Future[Effect[E, S]]): Behavior[InternalProtocol] = {
    setup.setMdcPhase(PersistenceMdc.WaitingAsyncEffect)
    recursiveUnstashOne = 0
    setup.context.pipeToSelf(laterEffect) {
      case Success(effect) => AsyncEffectCompleted(msg, effect)
      case Failure(exc) =>
        setup.internalLogger.debug(s"Async effect failed: $exc")
        AsyncEffectCompleted(msg, Effect.none[E, S].thenRun(_ => throw exc))
    }
    new WaitingAsyncEffect(state)
  }

  /** INTERNAL API */
  @InternalApi private[akka] class WaitingAsyncEffect(var state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible
      with WithMetadataAccessible {

    override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
      msg match {
        case completed: AsyncEffectCompleted[C @unchecked, E @unchecked, S @unchecked] =>
          onAsyncEffectCompleted(completed.cmd, completed.effect)
        case in: IncomingCommand[C @unchecked]         => onCommand(in)
        case re: ReplicatedEventEnvelope[E @unchecked] => onReplicatedEvent(re)
        case pe: PublishedEventImpl                    => onPublishedEvent(pe)
        case JournalResponse(r)                        => onDeleteEventsJournalResponse(r, state.state)
        case SnapshotterResponse(r)                    => onDeleteSnapshotResponse(r, state.state)
        case get: GetState[S @unchecked]               => stashInternal(get)
        case getSeqNr: GetSeenSequenceNr               => onGetSeenSequenceNr(getSeqNr)
        case getVersion: GetVersion                    => onGetVersion(getVersion)
        case _                                         => Behaviors.unhandled
      }
    }

    def onAsyncEffectCompleted(cmd: C, effect: Effect[E, S]): Behavior[InternalProtocol] = {
      val handlingCommands = new HandlingCommands(state)
      handlingCommands.runEffect(cmd, effect)
    }

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    def onGetSeenSequenceNr(get: GetSeenSequenceNr): WaitingAsyncEffect = {
      get.replyTo ! state.seenPerReplica.getOrElse(get.replica, 0L)
      this
    }

    def onGetVersion(get: GetVersion): Behavior[InternalProtocol] = {
      get.replyTo ! state.version
      this
    }

    def onReplicatedEvent(event: InternalProtocol.ReplicatedEventEnvelope[E]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        Behaviors.unhandled
      } else {
        stashInternal(event)
      }
    }

    def onPublishedEvent(event: PublishedEventImpl): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        Behaviors.unhandled
      } else {
        stashInternal(event)
      }
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for completion of async effect before stopping
        state = state.copy(receivedPoisonPill = true)
        this
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false)) this
        else Behaviors.unhandled
    }

    // WithSeqNrAccessible
    override def currentSequenceNumber: Long =
      setup.currentSequenceNumber

    // WithMetadataAccessible
    override def metadata[M: ClassTag]: Option[M] =
      CompositeMetadata.extract[M](setup.currentMetadata)
  }

  // ===============================================

  def waitAsyncReplicationIntercept(
      state: RunningState[S],
      interceptResult: Future[Done],
      nextBehaviorF: () => Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    setup.setMdcPhase(PersistenceMdc.AsyncReplicationIntercept)
    recursiveUnstashOne = 0
    setup.context.pipeToSelf(interceptResult) {
      case Success(_) => AsyncReplicationInterceptCompleted(nextBehaviorF)
      case Failure(exc) =>
        setup.internalLogger.debug(s"Async replication intercept failed: $exc")
        throw exc
    }
    new WaitingAsyncReplicationIntercept(state)
  }

  /** INTERNAL API */
  @InternalApi private[akka] final class WaitingAsyncReplicationIntercept(_state: RunningState[S])
      extends WaitingAsyncEffect(_state)
      with WithSeqNrAccessible
      with WithMetadataAccessible {

    override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
      msg match {
        case AsyncReplicationInterceptCompleted(nextBehaviorF) => nextBehaviorF()
        case in: IncomingCommand[C @unchecked]                 => onCommand(in)
        case re: ReplicatedEventEnvelope[E @unchecked]         => onReplicatedEvent(re)
        case pe: PublishedEventImpl                            => onPublishedEvent(pe)
        case JournalResponse(r)                                => onDeleteEventsJournalResponse(r, state.state)
        case SnapshotterResponse(r)                            => onDeleteSnapshotResponse(r, state.state)
        case get: GetState[S @unchecked]                       => stashInternal(get)
        case getSeqNr: GetSeenSequenceNr                       => onGetSeenSequenceNr(getSeqNr)
        case getVersion: GetVersion                            => onGetVersion(getVersion)
        case _                                                 => Behaviors.unhandled
      }
    }
  }

  // ===============================================

  /** INTERNAL API */
  @InternalApi private[akka] class StoringSnapshot(
      state: RunningState[S],
      sideEffects: immutable.Seq[SideEffect[S]],
      snapshotReason: SnapshotAfterPersist)
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible
      with WithMetadataAccessible {
    setup.setMdcPhase(PersistenceMdc.StoringSnapshot)
    recursiveUnstashOne = 0

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    def onSaveSnapshotResponse(response: SnapshotProtocol.Response): Unit = {
      val signal = response match {
        case SaveSnapshotSuccess(meta) =>
          setup.internalLogger.debug(s"Persistent snapshot [{}] saved successfully", meta)
          if (snapshotReason == SnapshotWithoutRetention && setup.snapshotWhen.deleteEventsOnSnapshot) {
            def retentionBasedSequence = setup.retention match {
              case s @ SnapshotCountRetentionCriteriaImpl(_, _, true) =>
                val deleteEventsToSeqNr = s.deleteUpperSequenceNr(meta.sequenceNr)
                setup.retentionProgressDeleteEventsStarted(state.seqNr, deleteEventsToSeqNr)
                Some(deleteEventsToSeqNr) // keepNSnapshots batches of events
              case _ => None
            }
            val deleteEventsToSeqNr =
              if (setup.isOnlyOneSnapshot) Some(meta.sequenceNr) // delete all events up to the snapshot
              else retentionBasedSequence
            internalDeleteEvents(meta.sequenceNr, deleteEventsToSeqNr.getOrElse(meta.sequenceNr))

          } else if (snapshotReason == SnapshotWithRetention) {
            // deletion of old events and snapshots are triggered by the SaveSnapshotSuccess
            setup.retention match {
              case DisabledRetentionCriteria => // no further actions
              case s @ SnapshotCountRetentionCriteriaImpl(_, _, true) =>
                setup.retentionProgressSaveSnapshotEnded(state.seqNr, success = true)
                // deleteEventsOnSnapshot == true, deletion of old events
                val deleteEventsToSeqNr = {
                  if (setup.isOnlyOneSnapshot) meta.sequenceNr // delete all events up to the snapshot
                  else s.deleteUpperSequenceNr(meta.sequenceNr) // keepNSnapshots batches of events
                }
                // snapshot deletion then happens on event deletion success in Running.onDeleteEventsJournalResponse
                setup.retentionProgressDeleteEventsStarted(state.seqNr, deleteEventsToSeqNr)
                internalDeleteEvents(meta.sequenceNr, deleteEventsToSeqNr)
              case s @ SnapshotCountRetentionCriteriaImpl(_, _, false) =>
                setup.retentionProgressSaveSnapshotEnded(state.seqNr, success = true)
                // deleteEventsOnSnapshot == false, deletion of old snapshots
                if (!setup.isOnlyOneSnapshot) {
                  val deleteSnapshotsToSeqNr = s.deleteUpperSequenceNr(meta.sequenceNr)
                  setup.retentionProgressDeleteSnapshotsStarted(deleteSnapshotsToSeqNr)
                  internalDeleteSnapshots(deleteSnapshotsToSeqNr)
                }
              case unexpected => throw new IllegalStateException(s"Unexpected retention criteria: $unexpected")
            }
          }

          Some(SnapshotCompleted(SnapshotMetadata.fromClassic(meta)))

        case SaveSnapshotFailure(meta, error) =>
          if (snapshotReason == SnapshotWithRetention)
            setup.retentionProgressSaveSnapshotEnded(state.seqNr, success = false)
          setup.internalLogger.warn("Failed to save snapshot given metadata [{}] due to: {}", meta, error.getMessage)
          Some(SnapshotFailed(SnapshotMetadata.fromClassic(meta), error))

        case _ =>
          None
      }

      signal match {
        case Some(signal) =>
          setup.internalLogger.debug("Received snapshot response [{}].", response)
          if (setup.onSignal(state.state, signal, catchAndLog = false)) {
            setup.internalLogger.debug("Emitted signal [{}].", signal)
          }
        case None =>
          setup.internalLogger.debug("Received snapshot response [{}], no signal emitted.", response)
      }
    }

    def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = msg match {
      case cmd: IncomingCommand[C] @unchecked =>
        onCommand(cmd)
      case JournalResponse(r) =>
        onDeleteEventsJournalResponse(r, state.state)
      case SnapshotterResponse(response) =>
        response match {
          case _: SaveSnapshotSuccess | _: SaveSnapshotFailure =>
            onSaveSnapshotResponse(response)
            tryUnstashOne(applySideEffects(sideEffects, state))
          case _ =>
            onDeleteSnapshotResponse(response, state.state)
        }
      case get: GetState[S @unchecked] =>
        stashInternal(get)
      case get: GetSeenSequenceNr =>
        stashInternal(get)
      case get: GetVersion =>
        stashInternal(get)
      case _ =>
        Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for snapshot response before stopping
        new StoringSnapshot(state.copy(receivedPoisonPill = true), sideEffects, snapshotReason)
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false))
          Behaviors.same
        else
          Behaviors.unhandled
    }

    // WithSeqNrAccessible
    override def currentSequenceNumber: Long =
      setup.currentSequenceNumber

    // WithMetadataAccessible
    override def metadata[M: ClassTag]: Option[M] =
      CompositeMetadata.extract[M](setup.currentMetadata)
  }

  // ===============================================

  /** INTERNAL API */
  @InternalApi private[akka] class WaitingForContinueUnstash(state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible
      with WithMetadataAccessible {

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = msg match {
      case ContinueUnstash =>
        tryUnstashOne(new HandlingCommands(state))
      case cmd: IncomingCommand[C] @unchecked =>
        onCommand(cmd)
      case get: GetState[S @unchecked] =>
        stashInternal(get)
      case get: GetSeenSequenceNr =>
        stashInternal(get)
      case get: GetVersion =>
        stashInternal(get)
      case _ =>
        Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for ContinueUnstash before stopping
        new WaitingForContinueUnstash(state.copy(receivedPoisonPill = true))
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false))
          Behaviors.same
        else
          Behaviors.unhandled
    }

    // WithSeqNrAccessible
    override def currentSequenceNumber: Long =
      setup.currentSequenceNumber

    // WithMetadataAccessible
    override def metadata[M: ClassTag]: Option[M] =
      CompositeMetadata.extract[M](setup.currentMetadata)
  }

  // --------------------------

  def applySideEffects(effects: immutable.Seq[SideEffect[S]], state: RunningState[S]): Behavior[InternalProtocol] = {
    var behavior: Behavior[InternalProtocol] = new HandlingCommands(state)
    val it = effects.iterator

    // if at least one effect results in a `stop`, we need to stop
    // manual loop implementation to avoid allocations and multiple scans
    while (it.hasNext) {
      val effect = it.next()
      behavior = applySideEffect(effect, state, behavior)
    }

    if (state.receivedPoisonPill && isInternalStashEmpty && !isUnstashAllInProgress)
      Behaviors.stopped
    else
      behavior
  }

  def applySideEffect(
      effect: SideEffect[S],
      state: RunningState[S],
      behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    effect match {
      case _: Stop.type @unchecked =>
        Behaviors.stopped

      case _: UnstashAll.type @unchecked =>
        unstashAll()
        behavior

      case callback: Callback[Any] @unchecked =>
        callback.sideEffect(state.state)
        behavior

      case _ =>
        // case _: Callback[S] should be covered by above case, but needed needed to silence Scala 3 exhaustive match
        throw new IllegalStateException(
          s"Unexpected effect [${effect.getClass.getName}]. This is a bug, please report https://github.com/akka/akka-core/issues")
    }
  }

  /**
   * Handle journal responses for non-persist events workloads.
   * These are performed in the background and may happen in all phases.
   */
  def onDeleteEventsJournalResponse(response: JournalProtocol.Response, state: S): Behavior[InternalProtocol] = {
    val signal = response match {
      case DeleteMessagesSuccess(toSequenceNr) =>
        setup.internalLogger.debug("Persistent events to sequenceNr [{}] deleted successfully.", toSequenceNr)
        setup.retentionProgressDeleteEventsEnded(toSequenceNr, success = true)
        setup.retention match {
          case DisabledRetentionCriteria => // no further actions
          case _: SnapshotCountRetentionCriteriaImpl =>
            if (!setup.isOnlyOneSnapshot) {
              // The reason for -1 is that a snapshot at the exact toSequenceNr is still useful and the events
              // after that can be replayed after that snapshot, but replaying the events after toSequenceNr without
              // starting at the snapshot at toSequenceNr would be invalid.
              val deleteSnapshotsToSeqNr = toSequenceNr - 1
              setup.retentionProgressDeleteSnapshotsStarted(deleteSnapshotsToSeqNr)
              internalDeleteSnapshots(deleteSnapshotsToSeqNr)
            }
          case unexpected => throw new IllegalStateException(s"Unexpected retention criteria: $unexpected")
        }
        Some(DeleteEventsCompleted(toSequenceNr))
      case DeleteMessagesFailure(e, toSequenceNr) =>
        setup.retentionProgressDeleteEventsEnded(toSequenceNr, success = false)
        Some(DeleteEventsFailed(toSequenceNr, e))
      case _ =>
        None
    }

    signal match {
      case Some(sig) =>
        if (setup.onSignal(state, sig, catchAndLog = false)) Behaviors.same else Behaviors.unhandled
      case None =>
        Behaviors.unhandled // unexpected journal response
    }
  }

  /**
   * Handle snapshot responses for non-persist events workloads.
   * These are performed in the background and may happen in all phases.
   */
  def onDeleteSnapshotResponse(response: SnapshotProtocol.Response, state: S): Behavior[InternalProtocol] = {
    val signal = response match {
      case DeleteSnapshotsSuccess(criteria) =>
        setup.retentionProgressDeleteSnapshotsEnded(criteria.maxSequenceNr, success = true)
        Some(DeleteSnapshotsCompleted(DeletionTarget.Criteria(SnapshotSelectionCriteria.fromClassic(criteria))))
      case DeleteSnapshotsFailure(criteria, error) =>
        setup.retentionProgressDeleteSnapshotsEnded(criteria.maxSequenceNr, success = false)
        Some(DeleteSnapshotsFailed(DeletionTarget.Criteria(SnapshotSelectionCriteria.fromClassic(criteria)), error))
      case DeleteSnapshotSuccess(meta) =>
        Some(DeleteSnapshotsCompleted(DeletionTarget.Individual(SnapshotMetadata.fromClassic(meta))))
      case DeleteSnapshotFailure(meta, error) =>
        Some(DeleteSnapshotsFailed(DeletionTarget.Individual(SnapshotMetadata.fromClassic(meta)), error))
      case _ =>
        None
    }

    signal match {
      case Some(sig) =>
        if (setup.onSignal(state, sig, catchAndLog = false)) Behaviors.same else Behaviors.unhandled
      case None =>
        Behaviors.unhandled // unexpected snapshot response
    }
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteFailed(ctx: ActorContext[_], reason: Throwable, event: PersistentRepr): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteRejected(ctx: ActorContext[_], reason: Throwable, event: PersistentRepr): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteSuccess(ctx: ActorContext[_], event: PersistentRepr): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteDone(ctx: ActorContext[_], event: PersistentRepr): Unit = ()
}
