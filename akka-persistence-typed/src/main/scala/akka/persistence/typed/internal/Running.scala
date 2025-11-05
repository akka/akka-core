/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.typed.internal

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable

import akka.Done
import akka.actor.UnhandledMessage
import akka.actor.typed.{ Behavior, Signal }
import akka.actor.typed.ActorRef
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors, LoggerOps }
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.event.Logging
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
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.{ GetSeenSequenceNr, GetState, GetStateReply }
import akka.persistence.typed.internal.InternalProtocol.ReplicatedEventEnvelope
import akka.persistence.typed.internal.JournalInteractions.EventToPersist
import akka.persistence.typed.internal.Running.WithSeqNrAccessible
import akka.persistence.typed.scaladsl.Effect
import akka.stream.{ RestartSettings, SystemMaterializer, WatchedActorTerminatedException }
import akka.stream.scaladsl.{ RestartSource, Sink }
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.OptionVal
import akka.util.Timeout
import akka.util.unused

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

  trait WithSeqNrAccessible {
    def currentSequenceNumber: Long
  }

  final case class RunningState[State](
      seqNr: Long,
      state: State,
      receivedPoisonPill: Boolean,
      version: VersionVector,
      seenPerReplica: Map[ReplicaId, Long],
      replicationControl: Map[ReplicaId, ReplicationStreamControl]) {

    def nextSequenceNr(): RunningState[State] =
      copy(seqNr = seqNr + 1)

    def updateLastSequenceNr(persistent: PersistentRepr): RunningState[State] =
      if (persistent.sequenceNr > seqNr) copy(seqNr = persistent.sequenceNr) else this

    def applyEvent[C, E](setup: BehaviorSetup[C, E, State], event: E): RunningState[State] = {
      val updated = setup.eventHandler(state, event)
      copy(state = updated)
    }
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
                  // from each replica, only get the events that originated there, this prevents most of the event filtering
                  // the downside is that events can't be received via other replicas in the event of an uneven network partition
                  .filter(event =>
                    event.eventMetadata match {
                      case Some(replicatedMeta: ReplicatedEventMetadata) => replicatedMeta.originReplica == replicaId
                      case _ =>
                        throw new IllegalArgumentException(
                          s"Replication stream from replica ${replicaId} for ${setup.persistenceId} contains event " +
                          s"(sequence nr ${event.sequenceNr}) without replication metadata. " +
                          s"Is the persistence id used by a regular event sourced actor there or the journal for that replica (${queryPluginId}) " +
                          "used that does not support Replicated Event Sourcing?")
                    })
                  .viaMat(new FastForwardingFilter)(Keep.right)
                  .mapMaterializedValue(streamControl => controlRef.set(streamControl))
              }
            }
          }
          // needs to be outside of the restart source so that it actually cancels when terminating the replica
          .via(ActorFlow
            .ask[EventEnvelope, ReplicatedEventEnvelope[E], ReplicatedEventAck.type](ref) { (eventEnvelope, replyTo) =>
              // Need to handle this not being available migration from non-replicated is supported
              val meta = eventEnvelope.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
              val re =
                ReplicatedEvent[E](
                  eventEnvelope.event.asInstanceOf[E],
                  meta.originReplica,
                  meta.originSequenceNr,
                  meta.version)
              ReplicatedEventEnvelope(re, replyTo)
            }
            .recoverWithRetries(1, {
              // not a failure, the replica is stopping, complete the stream
              case _: WatchedActorTerminatedException =>
                Source.empty
            }))

        source.runWith(Sink.ignore)(SystemMaterializer(system).materializer)

        // TODO support from journal to fast forward https://github.com/akka/akka/issues/29311
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

  // Needed for WithSeqNrAccessible, when unstashing
  private var _currentSequenceNumber = 0L

  final class HandlingCommands(state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible {

    _currentSequenceNumber = state.seqNr

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
      val (next, doUnstash) = applyEffects(cmd, state, effect.asInstanceOf[EffectImpl[E, S]]) // TODO can we avoid the cast?
      if (doUnstash) tryUnstashOne(next)
      else next
    }

    def onReplicatedEvent(
        state: Running.RunningState[S],
        envelope: ReplicatedEventEnvelope[E],
        replication: ReplicationSetup): Behavior[InternalProtocol] = {
      setup.internalLogger.debugN(
        "Replica {} received replicated event. Replica seqs nrs: {}. Envelope {}",
        setup.replication,
        state.seenPerReplica,
        envelope)
      envelope.ack ! ReplicatedEventAck
      if (envelope.event.originReplica != replication.replicaId && !alreadySeen(envelope.event)) {
        setup.internalLogger.debug(
          "Saving event [{}] from [{}] as first time",
          envelope.event.originSequenceNr,
          envelope.event.originReplica)
        handleExternalReplicatedEventPersist(replication, envelope.event, None)
      } else {
        setup.internalLogger.debug(
          "Filtering event [{}] from [{}] as it was already seen",
          envelope.event.originSequenceNr,
          envelope.event.originReplica)
        tryUnstashOne(this)
      }
    }

    def onPublishedEvent(state: Running.RunningState[S], event: PublishedEventImpl): Behavior[InternalProtocol] = {
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
              onPublishedEvent(state, replication, replicatedEventMetaData, event)
          }
      }
      tryUnstashOne(newBehavior)
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
      } else if (!replication.allReplicas.contains(originReplicaId)) {
        log.warnN(
          "Received published replicated event from replica [{}], which is unknown. Replicated Event Sourcing must be set up with a list of all replicas (known are [{}]).",
          originReplicaId,
          replication.allReplicas.mkString(", "))
        this
      } else {
        val seenSequenceNr = state.seenPerReplica.getOrElse(originReplicaId, 0L)
        if (seenSequenceNr >= event.sequenceNumber) {
          // already seen/deduplication
          if (log.isDebugEnabled)
            log.debugN(
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
            log.debugN(
              "Ignoring published replicated event with replication seqNr [{}] from replica [{}] " +
              "because expected replication seqNr was [{}] ",
              event.sequenceNumber,
              originReplicaId,
              seenSequenceNr + 1)
          }
          this
        } else {
          if (log.isTraceEnabled) {
            log.traceN(
              "Received published replicated event [{}] with timestamp [{} (UTC)] from replica [{}] seqNr [{}]",
              Logging.simpleName(event.event.getClass),
              formatTimestamp(event.timestamp),
              originReplicaId,
              event.sequenceNumber)
          }

          // fast forward stream for source replica
          state.replicationControl.get(originReplicaId).foreach(_.fastForward(event.sequenceNumber))

          handleExternalReplicatedEventPersist(
            replication,
            ReplicatedEvent(
              event.event.asInstanceOf[E],
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

    private def handleExternalReplicatedEventPersist(
        replication: ReplicationSetup,
        event: ReplicatedEvent[E],
        ackToOnPersisted: Option[ActorRef[Done]]): Behavior[InternalProtocol] = {
      _currentSequenceNumber = state.seqNr + 1
      val isConcurrent: Boolean = event.originVersion <> state.version
      val updatedVersion = event.originVersion.merge(state.version)

      if (setup.internalLogger.isDebugEnabled())
        setup.internalLogger.debugN(
          "Processing event [{}] with version [{}]. Local version: {}. Updated version {}. Concurrent? {}",
          Logging.simpleName(event.event.getClass),
          event.originVersion,
          state.version,
          updatedVersion,
          isConcurrent)

      replication.setContext(recoveryRunning = false, event.originReplica, concurrent = isConcurrent)

      val stateAfterApply = state.applyEvent(setup, event.event)
      val eventToPersist = adaptEvent(stateAfterApply.state, event.event)
      val eventAdapterManifest = setup.eventAdapter.manifest(event.event)

      replication.clearContext()

      val sideEffects = ackToOnPersisted match {
        case None => Nil
        case Some(ref) =>
          SideEffect { (_: S) =>
            ref ! Done
          } :: Nil
      }

      val newState2: RunningState[S] = internalPersist(
        setup.context,
        null,
        stateAfterApply,
        eventToPersist,
        eventAdapterManifest,
        OptionVal.Some(
          ReplicatedEventMetadata(event.originReplica, event.originSequenceNr, updatedVersion, isConcurrent)))
      val shouldSnapshotAfterPersist = setup.shouldSnapshot(newState2.state, event.event, newState2.seqNr)
      // FIXME validate this is the correct sequence nr from that replica https://github.com/akka/akka/issues/29259
      val updatedSeen = newState2.seenPerReplica.updated(event.originReplica, event.originSequenceNr)
      persistingEvents(
        newState2.copy(seenPerReplica = updatedSeen, version = updatedVersion),
        state,
        numberOfEvents = 1,
        shouldSnapshotAfterPersist,
        shouldPublish = false,
        sideEffects)
    }

    private def handleEventPersist(
        event: E,
        cmd: Any,
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      try {
        // apply the event before persist so that validation exception is handled before persisting
        // the invalid event, in case such validation is implemented in the event handler.
        // also, ensure that there is an event handler for each single event
        _currentSequenceNumber = state.seqNr + 1

        setup.replication.foreach(r => r.setContext(recoveryRunning = false, r.replicaId, concurrent = false))

        val stateAfterApply = state.applyEvent(setup, event)
        val eventToPersist = adaptEvent(stateAfterApply.state, event)
        val eventAdapterManifest = setup.eventAdapter.manifest(event)

        val newState2 = setup.replication match {
          case Some(replication) =>
            val updatedVersion = stateAfterApply.version.updated(replication.replicaId.id, _currentSequenceNumber)
            val r = internalPersist(
              setup.context,
              cmd,
              stateAfterApply,
              eventToPersist,
              eventAdapterManifest,
              OptionVal.Some(
                ReplicatedEventMetadata(
                  replication.replicaId,
                  _currentSequenceNumber,
                  updatedVersion,
                  concurrent = false))).copy(version = updatedVersion)

            if (setup.internalLogger.isTraceEnabled())
              setup.internalLogger.traceN(
                "Event persisted [{}]. Version vector after: [{}]",
                Logging.simpleName(event.getClass),
                r.version)

            r
          case None =>
            internalPersist(setup.context, cmd, stateAfterApply, eventToPersist, eventAdapterManifest, OptionVal.None)
        }

        val shouldSnapshotAfterPersist = setup.shouldSnapshot(newState2.state, event, newState2.seqNr)
        (
          persistingEvents(
            newState2,
            state,
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
        events: immutable.Seq[E],
        cmd: Any,
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      if (events.nonEmpty) {
        try {
          // apply the event before persist so that validation exception is handled before persisting
          // the invalid event, in case such validation is implemented in the event handler.
          // also, ensure that there is an event handler for each single event
          _currentSequenceNumber = state.seqNr

          val metadataTemplate: Option[ReplicatedEventMetadata] = setup.replication match {
            case Some(replication) =>
              replication.setContext(recoveryRunning = false, replication.replicaId, concurrent = false) // local events are never concurrent
              Some(ReplicatedEventMetadata(replication.replicaId, 0L, state.version, concurrent = false)) // we replace it with actual seqnr later
            case None => None
          }

          var currentState = state
          var shouldSnapshotAfterPersist: SnapshotAfterPersist = NoSnapshot
          var eventsToPersist: List[EventToPersist] = Nil

          events.foreach { event =>
            _currentSequenceNumber += 1
            if (shouldSnapshotAfterPersist == NoSnapshot)
              shouldSnapshotAfterPersist = setup.shouldSnapshot(currentState.state, event, _currentSequenceNumber)
            val evtManifest = setup.eventAdapter.manifest(event)
            val eventMetadata = metadataTemplate match {
              case Some(template) =>
                val updatedVersion = currentState.version.updated(template.originReplica.id, _currentSequenceNumber)
                if (setup.internalLogger.isDebugEnabled)
                  setup.internalLogger.traceN(
                    "Processing event [{}] with version vector [{}]",
                    Logging.simpleName(event.getClass),
                    updatedVersion)
                currentState = currentState.copy(version = updatedVersion)
                Some(template.copy(originSequenceNr = _currentSequenceNumber, version = updatedVersion))
              case None => None
            }

            currentState = currentState.applyEvent(setup, event)

            val adaptedEvent = adaptEvent(currentState.state, event)

            eventsToPersist = EventToPersist(adaptedEvent, evtManifest, eventMetadata) :: eventsToPersist
          }

          val newState2 =
            internalPersistAll(setup.context, cmd, currentState, eventsToPersist.reverse)

          (
            persistingEvents(
              newState2,
              state,
              events.size,
              shouldSnapshotAfterPersist,
              shouldPublish = true,
              sideEffects = sideEffects),
            false)
        } finally {
          setup.replication.foreach(_.clearContext())
        }
      } else {
        // run side-effects even when no events are emitted
        (applySideEffects(sideEffects, state), true)
      }
    }
    @tailrec def applyEffects(
        msg: Any,
        state: RunningState[S],
        effect: Effect[E, S],
        sideEffects: immutable.Seq[SideEffect[S]] = Nil): (Behavior[InternalProtocol], Boolean) = {
      if (setup.internalLogger.isDebugEnabled && !effect.isInstanceOf[CompositeEffect[_, _]])
        setup.internalLogger.debugN(
          s"Handled command [{}], resulting effect: [{}], side effects: [{}]",
          msg.getClass.getName,
          effect,
          sideEffects.size)

      effect match {
        case CompositeEffect(eff, currentSideEffects) =>
          // unwrap and accumulate effects
          applyEffects(msg, state, eff, currentSideEffects ++ sideEffects)

        case Persist(event) =>
          handleEventPersist(event, msg, sideEffects)

        case PersistAll(events) =>
          handleEventPersistAll(events, msg, sideEffects)

        case _: PersistNothing.type =>
          (applySideEffects(sideEffects, state), true)

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

    setup.setMdcPhase(PersistenceMdc.RunningCmds)

    override def currentSequenceNumber: Long =
      _currentSequenceNumber
  }

  // ===============================================

  def persistingEvents(
      state: RunningState[S],
      visibleState: RunningState[S], // previous state until write success
      numberOfEvents: Int,
      shouldSnapshotAfterPersist: SnapshotAfterPersist,
      shouldPublish: Boolean,
      sideEffects: immutable.Seq[SideEffect[S]]): Behavior[InternalProtocol] = {
    setup.setMdcPhase(PersistenceMdc.PersistingEvents)
    new PersistingEvents(state, visibleState, numberOfEvents, shouldSnapshotAfterPersist, shouldPublish, sideEffects)
  }

  /** INTERNAL API */
  @InternalApi private[akka] class PersistingEvents(
      var state: RunningState[S],
      var visibleState: RunningState[S], // previous state until write success
      numberOfEvents: Int,
      shouldSnapshotAfterPersist: SnapshotAfterPersist,
      shouldPublish: Boolean,
      var sideEffects: immutable.Seq[SideEffect[S]],
      persistStartTime: Long = System.nanoTime())
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible {

    private var eventCounter = 0

    override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
      msg match {
        case JournalResponse(r)                        => onJournalResponse(r)
        case in: IncomingCommand[C @unchecked]         => onCommand(in)
        case re: ReplicatedEventEnvelope[E @unchecked] => onReplicatedEvent(re)
        case pe: PublishedEventImpl                    => onPublishedEvent(pe)
        case get: GetState[S @unchecked]               => stashInternal(get)
        case getSeqNr: GetSeenSequenceNr               => onGetSeenSequenceNr(getSeqNr)
        case SnapshotterResponse(r)                    => onDeleteSnapshotResponse(r, visibleState.state)
        case RecoveryTickEvent(_)                      => Behaviors.unhandled
        case RecoveryPermitGranted                     => Behaviors.unhandled
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
        setup.internalLogger.debug2(
          "Received Journal response: {} after: {} nanos",
          response,
          System.nanoTime() - persistStartTime)
      }

      def onWriteResponse(p: PersistentRepr): Behavior[InternalProtocol] = {
        state = state.updateLastSequenceNr(p)
        eventCounter += 1

        onWriteSuccess(setup.context, p)

        if (setup.publishEvents && shouldPublish) {
          val meta = setup.replication.map(replication =>
            new ReplicatedPublishedEventMetaData(replication.replicaId, state.version))
          context.system.eventStream ! EventStream.Publish(
            PublishedEventImpl(setup.persistenceId, p.sequenceNr, p.payload, p.timestamp, meta, None))
        }

        // only once all things are applied we can revert back
        if (eventCounter < numberOfEvents) {
          onWriteDone(setup.context, p)
          this
        } else {
          visibleState = state
          def skipRetention(): Boolean = {
            // only one retention process at a time
            val inProgress = shouldSnapshotAfterPersist == SnapshotWithRetention && setup.isRetentionInProgress()
            if (inProgress)
              setup.internalLogger.info(
                "Skipping retention at seqNr [{}] because previous retention has not completed yet. " +
                "Next retention will cover skipped retention.",
                state.seqNr)
            inProgress
          }
          if (shouldSnapshotAfterPersist == NoSnapshot || state.state == null || skipRetention()) {
            val newState = applySideEffects(sideEffects, state)
            onWriteDone(setup.context, p)
            tryUnstashOne(newState)
          } else {
            if (shouldSnapshotAfterPersist == SnapshotWithRetention)
              setup.retentionProgressSaveSnapshotStarted(state.seqNr)
            internalSaveSnapshot(state)
            new StoringSnapshot(state, sideEffects, shouldSnapshotAfterPersist)
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
            onWriteRejected(setup.context, cause, p)
            throw new EventRejectedException(setup.persistenceId, p.sequenceNr, cause)
          } else this

        case WriteMessageFailure(p, cause, id) =>
          if (id == setup.writerIdentity.instanceId) {
            onWriteFailed(setup.context, cause, p)
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

    override def currentSequenceNumber: Long = {
      _currentSequenceNumber
    }
  }

  // ===============================================

  /** INTERNAL API */
  @InternalApi private[akka] class StoringSnapshot(
      state: RunningState[S],
      sideEffects: immutable.Seq[SideEffect[S]],
      snapshotReason: SnapshotAfterPersist)
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithSeqNrAccessible {
    setup.setMdcPhase(PersistenceMdc.StoringSnapshot)

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
          if (snapshotReason == SnapshotWithRetention) {
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
          setup.internalLogger.warn2("Failed to save snapshot given metadata [{}] due to: {}", meta, error.getMessage)
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

    override def currentSequenceNumber: Long =
      _currentSequenceNumber
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

      case callback: Callback[_] =>
        callback.sideEffect(state.state)
        behavior
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

  @InternalStableApi
  private[akka] def onWriteFailed(
      @unused ctx: ActorContext[_],
      @unused reason: Throwable,
      @unused event: PersistentRepr): Unit = ()
  @InternalStableApi
  private[akka] def onWriteRejected(
      @unused ctx: ActorContext[_],
      @unused reason: Throwable,
      @unused event: PersistentRepr): Unit = ()
  @InternalStableApi
  private[akka] def onWriteSuccess(@unused ctx: ActorContext[_], @unused event: PersistentRepr): Unit = ()
  @InternalStableApi
  private[akka] def onWriteDone(@unused ctx: ActorContext[_], @unused event: PersistentRepr): Unit = ()
}
