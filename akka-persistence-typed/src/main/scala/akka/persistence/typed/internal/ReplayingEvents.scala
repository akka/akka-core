/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.collection.immutable
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.internal.UnstashException
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import akka.actor.typed.Signal
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.persistence.typed.EmptyEventSeq
import akka.persistence.typed.EventSeq
import akka.persistence.typed.EventsSeq
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.RecoveryFailed
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.SingleEventSeq
import akka.persistence.typed.internal.BehaviorSetup.SnapshotWithoutRetention
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetSeenSequenceNr
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetState
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetVersion
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.WithMetadataAccessible
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.WithSeqNrAccessible
import akka.persistence.typed.internal.ReplayingEvents.ReplayingState
import akka.persistence.typed.internal.Running.startReplicationStream
import akka.util.OptionVal
import akka.util.PrettyDuration._

/***
 * INTERNAL API
 *
 * Third (of four) behavior of an EventSourcedBehavior.
 *
 * In this behavior we finally start replaying events, beginning from the last applied sequence number
 * (i.e. the one up-until-which the snapshot recovery has brought us).
 *
 * Once recovery is completed, the actor becomes [[Running]], stashed messages are flushed
 * and control is given to the user's handlers to drive the actors behavior from there.
 *
 * See next behavior [[Running]].
 * See previous behavior [[ReplayingSnapshot]].
 */
@InternalApi
private[akka] object ReplayingEvents {

  @InternalApi
  private[akka] final case class ReplayingState[State](
      seqNr: Long,
      state: State,
      eventSeenInInterval: Boolean,
      toSeqNr: Long,
      receivedPoisonPill: Boolean,
      recoveryStartTime: Long,
      version: VersionVector,
      seenSeqNrPerReplica: Map[ReplicaId, Long],
      eventsReplayed: Long)

  def apply[C, E, S](setup: BehaviorSetup[C, E, S], state: ReplayingState[S]): Behavior[InternalProtocol] =
    Behaviors.setup { _ =>
      // protect against event recovery stalling forever because of journal overloaded and such
      setup.startRecoveryTimer(snapshot = false)
      new ReplayingEvents[C, E, S](setup.setMdcPhase(PersistenceMdc.ReplayingEvents), state)
    }

}

@InternalApi
private[akka] final class ReplayingEvents[C, E, S](
    override val setup: BehaviorSetup[C, E, S],
    var state: ReplayingState[S])
    extends AbstractBehavior[InternalProtocol](setup.context)
    with JournalInteractions[C, E, S]
    with SnapshotInteractions[C, E, S]
    with StashManagement[C, E, S]
    with WithSeqNrAccessible
    with WithMetadataAccessible {

  import InternalProtocol._
  import ReplayingEvents.ReplayingState

  replayEvents(state.seqNr + 1L, state.toSeqNr)
  onRecoveryStart(setup.context)

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryStart(context: ActorContext[_]): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryComplete(context: ActorContext[_]): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryFailed(context: ActorContext[_], reason: Throwable, event: Option[Any]): Unit =
    ()

  override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
    msg match {
      case JournalResponse(r)                         => onJournalResponse(r)
      case SnapshotterResponse(r)                     => onSnapshotterResponse(r)
      case RecoveryTickEvent(snap)                    => onRecoveryTick(snap)
      case evt: ReplicatedEventEnvelope[E @unchecked] => onInternalCommand(evt)
      case pe: PublishedEventImpl                     => onInternalCommand(pe)
      case cmd: IncomingCommand[C @unchecked]         => onInternalCommand(cmd)
      case get: GetState[S @unchecked]                => stashInternal(get)
      case get: GetSeenSequenceNr                     => stashInternal(get)
      case get: GetVersion                            => stashInternal(get)
      case RecoveryPermitGranted                      => Behaviors.unhandled // should not happen, we already have the permit
      case ContinueUnstash                            => Behaviors.unhandled
      case _: AsyncEffectCompleted[_, _, _]           => Behaviors.unhandled
      case _: AsyncReplicationInterceptCompleted      => Behaviors.unhandled
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
    case PoisonPill =>
      state = state.copy(receivedPoisonPill = true)
      this
    case signal =>
      if (setup.onSignal(state.state, signal, catchAndLog = true)) this
      else Behaviors.unhandled
  }

  private def onJournalResponse(response: JournalProtocol.Response): Behavior[InternalProtocol] = {
    response match {
      case ReplayedMessage(repr) =>
        var eventForErrorReporting: OptionVal[Any] = OptionVal.None
        try {
          val eventSeq =
            if (repr.payload == FilteredPayload) EventSeq.empty // ignore FilteredPayload
            else setup.eventAdapter.fromJournal(repr.payload, repr.manifest)

          def handleEvent(event: E): Unit = {
            eventForErrorReporting = OptionVal.Some(event)
            state = state.copy(seqNr = repr.sequenceNr, eventsReplayed = state.eventsReplayed + 1)
            setup.currentSequenceNumber = repr.sequenceNr
            setup.currentMetadata = repr.metadata

            val replicatedMetaAndSelfReplica: Option[(ReplicatedEventMetadata, ReplicaId, ReplicationSetup)] =
              setup.replication match {
                case Some(replication) =>
                  val meta =
                    CompositeMetadata.extract[ReplicatedEventMetadata](repr.metadata).getOrElse {
                      // migrated from non-replicated, fill in metadata
                      ReplicatedEventMetadata(
                        originReplica = replication.replicaId,
                        originSequenceNr = repr.sequenceNr,
                        version = VersionVector(replication.replicaId.id, repr.sequenceNr),
                        concurrent = false)
                    }

                  replication.setContext(recoveryRunning = true, meta.originReplica, meta.concurrent)
                  Some((meta, replication.replicaId, replication))
                case None => None
              }

            val newState = setup.eventHandler(state.state, event)

            setup.replication match {
              case Some(replication) =>
                replication.clearContext()
              case None =>
            }

            replicatedMetaAndSelfReplica match {
              case Some((meta, selfReplica, replication)) =>
                // keep track of highest origin seqnr per other replica
                val updatedSeen =
                  if (meta.originReplica == selfReplica)
                    state.seenSeqNrPerReplica
                  else
                    state.seenSeqNrPerReplica + (meta.originReplica -> meta.originSequenceNr)

                state = state.copy(
                  state = newState,
                  eventSeenInInterval = true,
                  version = meta.version,
                  seenSeqNrPerReplica = updatedSeen)
                replication.clearContext()

              case _ =>
                state = state.copy(state = newState, eventSeenInInterval = true)
            }
          }

          eventSeq match {
            case SingleEventSeq(event) => handleEvent(event)
            case EventsSeq(events)     => events.foreach(handleEvent)
            case EmptyEventSeq         => // no events
          }

          this
        } catch {
          case NonFatal(ex) =>
            state = state.copy(seqNr = repr.sequenceNr)
            setup.currentSequenceNumber = repr.sequenceNr
            setup.currentMetadata = repr.metadata
            onRecoveryFailure(ex, eventForErrorReporting.toOption, "replaying-event")
        }

      case RecoverySuccess(highestJournalSeqNr) =>
        try {
          val highestSeqNr = Math.max(highestJournalSeqNr, state.seqNr)
          state = state.copy(seqNr = highestSeqNr)
          setup.currentSequenceNumber = highestSeqNr
          setup.internalLogger.debug("Recovery successful, recovered until sequenceNr: [{}]", highestSeqNr)
          onRecoveryCompleted(state)
        } catch {
          case ex: UnstashException[_] =>
            // let supervisor handle it, don't treat as recovery failure
            throw ex

          case NonFatal(cause) =>
            onRecoveryFailure(cause, None, "recovery-completed")
        }

      case ReplayMessagesFailure(cause) =>
        onRecoveryFailure(cause, Some(response), "failure-from-journal")

      case _ =>
        Behaviors.unhandled
    }
  }

  private def onInternalCommand(cmd: InternalProtocol): Behavior[InternalProtocol] = {
    // during recovery, stash all incoming commands
    if (state.receivedPoisonPill) {
      if (setup.settings.logOnStashing)
        setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
      Behaviors.unhandled
    } else {
      stashInternal(cmd)
    }
  }

  protected def onRecoveryTick(snapshot: Boolean): Behavior[InternalProtocol] =
    if (!snapshot) {
      if (state.eventSeenInInterval) {
        state = state.copy(eventSeenInInterval = false)
        this
      } else {
        val msg =
          s"Replay timed out, didn't get event within [${setup.settings.recoveryEventTimeout}], highest sequence number seen [${state.seqNr}]"
        onRecoveryFailure(new RecoveryTimedOut(msg), None, "retrieving-events")
      }
    } else {
      // snapshot timeout, but we're already in the events recovery phase
      Behaviors.unhandled
    }

  def onSnapshotterResponse(response: SnapshotProtocol.Response): Behavior[InternalProtocol] = {
    setup.internalLogger
      .warn("Unexpected [{}] from SnapshotStore, already in replaying events state.", Logging.simpleName(response))
    Behaviors.unhandled // ignore the response
  }

  /**
   * Called whenever a message replay fails.
   *
   * This method throws `JournalFailureException` which will be caught by the internal
   * supervision strategy to stop or restart the actor with backoff.
   *
   * @param cause failure cause.
   * @param event the event that was being processed when the exception was thrown
   */
  private def onRecoveryFailure(cause: Throwable, event: Option[Any], phase: String): Behavior[InternalProtocol] = {
    val instrumentationEvent =
      event match {
        case Some(_: Message) | None => null
        case Some(evt)               => evt
      }
    setup.instrumentation.recoveryFailed(setup.context.self, cause, instrumentationEvent)
    onRecoveryFailed(setup.context, cause, event)

    setup.onSignal(state.state, RecoveryFailed(cause), catchAndLog = true)
    setup.cancelRecoveryTimer()
    tryReturnRecoveryPermit("on replay failure: " + cause.getMessage)
    if (setup.internalLogger.isDebugEnabled) {
      setup.internalLogger.debug(
        "Recovery failure for persistenceId [{}] after {}",
        setup.persistenceId,
        (System.nanoTime() - state.recoveryStartTime).nanos.pretty)
    }
    val sequenceNr = state.seqNr

    val msg = event match {
      case Some(_: Message) | None =>
        s"Exception during recovery. Last known sequence number [$sequenceNr]. " +
        s"PersistenceId [${setup.persistenceId.id}], phase [$phase] due to: ${cause.getMessage}"
      case Some(evt) =>
        s"Exception during recovery while handling [${evt.getClass.getName}] with sequence number [$sequenceNr]. " +
        s"PersistenceId [${setup.persistenceId.id}], phase [$phase] due to: ${cause.getMessage}"
    }

    throw new JournalFailureException(msg, cause)
  }

  private def onRecoveryCompleted(state: ReplayingState[S]): Behavior[InternalProtocol] =
    try {
      setup.instrumentation.recoveryDone(setup.context.self)
      onRecoveryComplete(setup.context)
      tryReturnRecoveryPermit("replay completed successfully")
      if (setup.internalLogger.isDebugEnabled) {
        if (setup.replication.isDefined)
          setup.internalLogger.debug(
            "Recovery for persistenceId [{}] took {}, version [{}], seenSeqNrPerReplica [{}]",
            setup.persistenceId,
            (System.nanoTime() - state.recoveryStartTime).nanos.pretty,
            state.version,
            state.seenSeqNrPerReplica)
        else
          setup.internalLogger.debug(
            "Recovery for persistenceId [{}] took {}",
            setup.persistenceId,
            (System.nanoTime() - state.recoveryStartTime).nanos.pretty)
      }

      setup.onSignal(state.state, RecoveryCompleted, catchAndLog = false)

      if (state.receivedPoisonPill && isInternalStashEmpty && !isUnstashAllInProgress)
        Behaviors.stopped
      else {
        val runningState = Running.RunningState[S](
          seqNr = state.seqNr,
          state = state.state,
          receivedPoisonPill = state.receivedPoisonPill,
          version = state.version,
          seenPerReplica = state.seenSeqNrPerReplica,
          replicationControl = Map.empty,
          instrumentationContexts = Map.empty)
        val running = new Running(setup.setMdcPhase(PersistenceMdc.RunningCmds))
        val initialRunningState = setup.replication match {
          case Some(replication)
              if replication.allReplicasAndQueryPlugins.values.forall(_ != ReplicationContextImpl.NoPlugin) =>
            startReplicationStream(setup, runningState, replication)
          case _ => runningState
        }
        setup.retention match {
          case criteria: SnapshotCountRetentionCriteriaImpl if criteria.snapshotEveryNEvents <= state.eventsReplayed =>
            internalSaveSnapshot(initialRunningState, setup.currentMetadata)
            new running.StoringSnapshot(initialRunningState, immutable.Seq.empty, SnapshotWithoutRetention)
          case _ =>
            tryUnstashOne(new running.HandlingCommands(initialRunningState))
        }
      }
    } finally {
      setup.cancelRecoveryTimer()
    }

  // WithSeqNrAccessible
  override def currentSequenceNumber: Long =
    setup.currentSequenceNumber

  // WithMetadataAccessible
  override def metadata[M: ClassTag]: Option[M] =
    CompositeMetadata.extract[M](setup.currentMetadata)

}
