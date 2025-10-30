/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.slf4j.Logger
import org.slf4j.MDC

import akka.actor.{ ActorRef => ClassicActorRef }
import akka.actor.Cancellable
import akka.actor.typed.Signal
import akka.actor.typed.scaladsl.ActorContext
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.EventWithMetadata
import akka.persistence.typed.scaladsl.Recovery
import akka.persistence.typed.scaladsl.ReplicationInterceptor
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.persistence.typed.telemetry.EventSourcedBehaviorInstrumentation
import akka.persistence.typed.scaladsl.SnapshotWhenPredicate
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] object BehaviorSetup {
  sealed trait SnapshotAfterPersist
  case object NoSnapshot extends SnapshotAfterPersist
  case object SnapshotWithRetention extends SnapshotAfterPersist
  case object SnapshotWithoutRetention extends SnapshotAfterPersist
}

/**
 * INTERNAL API: Carry state for the Persistent behavior implementation behaviors.
 */
@InternalApi
private[akka] final class BehaviorSetup[C, E, S](
    val context: ActorContext[InternalProtocol],
    val persistenceId: PersistenceId,
    val emptyState: S,
    val commandHandler: EventSourcedBehavior.CommandHandler[C, E, S],
    val eventHandler: EventSourcedBehavior.EventHandler[S, E],
    val writerIdentity: EventSourcedBehaviorImpl.WriterIdentity,
    private val signalHandler: PartialFunction[(S, Signal), Unit],
    val tagger: (S, E) => Set[String],
    val eventAdapter: EventAdapter[E, Any],
    val snapshotAdapter: SnapshotAdapter[S],
    val snapshotWhen: SnapshotWhenPredicate[S, E],
    val recovery: Recovery,
    val retention: RetentionCriteria,
    var holdingRecoveryPermit: Boolean,
    val settings: EventSourcedSettings,
    val stashState: StashState,
    val replication: Option[ReplicationSetup],
    val publishEvents: Boolean,
    private val internalLoggerFactory: () => Logger,
    private var retentionInProgress: Boolean,
    val instrumentation: EventSourcedBehaviorInstrumentation,
    val replicationInterceptor: Option[ReplicationInterceptor[S, E]],
    val replicatedEventTransformation: Option[(S, EventWithMetadata[E]) => Seq[EventWithMetadata[E]]]) {

  import BehaviorSetup._
  import InternalProtocol.RecoveryTickEvent

  import akka.actor.typed.scaladsl.adapter._

  val persistence: Persistence = Persistence(context.system.toClassic)

  val journal: ClassicActorRef = persistence.journalFor(settings.journalPluginId)
  val snapshotStore: ClassicActorRef = persistence.snapshotStoreFor(settings.snapshotPluginId)

  val (isSnapshotOptional: Boolean, isOnlyOneSnapshot: Boolean) = {
    val snapshotStoreConfig = Persistence(context.system.classicSystem).configFor(snapshotStore)
    (snapshotStoreConfig.getBoolean("snapshot-is-optional"), snapshotStoreConfig.getBoolean("only-one-snapshot"))
  }

  if (isSnapshotOptional && (retention match {
        case SnapshotCountRetentionCriteriaImpl(_, _, true) => true
        case _                                              => false
      })) {
    throw new IllegalArgumentException(
      "Retention criteria with delete events can't be used together with snapshot-is-optional=true. " +
      "That can result in wrong recovered state if snapshot load fails.")
  }

  if (isSnapshotOptional && snapshotWhen.deleteEventsOnSnapshot) {
    throw new IllegalArgumentException(
      "SnapshotWhen predicate with delete events can't be used together with snapshot-is-optional=true. " +
      "That can result in wrong recovered state if snapshot load fails.")
  }

  val replicaId: Option[ReplicaId] = replication.map(_.replicaId)

  def selfClassic: ClassicActorRef = context.self.toClassic

  private var mdcPhase = PersistenceMdc.Initializing

  // Needed for WithSeqNrAccessible
  var currentSequenceNumber = 0L

  // Needed for WithMetadataAccessible
  var currentMetadata: Option[Any] = None

  if (isOnlyOneSnapshot) {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, keepNSnapshots, _) if keepNSnapshots > 1 =>
        // not using internalLogger because it's probably not good to use mdc from the constructor
        internalLoggerFactory().warn(
          "Retention has been defined with keepNSnapshots [{}] for persistenceId [{}], " +
          "but the snapshot store will only keep one snapshot. You can silence this warning and benefit from " +
          "a performance optimization by defining the retention criteria without the keepNSnapshots parameter.",
          keepNSnapshots,
          persistenceId)
      case _ =>
    }
  }

  def internalLogger: Logger = {
    PersistenceMdc.setMdc(persistenceId, mdcPhase)
    internalLoggerFactory()
  }

  def setMdcPhase(phaseName: String): BehaviorSetup[C, E, S] = {
    mdcPhase = phaseName
    this
  }

  private var recoveryTimer: OptionVal[Cancellable] = OptionVal.None

  def startRecoveryTimer(snapshot: Boolean): Unit = {
    cancelRecoveryTimer()
    implicit val ec: ExecutionContext = context.executionContext
    val timer =
      if (snapshot)
        context.scheduleOnce(settings.recoveryEventTimeout, context.self, RecoveryTickEvent(snapshot = true))
      else
        context.system.scheduler.scheduleWithFixedDelay(settings.recoveryEventTimeout, settings.recoveryEventTimeout) {
          () =>
            context.self ! RecoveryTickEvent(snapshot = false)
        }
    recoveryTimer = OptionVal.Some(timer)
  }

  def cancelRecoveryTimer(): Unit = {
    recoveryTimer match {
      case OptionVal.Some(t) => t.cancel()
      case _                 =>
    }
    recoveryTimer = OptionVal.None
  }

  /**
   * Applies the `signalHandler` if defined and returns true, otherwise returns false.
   * If an exception is thrown and `catchAndLog=true` it is logged and returns true, otherwise it is thrown.
   *
   * `catchAndLog=true` should be used for "unknown" signals in the phases before Running
   * to avoid restart loops if restart supervision is used.
   */
  def onSignal[T](state: S, signal: Signal, catchAndLog: Boolean): Boolean = {
    try {
      var handled = true
      signalHandler.applyOrElse((state, signal), (_: (S, Signal)) => handled = false)
      handled
    } catch {
      case NonFatal(ex) =>
        if (catchAndLog) {
          internalLogger.error(s"Error while processing signal [$signal]: $ex", ex)
          true
        } else {
          if (internalLogger.isDebugEnabled)
            internalLogger.debug(s"Error while processing signal [$signal]: $ex", ex)
          throw ex
        }
    }
  }

  def shouldSnapshot(state: S, event: E, sequenceNr: Long): SnapshotAfterPersist = {
    retention match {
      case DisabledRetentionCriteria =>
        if (snapshotWhen.predicate(state, event, sequenceNr)) SnapshotWithoutRetention
        else NoSnapshot
      case s: SnapshotCountRetentionCriteriaImpl =>
        if (s.snapshotWhen(sequenceNr)) SnapshotWithRetention
        else if (snapshotWhen.predicate(state, event, sequenceNr)) SnapshotWithoutRetention
        else NoSnapshot
      case unexpected => throw new IllegalStateException(s"Unexpected retention criteria: $unexpected")
    }
  }

  // The retention process for SnapshotCountRetentionCriteria looks like this:
  // 1. Save snapshot after persisting events when shouldSnapshotAfterPersist returned SnapshotWithRetention.
  // 2. Delete events (when deleteEventsOnSnapshot=true), runs in background.
  // 3. Delete snapshots (when isOnlyOneSnapshot=false), runs in background.

  def isRetentionInProgress(): Boolean =
    retentionInProgress

  def retentionProgressSaveSnapshotStarted(sequenceNr: Long): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, _) =>
        internalLogger.debug("Starting retention at seqNr [{}], saving snapshot.", sequenceNr)
        retentionInProgress = true
      case _ =>
    }
  }

  def retentionProgressSaveSnapshotEnded(sequenceNr: Long, success: Boolean): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, deleteEvents) if retentionInProgress =>
        if (!success) {
          internalLogger.debug("Retention at seqNr [{}] is completed, saving snapshot failed.", sequenceNr)
          retentionInProgress = false
        } else if (deleteEvents) {
          internalLogger.debug("Retention at seqNr [{}], saving snapshot was successful.", sequenceNr)
        } else if (isOnlyOneSnapshot) {
          // no delete of events and no delete of snapshots => done
          internalLogger.debug("Retention at seqNr [{}] is completed, saving snapshot was successful.", sequenceNr)
          retentionInProgress = false
        } else {
          internalLogger.debug("Retention at seqNr [{}], saving snapshot was successful.", sequenceNr)
        }
      case _ =>
    }
  }

  def retentionProgressDeleteEventsStarted(sequenceNr: Long, deleteToSequenceNr: Long): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, true) if retentionInProgress =>
        if (deleteToSequenceNr > 0) {
          internalLogger.debug(
            "Retention at seqNr [{}], deleting events to seqNr [{}].",
            sequenceNr,
            deleteToSequenceNr)
        } else {
          internalLogger.debug("Retention is completed, no events to delete.")
          retentionInProgress = false
        }
      case _ =>
    }
  }

  def retentionProgressDeleteEventsEnded(deleteToSequenceNr: Long, success: Boolean): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, true) if retentionInProgress =>
        if (!success) {
          internalLogger.debug(
            "Retention at seqNr [{}] is completed, deleting events to seqNr [{}] failed.",
            deleteToSequenceNr)
          retentionInProgress = false
        } else if (isOnlyOneSnapshot) {
          // no delete of snapshots => done
          internalLogger.debug(
            "Retention is completed, deleting events to seqNr [{}] was successful.",
            deleteToSequenceNr)
          retentionInProgress = false
        } else {
          internalLogger.debug("Retention, deleting events to seqNr [{}] was successful.", deleteToSequenceNr)
        }
      case _ =>
    }
  }

  def retentionProgressDeleteSnapshotsStarted(deleteToSequenceNr: Long): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, _) if retentionInProgress =>
        if (deleteToSequenceNr > 0) {
          internalLogger.debug("Retention, deleting snapshots to seqNr [{}].", deleteToSequenceNr)
        } else {
          internalLogger.debug("Retention is completed, no snapshots to delete.")
          retentionInProgress = false
        }
      case _ =>
    }
  }

  def retentionProgressDeleteSnapshotsEnded(deleteToSequenceNr: Long, success: Boolean): Unit = {
    retention match {
      case SnapshotCountRetentionCriteriaImpl(_, _, _) if retentionInProgress =>
        if (success) {
          // delete snapshot is last step => done
          internalLogger.debug(
            "Retention is completed, deleting snapshots to seqNr [{}] was successful.",
            deleteToSequenceNr)
          retentionInProgress = false
        } else {
          internalLogger.debug("Retention is completed, deleting snapshots to seqNr [{}] failed.", deleteToSequenceNr)
          retentionInProgress = false
        }

      case _ =>
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object PersistenceMdc {
  // format: OFF
  val Initializing      = "initializing"
  val AwaitingPermit    = "get-permit"
  val ReplayingSnapshot = "load-snap"
  val ReplayingEvents   = "replay-evt"
  val RunningCmds       = "running-cmd"
  val PersistingEvents  = "persist-evt"
  val WaitingAsyncEffect = "async-effect"
  val AsyncReplicationIntercept = "repl-intercept"
  val StoringSnapshot   = "storing-snap"
  // format: ON

  val PersistencePhaseKey = "persistencePhase"
  val PersistenceIdKey = "persistenceId"

  // MDC is cleared (if used) from aroundReceive in ActorAdapter after processing each message,
  // but important to call `context.log` to mark MDC as used
  def setMdc(persistenceId: PersistenceId, phase: String): Unit = {
    MDC.put(PersistenceIdKey, persistenceId.id)
    MDC.put(PersistencePhaseKey, phase)
  }

}
