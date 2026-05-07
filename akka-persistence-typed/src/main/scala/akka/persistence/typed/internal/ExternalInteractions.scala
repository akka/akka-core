/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.collection.immutable

import akka.actor.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.PreRestart
import akka.actor.typed.Signal
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence._
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.util.OptionVal

/** INTERNAL API */
@InternalApi
private[akka] object JournalInteractions {

  type EventOrTaggedOrReplicated = Any // `Any` since can be `E` or `Tagged` or a `ReplicatedEvent`

  final case class EventToPersist(adaptedEvent: EventOrTaggedOrReplicated, manifest: String, metadata: Option[Any])

}

/** INTERNAL API */
@InternalApi
private[akka] trait JournalInteractions[C, E, S] {

  import JournalInteractions._

  def setup: BehaviorSetup[C, E, S]

  protected def internalPersist(
      cmd: OptionVal[Any],
      state: Running.RunningState[S],
      event: EventOrTaggedOrReplicated,
      eventAdapterManifest: String,
      metadata: Option[Any]): Running.RunningState[S] = {

    val newRunningState = state.nextSequenceNr()

    val repr = PersistentRepr(
      event,
      persistenceId = setup.persistenceId.id,
      sequenceNr = newRunningState.seqNr,
      manifest = eventAdapterManifest,
      writerUuid = setup.writerIdentity.writerUuid,
      sender = ActorRef.noSender)

    val instrumentationContext =
      setup.instrumentation.persistEventCalled(setup.context.self, repr.payload, cmd.orNull)

    onWriteInitiated(setup.context, cmd.orNull, repr)

    val reprWithMetadata = metadata match {
      case None    => repr
      case Some(m) => repr.withMetadata(m)
    }

    val write = AtomicWrite(reprWithMetadata) :: Nil

    setup.journal
      .tell(JournalProtocol.WriteMessages(write, setup.selfClassic, setup.writerIdentity.instanceId), setup.selfClassic)

    newRunningState.updateInstrumentationContext(repr.sequenceNr, instrumentationContext)
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteInitiated(ctx: ActorContext[_], cmd: Any, repr: PersistentRepr): Unit = ()

  protected def internalPersistAll(
      cmd: OptionVal[Any],
      state: Running.RunningState[S],
      events: immutable.Seq[EventToPersist]): Running.RunningState[S] = {
    if (events.nonEmpty) {
      var newState = state

      val writes = events.map {
        case EventToPersist(event, eventAdapterManifest, metadata) =>
          newState = newState.nextSequenceNr()
          val repr = PersistentRepr(
            event,
            persistenceId = setup.persistenceId.id,
            sequenceNr = newState.seqNr,
            manifest = eventAdapterManifest,
            writerUuid = setup.writerIdentity.writerUuid,
            sender = ActorRef.noSender)
          val instCtx = setup.instrumentation.persistEventCalled(setup.context.self, repr.payload, cmd.orNull)
          newState = newState.updateInstrumentationContext(repr.sequenceNr, instCtx)

          metadata match {
            case None    => repr
            case Some(m) => repr.withMetadata(m)
          }
      }

      onWritesInitiated(setup.context, cmd.orNull, writes)
      val write = AtomicWrite(writes)

      setup.journal.tell(
        JournalProtocol.WriteMessages(write :: Nil, setup.selfClassic, setup.writerIdentity.instanceId),
        setup.selfClassic)

      newState
    } else state
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWritesInitiated(ctx: ActorContext[_], cmd: Any, repr: immutable.Seq[PersistentRepr]): Unit = ()

  protected def replayEvents(fromSeqNr: Long, toSeqNr: Long): Unit = {
    val from =
      if (setup.recovery == ReplayOnlyLastRecovery) {
        setup.internalLogger.debug("Recovery from last event only.")
        -1L
      } else {
        setup.internalLogger.debug("Replaying events: from: {}, to: {}", fromSeqNr, toSeqNr)
        fromSeqNr
      }

    setup.journal.tell(
      ReplayMessages(from, toSeqNr, setup.recovery.toClassic.replayMax, setup.persistenceId.id, setup.selfClassic),
      setup.selfClassic)
  }

  protected def requestRecoveryPermit(): Unit = {
    setup.persistence.recoveryPermitter.tell(RecoveryPermitter.RequestRecoveryPermit, setup.selfClassic)
  }

  /** Intended to be used in .onSignal(returnPermitOnStop) by behaviors */
  protected def returnPermitOnStop
      : PartialFunction[(ActorContext[InternalProtocol], Signal), Behavior[InternalProtocol]] = {
    case (_, PostStop) =>
      tryReturnRecoveryPermit("PostStop")
      Behaviors.stopped
    case (_, PreRestart) =>
      tryReturnRecoveryPermit("PreRestart")
      Behaviors.stopped
  }

  /** Mutates setup, by setting the `holdingRecoveryPermit` to false */
  protected def tryReturnRecoveryPermit(reason: String): Unit = {
    if (setup.holdingRecoveryPermit) {
      setup.internalLogger.debug("Returning recovery permit, reason: {}", reason)
      setup.persistence.recoveryPermitter.tell(RecoveryPermitter.ReturnRecoveryPermit, setup.selfClassic)
      setup.holdingRecoveryPermit = false
    } // else, no need to return the permit
  }

  /**
   * On [[akka.persistence.SaveSnapshotSuccess]], if `SnapshotCountRetentionCriteria.deleteEventsOnSnapshot`
   * is enabled, old messages are deleted based on `SnapshotCountRetentionCriteria.snapshotEveryNEvents`
   * before old snapshots are deleted.
   */
  protected def internalDeleteEvents(lastSequenceNr: Long, toSequenceNr: Long): Unit = {
    if (setup.isSnapshotOptional) {
      setup.internalLogger.warn(
        "Delete events shouldn't be used together with snapshot-is-optional=true. " +
        "That can result in wrong recovered state if snapshot load fails.")
    }
    if (toSequenceNr > 0) {
      val self = setup.selfClassic

      if (toSequenceNr == Long.MaxValue || toSequenceNr <= lastSequenceNr) {
        setup.internalLogger.debug("Deleting events up to sequenceNr [{}]", toSequenceNr)
        setup.journal.tell(JournalProtocol.DeleteMessagesTo(setup.persistenceId.id, toSequenceNr, self), self)
      } else
        self ! DeleteMessagesFailure(
          new RuntimeException(
            s"toSequenceNr [$toSequenceNr] must be less than or equal to lastSequenceNr [$lastSequenceNr]"),
          toSequenceNr)
    }
  }
}

/** INTERNAL API */
@InternalApi
private[akka] trait SnapshotInteractions[C, E, S] {

  def setup: BehaviorSetup[C, E, S]

  /**
   * Instructs the snapshot store to load the specified snapshot and send it via an [[SnapshotOffer]]
   * to the running [[PersistentActor]].
   */
  protected def loadSnapshot(criteria: SnapshotSelectionCriteria, toSequenceNr: Long): Unit = {
    setup.snapshotStore.tell(LoadSnapshot(setup.persistenceId.id, criteria, toSequenceNr), setup.selfClassic)
  }

  protected def internalSaveSnapshot(state: Running.RunningState[S], metadata: Option[Any]): Unit = {
    setup.internalLogger.debug("Saving snapshot sequenceNr [{}]", state.seqNr)
    if (state.state == null)
      throw new IllegalStateException("A snapshot must not be a null state.")
    else {
      val replicatedSnapshotMetadata = setup.replication match {
        case Some(_) => Some(ReplicatedSnapshotMetadata(state.version, state.seenPerReplica))
        case None    => None
      }
      val newMetadata = metadata match {
        case None =>
          replicatedSnapshotMetadata
        case Some(CompositeMetadata(entries)) =>
          Some(CompositeMetadata(replicatedSnapshotMetadata.toSeq ++ entries))
        case Some(other) if replicatedSnapshotMetadata.isEmpty =>
          Some(other)
        case Some(other) =>
          Some(CompositeMetadata(replicatedSnapshotMetadata.toSeq :+ other))
      }

      setup.snapshotStore.tell(
        SnapshotProtocol.SaveSnapshot(
          new SnapshotMetadata(setup.persistenceId.id, state.seqNr, newMetadata),
          setup.snapshotAdapter.toJournal(state.state)),
        setup.selfClassic)
    }
  }

  /** Deletes the snapshots up to and including the `sequenceNr`. */
  protected def internalDeleteSnapshots(toSequenceNr: Long): Unit = {
    if (toSequenceNr > 0) {
      val snapshotCriteria = SnapshotSelectionCriteria(minSequenceNr = 0L, maxSequenceNr = toSequenceNr)
      setup.internalLogger.debug("Deleting snapshots to sequenceNr [{}]", toSequenceNr)
      setup.snapshotStore
        .tell(SnapshotProtocol.DeleteSnapshots(setup.persistenceId.id, snapshotCriteria), setup.selfClassic)
    }
  }
}
