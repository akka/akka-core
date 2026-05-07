/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorRefOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalStableApi
import akka.pattern.StatusReply
import akka.persistence.AtomicWrite
import akka.persistence.FilteredPayload
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object EventWriterExtension extends ExtensionId[EventWriterExtension] {
  def createExtension(system: ActorSystem[_]): EventWriterExtension = new EventWriterExtension(system)

  def get(system: ActorSystem[_]): EventWriterExtension = apply(system)
}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] class EventWriterExtension(system: ActorSystem[_]) extends Extension {

  private val settings = EventWriter.EventWriterSettings(system)
  private val writersPerJournalId = new ConcurrentHashMap[String, ActorRef[EventWriter.Command]]()

  def writerForJournal(journalId: Option[String]): ActorRef[EventWriter.Command] =
    writersPerJournalId.computeIfAbsent(
      journalId.getOrElse(""), { _ =>
        system.systemActorOf(
          EventWriter(journalId.getOrElse(""), settings),
          s"EventWriter-${URLEncoder.encode(journalId.getOrElse("default"), "UTF-8")}")
      })

}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object EventWriter {

  type SeqNr = Long
  type Pid = String

  object EventWriterSettings {
    def apply(system: ActorSystem[_]): EventWriterSettings = {
      val config = system.settings.config.getConfig("akka.persistence.typed.event-writer")
      EventWriterSettings(
        maxBatchSize = config.getInt("max-batch-size"),
        askTimeout = config.getDuration("ask-timeout").toScala,
        latestSequenceNumberCacheCapacity = config.getInt("latest-sequence-number-cache-capacity"))
    }

  }
  final case class EventWriterSettings(
      maxBatchSize: Int,
      askTimeout: FiniteDuration,
      latestSequenceNumberCacheCapacity: Int)

  sealed trait Command
  final case class Write(
      persistenceId: Pid,
      sequenceNumber: SeqNr,
      event: Any,
      isSnapshotEvent: Boolean,
      fillSequenceNumberGaps: Boolean,
      metadata: Option[Any],
      tags: Set[String],
      replyTo: ActorRef[StatusReply[WriteAck]])
      extends Command
  final case class WriteAck(persistenceId: Pid, sequenceNumber: SeqNr)

  private final case class MaxSeqNrForPid(persistenceId: Pid, sequenceNumber: SeqNr, reason: AskMaxSeqNrReason)
      extends Command
  private final case class MaxSeqNrForPidFailed(persistenceId: Pid, reason: AskMaxSeqNrReason) extends Command

  private sealed trait AskMaxSeqNrReason
  private object AskMaxSeqNrReason {
    final case class WriteFailure(originalErrorDesc: String) extends AskMaxSeqNrReason
    case object FillGaps extends AskMaxSeqNrReason
    case object SnapshotEvent extends AskMaxSeqNrReason
  }

  private case class StateForPid(
      waitingForReply: Map[SeqNr, (PersistentRepr, ActorRef[StatusReply[WriteAck]])],
      waitingForWrite: Vector[(PersistentRepr, ActorRef[StatusReply[WriteAck]])] = Vector.empty,
      writeErrorHandlingInProgress: Boolean = false,
      currentTransactionId: Int = 0,
      latestSeqNr: SeqNr = -1L,
      fillSequenceNumberGaps: Boolean,
      usedTimestamp: Long = 0L,
      waitingForSeqNrLookup: Vector[(PersistentRepr, ActorRef[StatusReply[WriteAck]])] = Vector.empty) {
    def idle: Boolean =
      waitingForReply.isEmpty && waitingForWrite.isEmpty

    def seqNrlookupInProgress: Boolean =
      waitingForSeqNrLookup.nonEmpty

    def nextExpectedSeqNr: SeqNr =
      if (waitingForWrite.nonEmpty)
        waitingForWrite.last._1.sequenceNr + 1
      else if (waitingForReply.nonEmpty)
        waitingForReply.keysIterator.max + 1
      else if (latestSeqNr == -1L)
        1L
      else
        latestSeqNr + 1

    def waitingForWriteExceedingMaxBatchSize(batchSize: Int): Boolean =
      waitingForWrite.size >= batchSize &&
      waitingForWrite.count { case (repr, _) => repr.payload != FilteredPayload } >= batchSize
  }

  def apply(journalPluginId: String, settings: EventWriterSettings): Behavior[Command] =
    Behaviors
      .supervise(Behaviors.setup[Command] { context =>
        val journal = Persistence(context.system).journalFor(journalPluginId)
        context.log.debug("Event writer for journal [{}] starting up", journalPluginId)
        apply(journal.toTyped, settings)
      })
      .onFailure[Exception](SupervisorStrategy.restart)

  def apply(journal: ActorRef[JournalProtocol.Message], settings: EventWriterSettings): Behavior[Command] =
    Behaviors
      .setup[AnyRef] { context =>
        val writerUuid = UUID.randomUUID().toString

        var perPidWriteState = Map.empty[Pid, StateForPid]
        var bypassCircuitBreaker = true // otherwise the duplicate key violations will flip the circuit breaker

        implicit val askTimeout: Timeout = settings.askTimeout

        def sendToJournal(transactionId: Int, reprs: Vector[PersistentRepr]): Unit = {
          if (context.log.isTraceEnabled)
            context.log.trace(
              "Writing events persistence id [{}], sequence nrs [{}-{}]",
              reprs.head.persistenceId,
              reprs.head.sequenceNr,
              reprs.last.sequenceNr)
          journal ! JournalProtocol.WriteMessages(
            AtomicWrite(reprs) :: Nil,
            context.self.toClassic,
            // Note: we use actorInstanceId to correlate replies from one write request
            actorInstanceId = transactionId,
            bypassCircuitBreaker)
        }

        def handleUpdatedStateForPid(pid: Pid, newStateForPid: StateForPid): Unit = {
          if (newStateForPid.waitingForReply.nonEmpty) {
            // more waiting replyTo before we could batch it or scrap the entry
            perPidWriteState = perPidWriteState.updated(pid, newStateForPid)
          } else {
            // note that cache eviction is different for fillSequenceNumberGaps and isSnapshotEvent
            if (newStateForPid.waitingForWrite.isEmpty && newStateForPid.fillSequenceNumberGaps) {
              perPidWriteState = perPidWriteState.updated(pid, newStateForPid)
              evictLeastRecentlyUsedPids()
            } else if (newStateForPid.waitingForWrite.isEmpty) {
              perPidWriteState = perPidWriteState - pid
            } else {
              // batch waiting for pid
              val newReplyTo = newStateForPid.waitingForWrite.map {
                case (repr, replyTo) => (repr.sequenceNr, (repr, replyTo))
              }.toMap
              val updatedState =
                newStateForPid.copy(
                  newReplyTo,
                  waitingForWrite = Vector.empty,
                  currentTransactionId = newStateForPid.currentTransactionId + 1)

              if (context.log.isTraceEnabled())
                context.log.trace(
                  "Writing batch of {} events for pid [{}], seq nrs [{}-{}], tx id [{}]",
                  newStateForPid.waitingForWrite.size,
                  pid,
                  newStateForPid.waitingForWrite.head._1.sequenceNr,
                  newStateForPid.waitingForWrite.last._1.sequenceNr,
                  updatedState.currentTransactionId)

              val batch = newStateForPid.waitingForWrite.map { case (repr, _) => repr }
              sendToJournal(updatedState.currentTransactionId, batch)
              perPidWriteState = perPidWriteState.updated(pid, updatedState)

            }
          }
        }

        def askMaxSeqNr(pid: Pid, reason: AskMaxSeqNrReason): Unit = {
          context.log.trace("Request highest sequence number for pid [{}]", pid)
          context.ask(
            journal,
            (replyTo: ActorRef[JournalProtocol.Response]) =>
              JournalProtocol.ReplayMessages(0L, 0L, 1L, pid, replyTo.toClassic)) {
            case Success(JournalProtocol.RecoverySuccess(highestSequenceNr)) =>
              MaxSeqNrForPid(pid, highestSequenceNr, reason)
            case Success(JournalProtocol.ReplayMessagesFailure(_)) =>
              MaxSeqNrForPidFailed(pid, reason)
            case Failure(_) =>
              MaxSeqNrForPidFailed(pid, reason)
            case Success(unexpected) =>
              throw new IllegalArgumentException(
                s"Got unexpected reply from journal ${unexpected.getClass} for pid $pid. " +
                s"original reason: $reason")
          }
        }

        def evictLeastRecentlyUsedPids(): Unit = {
          import settings.latestSequenceNumberCacheCapacity
          val accumulationFactor = 1.1
          if (perPidWriteState.size >= latestSequenceNumberCacheCapacity * accumulationFactor) {
            val idleEntries =
              perPidWriteState.iterator.filter {
                case (_, stateForPid) => stateForPid.idle && stateForPid.waitingForSeqNrLookup.isEmpty
              }.toVector

            if (idleEntries.size >= latestSequenceNumberCacheCapacity * accumulationFactor) {
              val pidsToRemove =
                idleEntries
                  .sortBy {
                    case (_, stateForPid) => stateForPid.usedTimestamp
                  }
                  .take(idleEntries.size - latestSequenceNumberCacheCapacity)
                  .map { case (pid, _) => pid }

              if (context.log.isTraceEnabled)
                context.log.trace(
                  "Evicted cache from [{}] to [{}], persistence ids [{}]",
                  idleEntries.size,
                  idleEntries.size - pidsToRemove.size,
                  pidsToRemove.mkString(", "))

              perPidWriteState --= pidsToRemove
            }
          }
        }

        def handleJournalResponse(response: JournalProtocol.Response): Behavior[AnyRef] =
          response match {
            case JournalProtocol.WriteMessageSuccess(message, transactionId) =>
              bypassCircuitBreaker = true
              val pid = message.persistenceId
              val sequenceNr = message.sequenceNr
              perPidWriteState.get(pid) match {
                case None =>
                  context.log.debug(
                    "Got write success reply for event with no previous state for pid, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                    pid,
                    sequenceNr,
                    transactionId)
                case Some(stateForPid) =>
                  if (transactionId == stateForPid.currentTransactionId) {
                    stateForPid.waitingForReply.get(sequenceNr) match {
                      case None =>
                        context.log.debug(
                          "Got write reply for event with no waiting request for seq nr, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                          pid,
                          sequenceNr,
                          transactionId)
                      case Some((_, waiting)) =>
                        context.log.trace(
                          "Successfully wrote event persistence id [{}], sequence nr [{}]",
                          pid,
                          sequenceNr)
                        waiting ! StatusReply.success(WriteAck(pid, sequenceNr))
                        val newState = stateForPid.copy(
                          waitingForReply = stateForPid.waitingForReply - sequenceNr,
                          latestSeqNr = sequenceNr,
                          usedTimestamp = System.currentTimeMillis())
                        handleUpdatedStateForPid(pid, newState)
                    }
                  } else {
                    if (context.log.isTraceEnabled) {
                      context.log.trace(
                        "Got reply for old tx id [{}] (current [{}]) for pid [{}], ignoring",
                        transactionId,
                        stateForPid.currentTransactionId,
                        pid)
                    }
                  }
              }
              Behaviors.same

            case JournalProtocol.WriteMessageFailure(message, error, transactionId) =>
              val pid = message.persistenceId
              val sequenceNr = message.sequenceNr
              perPidWriteState.get(pid) match {
                case None =>
                  context.log.debug(
                    "Got write error reply for event with no previous state for pid, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                    pid,
                    sequenceNr,
                    transactionId)
                case Some(state) =>
                  // write failure could be re-delivery, we need to check
                  state.waitingForReply.get(sequenceNr) match {
                    case None =>
                      context.log.debug(
                        "Got write error reply for event with no waiting request for seq nr, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                        pid,
                        sequenceNr,
                        transactionId)
                    case Some(_) =>
                      // quite likely a re-delivery of already persisted events, the whole batch will fail
                      // check highest seqnr and see if we can ack events
                      if (!state.writeErrorHandlingInProgress && state.currentTransactionId == transactionId) {
                        perPidWriteState =
                          perPidWriteState.updated(pid, state.copy(writeErrorHandlingInProgress = true))
                        askMaxSeqNr(pid, AskMaxSeqNrReason.WriteFailure(error.getMessage))
                      } else {
                        context.log.trace(
                          "Ignoring failure for pid [{}], seq nr [{}], tx id [{}], since write error handling already in progress or old tx id (current tx id [{}])",
                          pid,
                          sequenceNr,
                          transactionId,
                          state.currentTransactionId)
                      }
                  }
              }
              Behaviors.same

            case _ =>
              // ignore all other journal protocol messages
              Behaviors.same
          }

        def handleWrite(
            repr: PersistentRepr,
            replyTo: ActorRef[StatusReply[WriteAck]],
            isSnapshotEvent: Boolean,
            fillSequenceNumberGaps: Boolean,
            calledAfterMaxSeqNr: Boolean): Unit = {
          val persistenceId = repr.persistenceId
          val sequenceNumber = repr.sequenceNr
          val newStateForPid =
            perPidWriteState.get(persistenceId) match {
              case None =>
                if ((fillSequenceNumberGaps || isSnapshotEvent) && sequenceNumber != 1L) {
                  val reason = if (isSnapshotEvent) AskMaxSeqNrReason.SnapshotEvent else AskMaxSeqNrReason.FillGaps
                  askMaxSeqNr(persistenceId, reason)
                  StateForPid(
                    waitingForReply = Map.empty,
                    waitingForSeqNrLookup = Vector((repr, replyTo)),
                    fillSequenceNumberGaps = fillSequenceNumberGaps)
                } else {
                  sendToJournal(1, Vector(repr))
                  StateForPid(
                    Map((repr.sequenceNr, (repr, replyTo))),
                    currentTransactionId = 1,
                    fillSequenceNumberGaps = fillSequenceNumberGaps)
                }

              case Some(state) =>
                // For a given persistenceId the fillSequenceNumberGaps is not supposed to change,
                // but we keep `true` if it has been set to `true`.
                // isSnapshotEvent and fillSequenceNumberGaps are handled in very similar way, but there is a
                // difference in the way latest sequence number is cached (see handleUpdatedStateForPid).
                val newFillSequenceNumberGaps = fillSequenceNumberGaps || state.fillSequenceNumberGaps
                val expectedSeqNr =
                  if (newFillSequenceNumberGaps || isSnapshotEvent) state.nextExpectedSeqNr
                  else repr.sequenceNr

                if (state.seqNrlookupInProgress) {
                  context.log.trace(
                    "Seq nr lookup in progress for persistence id [{}], adding sequence nr [{}] to pending",
                    persistenceId,
                    sequenceNumber)
                  state.copy(
                    waitingForSeqNrLookup = state.waitingForSeqNrLookup :+ ((repr, replyTo)),
                    fillSequenceNumberGaps = newFillSequenceNumberGaps)
                } else if (sequenceNumber == expectedSeqNr) {
                  if (state.idle) {
                    sendToJournal(state.currentTransactionId + 1, Vector(repr))
                    state.copy(
                      waitingForReply = Map((repr.sequenceNr, (repr, replyTo))),
                      currentTransactionId = state.currentTransactionId + 1,
                      fillSequenceNumberGaps = newFillSequenceNumberGaps)
                  } else {
                    // write in progress for pid, add write to batch and perform once current write completes
                    if (state.waitingForWriteExceedingMaxBatchSize(settings.maxBatchSize)) {
                      replyTo ! StatusReply.error(
                        s"Max batch reached for pid $persistenceId, at most ${settings.maxBatchSize} writes for " +
                        "the same pid may be in flight at the same time")
                      state
                    } else {
                      context.log.trace(
                        "Writing event in progress for persistence id [{}], adding sequence nr [{}] to batch",
                        persistenceId,
                        sequenceNumber)
                      state.copy(
                        waitingForWrite = state.waitingForWrite :+ ((repr, replyTo)),
                        fillSequenceNumberGaps = newFillSequenceNumberGaps)
                    }
                  }
                } else if (sequenceNumber < expectedSeqNr) {
                  context.log.trace("Duplicate seq nr [{}] for persistence id [{}]", sequenceNumber, persistenceId)
                  replyTo ! StatusReply.success(WriteAck(persistenceId, sequenceNumber)) // duplicate
                  state
                } else { // sequenceNumber > expectedSeqNr
                  require(
                    newFillSequenceNumberGaps || isSnapshotEvent,
                    s"Unexpected sequence number gap, expected [$expectedSeqNr], received [$sequenceNumber]. " +
                    "Enable akka.persistence.typed.event-writer.fill-sequence-number-gaps config if gaps are " +
                    "expected and should be filled with FilteredPayload.")

                  if (calledAfterMaxSeqNr || !state.idle) {
                    // here we can trust that the expectedSeqNr is correct and don't have to lookup max
                    val fillRepr = (expectedSeqNr until sequenceNumber).map { n =>
                      PersistentRepr(
                        FilteredPayload,
                        persistenceId = persistenceId,
                        sequenceNr = n,
                        manifest = "", // adapters would be on the producing side, already applied
                        writerUuid = writerUuid,
                        sender = akka.actor.ActorRef.noSender)
                    }.toVector
                    context.log.debug(
                      "Detected sequence nr gap{} [{}-{}] for persistence id [{}]. Filling with FilteredPayload.",
                      if (isSnapshotEvent) " for snapshot event" else "",
                      fillRepr.head.sequenceNr,
                      fillRepr.last.sequenceNr,
                      persistenceId)
                    val ignoreRef = context.system.ignoreRef
                    if (state.idle) {
                      sendToJournal(state.currentTransactionId + 1, fillRepr :+ repr)
                      val newWaitingForReply =
                        (fillRepr.map(r => r.sequenceNr -> (r -> ignoreRef)) :+ (repr.sequenceNr -> (repr -> replyTo))).toMap
                      state.copy(
                        waitingForReply = newWaitingForReply,
                        currentTransactionId = state.currentTransactionId + 1,
                        fillSequenceNumberGaps = newFillSequenceNumberGaps)
                    } else {
                      if (context.log.isTraceEnabled)
                        context.log.trace(
                          "Writing event in progress for persistence id [{}], adding sequence nrs [{}-{}] to batch",
                          persistenceId,
                          fillRepr.head.sequenceNr,
                          sequenceNumber)
                      val newWaitingForWrite = fillRepr.map(_ -> ignoreRef) :+ (repr -> replyTo)
                      state.copy(
                        waitingForWrite = state.waitingForWrite ++ newWaitingForWrite,
                        fillSequenceNumberGaps = newFillSequenceNumberGaps)
                    }
                  } else {
                    // No pending writes or we haven't just looked up latest sequence nr.
                    // The reason we don't trust a previous state.latestSeqNr is because another EventWriter
                    // on another node might have been taking writes since we retrieved it.
                    val reason = if (isSnapshotEvent) AskMaxSeqNrReason.SnapshotEvent else AskMaxSeqNrReason.FillGaps
                    askMaxSeqNr(persistenceId, reason)
                    context.log.trace(
                      "Seq nr lookup needed for persistence id [{}], adding sequence nr [{}] to pending",
                      persistenceId,
                      sequenceNumber)
                    state.copy(
                      waitingForSeqNrLookup = state.waitingForSeqNrLookup :+ ((repr, replyTo)),
                      fillSequenceNumberGaps = newFillSequenceNumberGaps)
                  }

                }
            }
          perPidWriteState = perPidWriteState.updated(persistenceId, newStateForPid)
        }

        Behaviors.receiveMessage {
          case Write(
              persistenceId,
              sequenceNumber,
              event,
              isSnapshotEvent,
              fillSequenceNumberGaps,
              metadata,
              tags,
              replyTo) =>
            val payload = if (tags.isEmpty) event else Tagged(event, tags)
            val repr = PersistentRepr(
              payload,
              persistenceId = persistenceId,
              sequenceNr = sequenceNumber,
              manifest = "", // adapters would be on the producing side, already applied
              writerUuid = writerUuid,
              sender = akka.actor.ActorRef.noSender)

            val reprWithMeta = metadata match {
              case Some(meta) => repr.withMetadata(meta)
              case _          => repr
            }

            handleWrite(reprWithMeta, replyTo, isSnapshotEvent, fillSequenceNumberGaps, calledAfterMaxSeqNr = false)
            Behaviors.same

          case MaxSeqNrForPid(pid, maxSeqNr, AskMaxSeqNrReason.WriteFailure(errorDesc)) =>
            bypassCircuitBreaker = true
            // write failed, so we looked up the maxSeqNr to detect if it was duplicate events, already in journal
            perPidWriteState.get(pid) match {
              case None =>
                context.log.debug(
                  "Got max seq nr with no waiting previous state for pid, ignoring (pid [{}], original error desc: {})",
                  pid,
                  errorDesc)
              case Some(state) =>
                val sortedSeqs = state.waitingForReply.keys.toSeq.sorted
                val (alreadyInJournal, needsWrite) = sortedSeqs.partition(seqNr => seqNr <= maxSeqNr)
                if (alreadyInJournal.isEmpty) {
                  // error was not about duplicates
                  state.waitingForReply.values.foreach {
                    case (_, replyTo) =>
                      replyTo ! StatusReply.error("Journal write failed")
                  }
                  context.log.warn(
                    "Failed writing event batch persistence id [{}], sequence nr [{}-{}]: {}",
                    pid,
                    sortedSeqs.head,
                    sortedSeqs.last,
                    errorDesc)
                  val newState = state.copy(waitingForReply = Map.empty, writeErrorHandlingInProgress = false)
                  handleUpdatedStateForPid(pid, newState)
                } else {
                  // ack all already written
                  val stateAfterWritten = alreadyInJournal
                    .foldLeft(state) { (state, seqNr) =>
                      val (_, replyTo) = state.waitingForReply(seqNr)
                      replyTo ! StatusReply.success(WriteAck(pid, seqNr))
                      state.copy(waitingForReply = state.waitingForReply - seqNr)
                    }
                    .copy(writeErrorHandlingInProgress = false)
                  if (needsWrite.isEmpty) {
                    handleUpdatedStateForPid(pid, stateAfterWritten)
                  } else {
                    // retrigger write for those left if any, note that we do a
                    val reprsToRewrite =
                      stateAfterWritten.waitingForReply.values.map { case (repr, _) => repr }.toVector
                    if (context.log.isDebugEnabled())
                      context.log.debug(
                        "Partial batch was duplicates, re-triggering write of persistence id [{}], sequence nr [{}-{}]",
                        pid,
                        reprsToRewrite.head.sequenceNr,
                        reprsToRewrite.last.sequenceNr)
                    // Not going via batch/handleUpdatedStateForPid here because adding partial
                    // failure to current waiting could go over batch size limit
                    val partialRewriteState =
                      stateAfterWritten.copy(currentTransactionId = stateAfterWritten.currentTransactionId + 1)
                    sendToJournal(partialRewriteState.currentTransactionId, reprsToRewrite)
                    handleUpdatedStateForPid(pid, partialRewriteState)
                  }

                }

            }
            Behaviors.same

          case MaxSeqNrForPid(pid, maxSeqNr, reason @ (AskMaxSeqNrReason.FillGaps | AskMaxSeqNrReason.SnapshotEvent)) =>
            bypassCircuitBreaker = true
            perPidWriteState.get(pid) match {
              case None =>
                context.log.debug("Got max seq nr with no waiting previous state for pid, ignoring (pid [{}])", pid)
              case Some(state) =>
                context.log.debug("Retrieved max seq nr [{}] for pid [{}]", maxSeqNr, pid)
                val waiting = state.waitingForSeqNrLookup
                perPidWriteState = perPidWriteState.updated(
                  pid,
                  state.copy(latestSeqNr = maxSeqNr, waitingForSeqNrLookup = Vector.empty))
                waiting.foreach {
                  case (repr, replyTo) =>
                    handleWrite(
                      repr,
                      replyTo,
                      isSnapshotEvent = reason == AskMaxSeqNrReason.SnapshotEvent,
                      fillSequenceNumberGaps = reason == AskMaxSeqNrReason.FillGaps,
                      calledAfterMaxSeqNr = true)
                }
            }
            Behaviors.same

          case MaxSeqNrForPidFailed(pid, errorDescOpt) =>
            bypassCircuitBreaker = false
            def errorDescInLog = errorDescOpt match {
              case AskMaxSeqNrReason.WriteFailure(errorDesc) => s", original error desc: $errorDesc"
              case _                                         => ""
            }
            perPidWriteState.get(pid) match {
              case None =>
                context.log.debug(
                  "Got max seq nr (failed) with no waiting previous state for persistence id [{}]{}",
                  pid,
                  errorDescInLog)
              case Some(state) =>
                state.waitingForReply.values.foreach {
                  case (_, replyTo) =>
                    replyTo ! StatusReply.error("Journal write failed")
                }
                val sortedSeqs = state.waitingForReply.keys.toSeq.sorted
                context.log.warn(
                  "Failed max seq nr, and therefore failed writing event batch persistence id [{}], sequence nr [{}-{}]{}",
                  pid,
                  sortedSeqs.head,
                  sortedSeqs.last,
                  errorDescInLog)
                val newState = state.copy(waitingForReply = Map.empty, writeErrorHandlingInProgress = false)
                handleUpdatedStateForPid(pid, newState)
            }
            Behaviors.same

          case response: JournalProtocol.Response => handleJournalResponse(response)

          case unexpected =>
            context.log.warn("Unexpected message sent to EventWriter [{}], ignored", unexpected.getClass)
            Behaviors.same

        }
      }
      .narrow[Command]

}
