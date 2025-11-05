/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.typed.internal

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.ClassicActorRefOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalStableApi
import akka.pattern.StatusReply
import akka.persistence.AtomicWrite
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.util.JavaDurationConverters.JavaDurationOps
import akka.util.Timeout

import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

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
    def apply(system: ActorSystem[_]): EventWriterSettings =
      EventWriterSettings(
        maxBatchSize = system.settings.config.getInt("akka.persistence.typed.event-writer.max-batch-size"),
        askTimeout = system.settings.config.getDuration("akka.persistence.typed.event-writer.ask-timeout").asScala)

  }
  final case class EventWriterSettings(maxBatchSize: Int, askTimeout: FiniteDuration)

  sealed trait Command
  final case class Write(
      persistenceId: Pid,
      sequenceNumber: SeqNr,
      event: Any,
      metadata: Option[Any],
      tags: Set[String],
      replyTo: ActorRef[StatusReply[WriteAck]])
      extends Command
  final case class WriteAck(persistenceId: Pid, sequenceNumber: SeqNr)

  private case class MaxSeqNrForPid(persistenceId: Pid, sequenceNumber: SeqNr, originalErrorDesc: String)
      extends Command

  private def emptyWaitingForWrite = Vector.empty[(PersistentRepr, ActorRef[StatusReply[WriteAck]])]
  private case class StateForPid(
      waitingForReply: Map[SeqNr, (PersistentRepr, ActorRef[StatusReply[WriteAck]])],
      waitingForWrite: Vector[(PersistentRepr, ActorRef[StatusReply[WriteAck]])] = emptyWaitingForWrite,
      writeErrorHandlingInProgress: Boolean = false,
      currentTransactionId: Int = 0)

  def apply(journalPluginId: String, eventWriterSettings: EventWriterSettings): Behavior[Command] =
    Behaviors
      .supervise(Behaviors.setup[Command] { context =>
        val journal = Persistence(context.system).journalFor(journalPluginId)
        context.log.debug("Event writer for journal [{}] starting up", journalPluginId)
        apply(journal.toTyped, eventWriterSettings)
      })
      .onFailure[Exception](SupervisorStrategy.restart)

  def apply(journal: ActorRef[JournalProtocol.Message], eventWriterSettings: EventWriterSettings): Behavior[Command] =
    Behaviors
      .setup[AnyRef] { context =>
        val writerUuid = UUID.randomUUID().toString

        var perPidWriteState = Map.empty[Pid, StateForPid]
        implicit val askTimeout: Timeout = eventWriterSettings.askTimeout

        def sendToJournal(transactionId: Int, reprs: Vector[PersistentRepr]) = {
          journal ! JournalProtocol.WriteMessages(
            AtomicWrite(reprs) :: Nil,
            context.self.toClassic,
            // Note: we use actorInstanceId to correlate replies from one write request
            actorInstanceId = transactionId)
        }

        def handleUpdatedStateForPid(pid: Pid, newStateForPid: StateForPid): Unit = {
          if (newStateForPid.waitingForReply.nonEmpty) {
            // more waiting replyTo before we could batch it or scrap the entry
            perPidWriteState = perPidWriteState.updated(pid, newStateForPid)
          } else {
            if (newStateForPid.waitingForWrite.isEmpty) {
              perPidWriteState = perPidWriteState - pid
            } else {
              // batch waiting for pid
              val newReplyTo = newStateForPid.waitingForWrite.map {
                case (repr, replyTo) => (repr.sequenceNr, (repr, replyTo))
              }.toMap
              val updatedState =
                newStateForPid.copy(
                  newReplyTo,
                  emptyWaitingForWrite,
                  currentTransactionId = newStateForPid.currentTransactionId + 1)

              if (context.log.isTraceEnabled())
                context.log.traceN(
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

        def handleJournalResponse(response: JournalProtocol.Response): Behavior[AnyRef] =
          response match {
            case JournalProtocol.WriteMessageSuccess(message, transactionId) =>
              val pid = message.persistenceId
              val sequenceNr = message.sequenceNr
              perPidWriteState.get(pid) match {
                case None =>
                  context.log.debugN(
                    "Got write success reply for event with no previous state for pid, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                    pid,
                    sequenceNr,
                    transactionId)
                case Some(stateForPid) =>
                  if (transactionId == stateForPid.currentTransactionId) {
                    stateForPid.waitingForReply.get(sequenceNr) match {
                      case None =>
                        context.log.debugN(
                          "Got write error reply for event with no waiting request for seq nr, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                          pid,
                          sequenceNr,
                          transactionId)
                      case Some((_, waiting)) =>
                        context.log.trace2(
                          "Successfully wrote event persistence id [{}], sequence nr [{}]",
                          pid,
                          message.sequenceNr)
                        waiting ! StatusReply.success(WriteAck(pid, sequenceNr))
                        val newState = stateForPid.copy(waitingForReply = stateForPid.waitingForReply - sequenceNr)
                        handleUpdatedStateForPid(pid, newState)
                    }
                  } else {
                    if (context.log.isTraceEnabled) {
                      context.log.traceN(
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
                  context.log.debugN(
                    "Got write error reply for event with no previous state for pid, ignoring (pid [{}], seq nr [{}], tx id [{}])",
                    pid,
                    sequenceNr,
                    transactionId)
                case Some(state) =>
                  // write failure could be re-delivery, we need to check
                  state.waitingForReply.get(sequenceNr) match {
                    case None =>
                      context.log.debugN(
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
                        context.ask(
                          journal,
                          (replyTo: ActorRef[JournalProtocol.Response]) =>
                            JournalProtocol.ReplayMessages(0L, 0L, 1L, pid, replyTo.toClassic)) {
                          case Success(JournalProtocol.RecoverySuccess(highestSequenceNr)) =>
                            MaxSeqNrForPid(pid, highestSequenceNr, error.getMessage)
                          case Success(unexpected) =>
                            throw new IllegalArgumentException(
                              s"Got unexpected reply from journal ${unexpected.getClass} for pid $pid")
                          case Failure(exception) =>
                            throw new RuntimeException(
                              s"Error finding highest sequence number in journal for pid $pid",
                              exception)
                        }
                      } else {
                        context.log.traceN(
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

        Behaviors.receiveMessage {
          case Write(persistenceId, sequenceNumber, event, metadata, tags, replyTo) =>
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

            val newStateForPid =
              perPidWriteState.get(persistenceId) match {
                case None =>
                  if (context.log.isTraceEnabled)
                    context.log.traceN(
                      "Writing event persistence id [{}], sequence nr [{}], payload {}",
                      persistenceId,
                      sequenceNumber,
                      event)
                  sendToJournal(1, Vector(reprWithMeta))
                  StateForPid(
                    Map((reprWithMeta.sequenceNr, (reprWithMeta, replyTo))),
                    emptyWaitingForWrite,
                    currentTransactionId = 1)
                case Some(state) =>
                  // write in progress for pid, add write to batch and perform once current write completes
                  if (state.waitingForWrite.size == eventWriterSettings.maxBatchSize) {
                    replyTo ! StatusReply.error(
                      s"Max batch reached for pid $persistenceId, at most ${eventWriterSettings.maxBatchSize} writes for " +
                      "the same pid may be in flight at the same time")
                    state
                  } else {
                    if (context.log.isTraceEnabled)
                      context.log.traceN(
                        "Writing event in progress for persistence id [{}], adding sequence nr [{}], payload {} to batch",
                        persistenceId,
                        sequenceNumber,
                        event)
                    state.copy(waitingForWrite = state.waitingForWrite :+ ((reprWithMeta, replyTo)))
                  }
              }
            perPidWriteState = perPidWriteState.updated(persistenceId, newStateForPid)
            Behaviors.same

          case MaxSeqNrForPid(pid, maxSeqNr, originalErrorDesc) =>
            // write failed, so we looked up the maxSeqNr to detect if it was duplicate events, already in journal
            perPidWriteState.get(pid) match {
              case None =>
                context.log.debug2(
                  "Got max seq nr with no waiting previous state for pid, ignoring (pid [{}], original error desc: {})",
                  pid,
                  originalErrorDesc)
              case Some(state) =>
                val sortedSeqs = state.waitingForReply.keys.toSeq.sorted
                val (alreadyInJournal, needsWrite) = sortedSeqs.partition(seqNr => seqNr <= maxSeqNr)
                if (alreadyInJournal.isEmpty) {
                  // error was not about duplicates
                  state.waitingForReply.values.foreach {
                    case (_, replyTo) =>
                      replyTo ! StatusReply.error("Journal write failed")
                  }
                  context.log.warnN(
                    "Failed writing event batch persistence id [{}], sequence nr [{}-{}]: {}",
                    pid,
                    sortedSeqs.head,
                    sortedSeqs.last,
                    originalErrorDesc)
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
                      context.log.debugN(
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

          case response: JournalProtocol.Response => handleJournalResponse(response)

          case unexpected =>
            context.log.warn("Unexpected message sent to EventWriter [{}], ignored", unexpected.getClass)
            Behaviors.same

        }
      }
      .narrow[Command]

}
