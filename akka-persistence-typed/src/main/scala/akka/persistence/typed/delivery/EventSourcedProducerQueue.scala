/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.delivery

import java.time.{ Duration => JavaDuration }

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.ApiMayChange
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.delivery.EventSourcedProducerQueue.CleanupTick
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.RetentionCriteria

/**
 * [[DurableProducerQueue]] that can be used with [[akka.actor.typed.delivery.ProducerController]]
 * for reliable delivery of messages. It is implemented with Event Sourcing and stores one
 * event before sending the message to the destination and one event for the confirmation
 * that the message has been delivered and processed.
 *
 * The [[DurableProducerQueue.LoadState]] request is used at startup to retrieve the unconfirmed messages.
 */
@ApiMayChange
object EventSourcedProducerQueue {
  import DurableProducerQueue._

  object Settings {

    /**
     * Scala API: Factory method from config `akka.reliable-delivery.producer-controller.event-sourced-durable-queue`
     * of the `ActorSystem`.
     */
    def apply(system: ActorSystem[_]): Settings =
      apply(system.settings.config.getConfig("akka.reliable-delivery.producer-controller.event-sourced-durable-queue"))

    /**
     * Scala API: Factory method from Config corresponding to
     * `akka.reliable-delivery.producer-controller.event-sourced-durable-queue`.
     */
    def apply(config: Config): Settings = {
      new Settings(
        restartMaxBackoff = config.getDuration("restart-max-backoff").toScala,
        snapshotEvery = config.getInt("snapshot-every"),
        keepNSnapshots = config.getInt("keep-n-snapshots"),
        deleteEvents = config.getBoolean("delete-events"),
        cleanupUnusedAfter = config.getDuration("cleanup-unused-after").toScala,
        journalPluginId = config.getString("journal-plugin-id"),
        snapshotPluginId = config.getString("snapshot-plugin-id"))
    }

    /**
     * Java API: Factory method from config `akka.reliable-delivery.producer-controller.event-sourced-durable-queue`
     * of the `ActorSystem`.
     */
    def create(system: ActorSystem[_]): Settings =
      apply(system)

    /**
     * Java API: Factory method from Config corresponding to
     * `akka.reliable-delivery.producer-controller.event-sourced-durable-queue`.
     */
    def create(config: Config): Settings =
      apply(config)
  }

  final class Settings private (
      val restartMaxBackoff: FiniteDuration,
      val snapshotEvery: Int,
      val keepNSnapshots: Int,
      val deleteEvents: Boolean,
      val cleanupUnusedAfter: FiniteDuration,
      val journalPluginId: String,
      val snapshotPluginId: String) {

    def withSnapshotEvery(newSnapshotEvery: Int): Settings =
      copy(snapshotEvery = newSnapshotEvery)

    def withKeepNSnapshots(newKeepNSnapshots: Int): Settings =
      copy(keepNSnapshots = newKeepNSnapshots)

    def withDeleteEvents(newDeleteEvents: Boolean): Settings =
      copy(deleteEvents = newDeleteEvents)

    /**
     * Scala API
     */
    def withRestartMaxBackoff(newRestartMaxBackoff: FiniteDuration): Settings =
      copy(restartMaxBackoff = newRestartMaxBackoff)

    /**
     * Java API
     */
    def withRestartMaxBackoff(newRestartMaxBackoff: JavaDuration): Settings =
      copy(restartMaxBackoff = newRestartMaxBackoff.toScala)

    /**
     * Java API
     */
    def getRestartMaxBackoff(): JavaDuration =
      restartMaxBackoff.toJava

    /**
     * Scala API
     */
    def withCleanupUnusedAfter(newCleanupUnusedAfter: FiniteDuration): Settings =
      copy(cleanupUnusedAfter = newCleanupUnusedAfter)

    /**
     * Java API
     */
    def withCleanupUnusedAfter(newCleanupUnusedAfter: JavaDuration): Settings =
      copy(cleanupUnusedAfter = newCleanupUnusedAfter.toScala)

    /**
     * Java API
     */
    def getCleanupUnusedAfter(): JavaDuration =
      cleanupUnusedAfter.toJava

    def withJournalPluginId(id: String): Settings =
      copy(journalPluginId = id)

    def withSnapshotPluginId(id: String): Settings =
      copy(snapshotPluginId = id)

    /**
     * Private copy method for internal use only.
     */
    private def copy(
        restartMaxBackoff: FiniteDuration = restartMaxBackoff,
        snapshotEvery: Int = snapshotEvery,
        keepNSnapshots: Int = keepNSnapshots,
        deleteEvents: Boolean = deleteEvents,
        cleanupUnusedAfter: FiniteDuration = cleanupUnusedAfter,
        journalPluginId: String = journalPluginId,
        snapshotPluginId: String = snapshotPluginId) =
      new Settings(
        restartMaxBackoff,
        snapshotEvery,
        keepNSnapshots,
        deleteEvents,
        cleanupUnusedAfter,
        journalPluginId,
        snapshotPluginId)

    override def toString: String =
      s"Settings($restartMaxBackoff,$snapshotEvery,$keepNSnapshots,$deleteEvents,$cleanupUnusedAfter,$journalPluginId,$snapshotPluginId)"
  }

  private case class CleanupTick[A]() extends DurableProducerQueue.Command[A]

  def apply[A](persistenceId: PersistenceId): Behavior[DurableProducerQueue.Command[A]] = {
    Behaviors.setup { context =>
      apply(persistenceId, Settings(context.system))
    }
  }

  def apply[A](persistenceId: PersistenceId, settings: Settings): Behavior[DurableProducerQueue.Command[A]] = {
    Behaviors.setup { context =>
      context.setLoggerName(classOf[EventSourcedProducerQueue[A]])
      val impl = new EventSourcedProducerQueue[A](context, settings.cleanupUnusedAfter)

      Behaviors.withTimers { timers =>
        // for sharding it can become many different confirmation qualifier and this
        // cleanup task is removing qualifiers from `state.confirmedSeqNr` that have not been used for a while
        context.self ! CleanupTick[A]()
        timers.startTimerWithFixedDelay(CleanupTick[A](), settings.cleanupUnusedAfter / 2)

        val retentionCriteria = RetentionCriteria.snapshotEvery(
          numberOfEvents = settings.snapshotEvery,
          keepNSnapshots = settings.keepNSnapshots)
        val retentionCriteria2 =
          if (settings.deleteEvents) retentionCriteria.withDeleteEventsOnSnapshot else retentionCriteria

        EventSourcedBehavior[Command[A], Event, State[A]](
          persistenceId,
          State.empty,
          (state, command) => impl.onCommand(state, command),
          (state, event) => impl.onEvent(state, event))
          .withRetention(retentionCriteria2)
          .withJournalPluginId(settings.journalPluginId)
          .withSnapshotPluginId(settings.snapshotPluginId)
          .onPersistFailure(SupervisorStrategy
            .restartWithBackoff(1.second.min(settings.restartMaxBackoff), settings.restartMaxBackoff, 0.1))
      }
    }
  }

  /**
   * Java API
   */
  def create[A](persistenceId: PersistenceId): Behavior[DurableProducerQueue.Command[A]] =
    apply(persistenceId)

  /**
   * Java API
   */
  def create[A](persistenceId: PersistenceId, settings: Settings): Behavior[DurableProducerQueue.Command[A]] =
    apply(persistenceId, settings)

}

/**
 * INTERNAL API
 */
private class EventSourcedProducerQueue[A](
    context: ActorContext[DurableProducerQueue.Command[A]],
    cleanupUnusedAfter: FiniteDuration) {
  import DurableProducerQueue._

  private val traceEnabled = context.log.isTraceEnabled
  // transient
  private var initialCleanupDone = false

  def onCommand(state: State[A], command: Command[A]): Effect[Event, State[A]] =
    if (initialCleanupDone) {
      command match {
        case StoreMessageSent(sent, replyTo) =>
          val currentSeqNr = state.currentSeqNr
          if (sent.seqNr == currentSeqNr) {
            if (traceEnabled)
              context.log.trace(
                "StoreMessageSent seqNr [{}], confirmationQualifier [{}]",
                sent.seqNr,
                sent.confirmationQualifier)
            Effect.persist(sent).thenReply(replyTo)(_ => StoreMessageSentAck(sent.seqNr))
          } else if (sent.seqNr == currentSeqNr - 1) {
            // already stored, could be a retry after timeout
            context.log.debug("Duplicate seqNr [{}], currentSeqNr [{}]", sent.seqNr, currentSeqNr)
            Effect.reply(replyTo)(StoreMessageSentAck(sent.seqNr))
          } else {
            // may happen after failure
            context.log.debug("Ignoring unexpected seqNr [{}], currentSeqNr [{}]", sent.seqNr, currentSeqNr)
            Effect.unhandled // no reply, request will timeout
          }

        case StoreMessageConfirmed(seqNr, confirmationQualifier, timestampMillis) =>
          if (traceEnabled)
            context.log.trace(
              "StoreMessageConfirmed seqNr [{}], confirmationQualifier [{}]",
              seqNr,
              confirmationQualifier)
          val previousConfirmedSeqNr = state.confirmedSeqNr.get(confirmationQualifier) match {
            case Some((nr, _)) => nr
            case None          => 0L
          }
          if (seqNr > previousConfirmedSeqNr)
            Effect.persist(Confirmed(seqNr, confirmationQualifier, timestampMillis))
          else
            Effect.none // duplicate

        case LoadState(replyTo) =>
          Effect.reply(replyTo)(state)

        case _: CleanupTick[_] =>
          onCleanupTick(state)

        case cmd =>
          throw new RuntimeException(s"Unexpected command $cmd")
      }
    } else {
      onCommandBeforeInitialCleanup(state, command)
    }

  private def onCleanupTick(state: State[A]): Effect[Event, State[A]] = {
    val old = oldUnconfirmedToCleanup(state)
    if (old.isEmpty) {
      Effect.none
    } else {
      if (context.log.isDebugEnabled)
        context.log.debug("Periodic cleanup [{}]", old.mkString(","))
      Effect.persist(DurableProducerQueue.Cleanup(old))
    }
  }

  private def oldUnconfirmedToCleanup(state: State[A]): Set[ConfirmationQualifier] = {
    val now = System.currentTimeMillis()
    state.confirmedSeqNr.collect {
      case (confirmationQualifier, (_, timestampMillis))
          if (now - timestampMillis) >= cleanupUnusedAfter.toMillis && !state.unconfirmed.exists(
            _.confirmationQualifier != confirmationQualifier) =>
        confirmationQualifier
    }.toSet
  }

  def onCommandBeforeInitialCleanup(state: State[A], command: Command[A]): Effect[Event, State[A]] = {
    command match {
      case _: CleanupTick[_] =>
        val old = oldUnconfirmedToCleanup(state)
        val stateWithoutPartialChunkedMessages = state.cleanupPartialChunkedMessages()
        initialCleanupDone = true
        if (old.isEmpty && (stateWithoutPartialChunkedMessages eq state)) {
          Effect.unstashAll()
        } else {
          if (context.log.isDebugEnabled)
            context.log.debug("Initial cleanup [{}]", old.mkString(","))
          Effect.persist(DurableProducerQueue.Cleanup(old)).thenUnstashAll()
        }
      case _ =>
        Effect.stash()
    }
  }

  def onEvent(state: State[A], event: Event): State[A] = {
    event match {
      case sent: MessageSent[A] @unchecked =>
        state.addMessageSent(sent)
      case Confirmed(seqNr, confirmationQualifier, timestampMillis) =>
        state.confirmed(seqNr, confirmationQualifier, timestampMillis)
      case Cleanup(confirmationQualifiers) =>
        state.cleanup(confirmationQualifiers).cleanupPartialChunkedMessages()
    }
  }

}
