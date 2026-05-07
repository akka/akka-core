/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.lang.{ Iterable => JIterable }

import scala.collection.immutable
import scala.util.control.NoStackTrace

import com.typesafe.config.Config

import akka.actor._
import akka.annotation.InternalApi
import akka.japi.Procedure
import akka.japi.Util

abstract class RecoveryCompleted

/**
 * Sent to a [[PersistentActor]] when the journal replay has been finished.
 */
@SerialVersionUID(1L)
case object RecoveryCompleted extends RecoveryCompleted {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Recovery mode configuration object to be returned in [[PersistentActor#recovery]].
 *
 * By default recovers from latest snapshot replays through to the last available event (last sequenceId).
 *
 * Recovery will start from a snapshot if the persistent actor has previously saved one or more snapshots
 * and at least one of these snapshots matches the specified `fromSnapshot` criteria.
 * Otherwise, recovery will start from scratch by replaying all stored events.
 *
 * If recovery starts from a snapshot, the persistent actor is offered that snapshot with a [[SnapshotOffer]]
 * message, followed by replayed messages, if any, that are younger than the snapshot, up to the
 * specified upper sequence number bound (`toSequenceNr`).
 *
 * @param fromSnapshot criteria for selecting a saved snapshot from which recovery should start. Default
 *                     is latest (= youngest) snapshot.
 * @param toSequenceNr upper sequence number bound (inclusive) for recovery. Default is no upper bound.
 * @param replayMax maximum number of messages to replay. Default is no limit.
 */
@SerialVersionUID(1L)
final case class Recovery(
    fromSnapshot: SnapshotSelectionCriteria = SnapshotSelectionCriteria.Latest,
    toSequenceNr: Long = Long.MaxValue,
    replayMax: Long = Long.MaxValue)

object Recovery {

  val default: Recovery = Recovery()

  /**
   * Convenience method for skipping recovery in [[PersistentActor]].
   *
   * It will still retrieve previously highest sequence number so that new events are persisted with
   * higher sequence numbers rather than starting from 1 and assuming that there are no
   * previous event with that sequence number.
   *
   * @see [[Recovery]]
   */
  val none: Recovery = Recovery(toSequenceNr = 0L, fromSnapshot = SnapshotSelectionCriteria.None)

  /**
   * Java API
   * @see [[Recovery]]
   */
  def create() = default

  /**
   * Java API
   * @see [[Recovery]]
   */
  def create(toSequenceNr: Long) =
    Recovery(toSequenceNr = toSequenceNr)

  /**
   * Java API
   * @see [[Recovery]]
   */
  def create(fromSnapshot: SnapshotSelectionCriteria) =
    Recovery(fromSnapshot = fromSnapshot)

  /**
   * Java API
   * @see [[Recovery]]
   */
  def create(fromSnapshot: SnapshotSelectionCriteria, toSequenceNr: Long) =
    Recovery(fromSnapshot, toSequenceNr)

  /**
   * Java API
   * @see [[Recovery]]
   */
  def create(fromSnapshot: SnapshotSelectionCriteria, toSequenceNr: Long, replayMax: Long) =
    Recovery(fromSnapshot, toSequenceNr, replayMax)

}

final class RecoveryTimedOut(message: String) extends RuntimeException(message) with NoStackTrace

/**
 * This defines how to handle the current received message which failed to stash, when the size of
 * Stash exceeding the capacity of Stash.
 */
sealed trait StashOverflowStrategy

/**
 * Discard the message to [[akka.actor.DeadLetter]].
 */
case object DiscardToDeadLetterStrategy extends StashOverflowStrategy {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Throw [[akka.actor.StashOverflowException]], hence the persistent actor will starting recovery
 * if guarded by default supervisor strategy.
 * Be carefully if used together with persist/persistAll or has many messages needed
 * to replay.
 */
case object ThrowOverflowExceptionStrategy extends StashOverflowStrategy {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Reply to sender with predefined response, and discard the received message silently.
 * @param response the message replying to sender with
 */
final case class ReplyToStrategy(response: Any) extends StashOverflowStrategy

/**
 * Implement this interface in order to configure the stashOverflowStrategy for
 * the internal stash of persistent actor.
 * An instance of this class must be instantiable using a no-arg constructor.
 */
trait StashOverflowStrategyConfigurator {
  def create(config: Config): StashOverflowStrategy
}

final class ThrowExceptionConfigurator extends StashOverflowStrategyConfigurator {
  override def create(config: Config) = ThrowOverflowExceptionStrategy
}

final class DiscardConfigurator extends StashOverflowStrategyConfigurator {
  override def create(config: Config) = DiscardToDeadLetterStrategy
}

/**
 * Scala API: A persistent Actor - can be used to implement command or Event Sourcing.
 */
trait PersistentActor extends Eventsourced with PersistenceIdentity {
  def receive = receiveCommand

  /**
   * Asynchronously persists `event`. On successful persistence, `handler` is called with the
   * persisted event. It is guaranteed that no new commands will be received by a persistent actor
   * between a call to `persist` and the execution of its `handler`. This also holds for
   * multiple `persist` calls per received command. Internally, this is achieved by stashing new
   * commands and unstashing them when the `event` has been persisted and handled. The stash used
   * for that is an internal stash which doesn't interfere with the inherited user stash.
   *
   * An event `handler` may close over persistent actor state and modify it. The `sender` of a persisted
   * event is the sender of the corresponding command. This means that one can reply to a command
   * sender within an event `handler`.
   *
   * Within an event handler, applications usually update persistent actor state using persisted event
   * data, notify listeners and reply to command senders.
   *
   * If persistence of an event fails, [[#onPersistFailure]] will be invoked and the actor will
   * unconditionally be stopped. The reason that it cannot resume when persist fails is that it
   * is unknown if the event was actually persisted or not, and therefore it is in an inconsistent
   * state. Restarting on persistent failures will most likely fail anyway, since the journal
   * is probably unavailable. It is better to stop the actor and after a back-off timeout start
   * it again.
   *
   * @param event event to be persisted
   * @param handler handler for each persisted `event`
   */
  def persist[A](event: A)(handler: A => Unit): Unit = {
    internalPersist(event)(handler)
  }

  /**
   * Asynchronously persists `events` in specified order. This is equivalent to calling
   * `persist[A](event: A)(handler: A => Unit)` multiple times with the same `handler`,
   * except that `events` are persisted atomically with this method.
   *
   * @param events events to be persisted
   * @param handler handler for each persisted `events`
   */
  def persistAll[A](events: immutable.Seq[A])(handler: A => Unit): Unit = {
    internalPersistAll(events)(handler)
  }

  /**
   * Asynchronously persists `event`. On successful persistence, `handler` is called with the
   * persisted event.
   *
   * Unlike `persist` the persistent actor will continue to receive incoming commands between the
   * call to `persist` and executing it's `handler`. This asynchronous, non-stashing, version of
   * of persist should be used when you favor throughput over the "command-2 only processed after
   * command-1 effects' have been applied" guarantee, which is provided by the plain `persist` method.
   *
   * An event `handler` may close over persistent actor state and modify it. The `sender` of a persisted
   * event is the sender of the corresponding command. This means that one can reply to a command
   * sender within an event `handler`.
   *
   * If persistence of an event fails, [[#onPersistFailure]] will be invoked and the actor will
   * unconditionally be stopped. The reason that it cannot resume when persist fails is that it
   * is unknown if the event was actually persisted or not, and therefore it is in an inconsistent
   * state. Restarting on persistent failures will most likely fail anyway, since the journal
   * is probably unavailable. It is better to stop the actor and after a back-off timeout start
   * it again.
   *
   * @param event event to be persisted
   * @param handler handler for each persisted `event`
   */
  def persistAsync[A](event: A)(handler: A => Unit): Unit = {
    internalPersistAsync(event)(handler)
  }

  /**
   * Asynchronously persists `events` in specified order. This is equivalent to calling
   * `persistAsync[A](event: A)(handler: A => Unit)` multiple times with the same `handler`,
   * except that `events` are persisted atomically with this method.
   *
   * @param events events to be persisted
   * @param handler handler for each persisted `events`
   */
  def persistAllAsync[A](events: immutable.Seq[A])(handler: A => Unit): Unit = {
    internalPersistAllAsync(events)(handler)
  }

  /**
   * Defer the handler execution until all pending handlers have been executed.
   * Allows to define logic within the actor, which will respect the invocation-order-guarantee
   * in respect to `persistAsync` or `persist` calls. That is, if `persistAsync` or `persist` was invoked before `deferAsync`,
   * the corresponding handlers will be invoked in the same order as they were registered in.
   *
   * This call will NOT result in `event` being persisted, use `persist` or `persistAsync` instead
   * if the given event should possible to replay.
   *
   * If there are no pending persist handler calls, the handler will be called immediately.
   *
   * If persistence of an earlier event fails, the persistent actor will stop, and the `handler`
   * will not be run.
   *
   * @param event event to be handled in the future, when preceding persist operations have been processes
   * @param handler handler for the given `event`
   */
  def deferAsync[A](event: A)(handler: A => Unit): Unit = {
    internalDeferAsync(event)(handler)
  }

  /**
   * Defer the handler execution until all pending handlers have been executed. It is guaranteed that no new commands
   * will be received by a persistent actor between a call to `defer` and the execution of its `handler`.
   * Allows to define logic within the actor, which will respect the invocation-order-guarantee
   * in respect to `persistAsync` or `persist` calls. That is, if `persistAsync` or `persist` was invoked before `defer`,
   * the corresponding handlers will be invoked in the same order as they were registered in.
   *
   * This call will NOT result in `event` being persisted, use `persist` or `persistAsync` instead
   * if the given event should possible to replay.
   *
   * If there are no pending persist handler calls, the handler will be called immediately.
   *
   * If persistence of an earlier event fails, the persistent actor will stop, and the `handler`
   * will not be run.
   *
   * @param event event to be handled in the future, when preceding persist operations have been processes
   * @param handler handler for the given `event`
   */
  def defer[A](event: A)(handler: A => Unit): Unit = {
    internalDefer(event)(handler)
  }
}

/**
 * Java API: an persistent actor - can be used to implement command or Event Sourcing.
 */
abstract class AbstractPersistentActor extends AbstractActor with AbstractPersistentActorLike {

  /**
   * Recovery handler that receives persisted events during recovery. If a state snapshot
   * has been captured and saved, this handler will receive a [[SnapshotOffer]] message
   * followed by events that are younger than the offered snapshot.
   *
   * This handler must not have side-effects other than changing persistent actor state i.e. it
   * should not perform actions that may fail, such as interacting with external services,
   * for example.
   *
   * If there is a problem with recovering the state of the actor from the journal, the error
   * will be logged and the actor will be stopped.
   *
   * @see [[Recovery]]
   */
  def createReceiveRecover(): AbstractActor.Receive

  /**
   * An persistent actor has to define its initial receive behavior by implementing
   * the `createReceive` method, also known as the command handler. Typically
   * validates commands against current state (and/or by communication with other actors).
   * On successful validation, one or more events are derived from a command and
   * these events are then persisted by calling `persist`.
   */
  def createReceive(): AbstractActor.Receive

  // Note that abstract methods createReceiveRecover and createReceive are also defined in
  // AbstractPersistentActorLike. They were included here also for binary compatibility reasons.
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait AbstractPersistentActorLike extends Eventsourced {

  /**
   * Recovery handler that receives persisted events during recovery. If a state snapshot
   * has been captured and saved, this handler will receive a [[SnapshotOffer]] message
   * followed by events that are younger than the offered snapshot.
   *
   * This handler must not have side-effects other than changing persistent actor state i.e. it
   * should not perform actions that may fail, such as interacting with external services,
   * for example.
   *
   * If there is a problem with recovering the state of the actor from the journal, the error
   * will be logged and the actor will be stopped.
   *
   * @see [[Recovery]]
   */
  def createReceiveRecover(): AbstractActor.Receive

  override final def receiveRecover: Receive = createReceiveRecover().onMessage.asInstanceOf[Receive]

  /**
   * An persistent actor has to define its initial receive behavior by implementing
   * the `createReceive` method, also known as the command handler. Typically
   * validates commands against current state (and/or by communication with other actors).
   * On successful validation, one or more events are derived from a command and
   * these events are then persisted by calling `persist`.
   */
  def createReceive(): AbstractActor.Receive

  override final def receiveCommand: Receive = createReceive().onMessage.asInstanceOf[Receive]

  /**
   * Java API: asynchronously persists `event`. On successful persistence, `handler` is called with the
   * persisted event. It is guaranteed that no new commands will be received by a persistent actor
   * between a call to `persist` and the execution of its `handler`. This also holds for
   * multiple `persist` calls per received command. Internally, this is achieved by stashing new
   * commands and unstashing them when the `event` has been persisted and handled. The stash used
   * for that is an internal stash which doesn't interfere with the inherited user stash.
   *
   * An event `handler` may close over persistent actor state and modify it. The `getSender()` of a persisted
   * event is the sender of the corresponding command. This means that one can reply to a command
   * sender within an event `handler`.
   *
   * Within an event handler, applications usually update persistent actor state using persisted event
   * data, notify listeners and reply to command senders.
   *
   * If persistence of an event fails, [[#onPersistFailure]] will be invoked and the actor will
   * unconditionally be stopped. The reason that it cannot resume when persist fails is that it
   * is unknown if the event was actually persisted or not, and therefore it is in an inconsistent
   * state. Restarting on persistent failures will most likely fail anyway, since the journal
   * is probably unavailable. It is better to stop the actor and after a back-off timeout start
   * it again.
   *
   * @param event event to be persisted.
   * @param handler handler for each persisted `event`
   */
  def persist[A](event: A, handler: Procedure[A]): Unit =
    internalPersist(event)(event => handler(event))

  /**
   * Java API: asynchronously persists `events` in specified order. This is equivalent to calling
   * `persist[A](event: A, handler: Procedure[A])` multiple times with the same `handler`,
   * except that `events` are persisted atomically with this method.
   *
   * @param events events to be persisted.
   * @param handler handler for each persisted `events`
   */
  def persistAll[A](events: JIterable[A], handler: Procedure[A]): Unit =
    internalPersistAll(Util.immutableSeq(events))(event => handler(event))

  /**
   * Java API: asynchronously persists `event`. On successful persistence, `handler` is called with the
   * persisted event.
   *
   * Unlike `persist` the persistent actor will continue to receive incoming commands between the
   * call to `persistAsync` and executing it's `handler`. This asynchronous, non-stashing, version of
   * of persist should be used when you favor throughput over the strict ordering guarantees that `persist` guarantees.
   *
   * If persistence of an event fails, [[#onPersistFailure]] will be invoked and the actor will
   * unconditionally be stopped. The reason that it cannot resume when persist fails is that it
   * is unknown if the event was actually persisted or not, and therefore it is in an inconsistent
   * state. Restarting on persistent failures will most likely fail anyway, since the journal
   * is probably unavailable. It is better to stop the actor and after a back-off timeout start
   * it again.
   *
   * @param event event to be persisted
   * @param handler handler for each persisted `event`
   */
  def persistAsync[A](event: A, handler: Procedure[A]): Unit =
    internalPersistAsync(event)(event => handler(event))

  /**
   * Java API: asynchronously persists `events` in specified order. This is equivalent to calling
   * `persistAsync[A](event: A)(handler: A => Unit)` multiple times with the same `handler`,
   * except that `events` are persisted atomically with this method.
   *
   * @param events events to be persisted
   * @param handler handler for each persisted `events`
   */
  def persistAllAsync[A](events: JIterable[A], handler: Procedure[A]): Unit =
    internalPersistAllAsync(Util.immutableSeq(events))(event => handler(event))

  /**
   * Defer the handler execution until all pending handlers have been executed.
   * Allows to define logic within the actor, which will respect the invocation-order-guarantee
   * in respect to `persistAsync` calls. That is, if `persistAsync` was invoked before defer,
   * the corresponding handlers will be invoked in the same order as they were registered in.
   *
   * This call will NOT result in `event` being persisted, please use `persist` or `persistAsync`,
   * if the given event should possible to replay.
   *
   * If there are no pending persist handler calls, the handler will be called immediately.
   *
   * If persistence of an earlier event fails, the persistent actor will stop, and the `handler`
   * will not be run.
   *
   * @param event event to be handled in the future, when preceding persist operations have been processes
   * @param handler handler for the given `event`
   */
  def deferAsync[A](event: A)(handler: Procedure[A]): Unit =
    internalDeferAsync(event)(event => handler(event))

  /**
   * Defer the handler execution until all pending handlers have been executed. It is guaranteed that no new commands
   * will be received by a persistent actor between a call to `defer` and the execution of its `handler`.
   * Allows to define logic within the actor, which will respect the invocation-order-guarantee
   * in respect to `persistAsync` or `persist` calls. That is, if `persistAsync` or `persist` was invoked before `defer`,
   * the corresponding handlers will be invoked in the same order as they were registered in.
   *
   * This call will NOT result in `event` being persisted, use `persist` or `persistAsync` instead
   * if the given event should possible to replay.
   *
   * If there are no pending persist handler calls, the handler will be called immediately.
   *
   * If persistence of an earlier event fails, the persistent actor will stop, and the `handler`
   * will not be run.
   *
   * @param event event to be handled in the future, when preceding persist operations have been processes
   * @param handler handler for the given `event`
   */
  def defer[A](event: A)(handler: Procedure[A]): Unit = {
    internalDefer(event)(event => handler(event))
  }

}

/**
 * Java API: Combination of [[AbstractPersistentActor]] and [[akka.actor.AbstractActorWithTimers]].
 */
abstract class AbstractPersistentActorWithTimers extends AbstractActor with Timers with AbstractPersistentActorLike
