/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.concurrent.Future

import akka.persistence.PersistentRepr

/**
 * Asynchronous message replay and sequence number recovery interface.
 */
trait AsyncRecovery {
  //#journal-plugin-api
  /**
   * Plugin API: asynchronously replays persistent messages. Implementations replay
   * a message by calling `replayCallback`. The returned future must be completed
   * when all messages (matching the sequence number bounds) have been replayed.
   * The future must be completed with a failure if any of the persistent messages
   * could not be replayed.
   *
   * The `replayCallback` must also be called with messages that have been marked
   * as deleted. In this case a replayed message's `deleted` method must return
   * `true`.
   *
   * The `toSequenceNr` is the lowest of what was returned by [[#asyncReadHighestSequenceNr]]
   * and what the user specified as recovery [[akka.persistence.Recovery]] parameter.
   * This does imply that this call is always preceded by reading the highest sequence
   * number for the given `persistenceId`.
   *
   * A special case is `fromSequenceNr` of -1, which means that only the last message if any
   * should be replayed.
   *
   * This call is NOT protected with a circuit-breaker because it may take long time
   * to replay all events. The plugin implementation itself must protect against
   * an unresponsive backend store and make sure that the returned Future is
   * completed with success or failure within reasonable time. It is not allowed
   * to ignore completing the future.
   *
   * @param persistenceId persistent actor id.
   * @param fromSequenceNr sequence number where replay should start (inclusive).
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param max maximum number of messages to be replayed.
   * @param recoveryCallback called to replay a single message. Can be called from any
   *                       thread.
   * @see [[AsyncWriteJournal]]
   */
  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit]

  /**
   * Plugin API: asynchronously reads the highest stored sequence number for the
   * given `persistenceId`. The persistent actor will use the highest sequence
   * number after recovery as the starting point when persisting new events.
   * This sequence number is also used as `toSequenceNr` in subsequent call
   * to [[#asyncReplayMessages]] unless the user has specified a lower `toSequenceNr`.
   * Journal must maintain the highest sequence number and never decrease it.
   *
   * This call is protected with a circuit-breaker.
   *
   * Please also note that requests for the highest sequence number may be made concurrently
   * to writes executing for the same `persistenceId`, in particular it is possible that
   * a restarting actor tries to recover before its outstanding writes have completed.
   *
   * @param persistenceId persistent actor id.
   * @param fromSequenceNr hint where to start searching for the highest sequence
   *                       number. When a persistent actor is recovering this
   *                       `fromSequenceNr` will be the sequence number of the used
   *                       snapshot or `0L` if no snapshot is used.
   */
  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]
  //#journal-plugin-api
}

/**
 * A plugin may implement this trait as an optimization. Combining `asyncReplayMessages` and
 * `asyncReadHighestSequenceNr` into one method. If this trait is implemented the
 * methods in [[AsyncRecovery]] will not be called.
 */
trait AsyncReplay {

  /**
   * Plugin API: asynchronously replays persistent messages. Implementations replay
   * a message by calling `replayCallback`. The returned future must be completed
   * with the highest sequence number when all messages (matching the sequence number bounds)
   * have been replayed. Journal must maintain the highest sequence number and never decrease it.
   * The future must be completed with a failure if any of the persistent messages
   * could not be replayed.
   *
   * The `toSequenceNr` will either be `Long.MaxValue` to replay all messages or otherwise a limited
   * upper sequence number. When replaying all messages the returned highest sequence number should
   * be the same as the last replayed message, and the implementation would typically not have to
   * read highest sequence number in other way than replaying the messages. With a custom `toSequenceNr`
   * the implementation would typically have to both replay messages and read the actual highest sequence
   * number.
   *
   * One special case is `toSequenceNr` of 0, which means that no messages should be replayed, but the
   * returned highest sequence number must still be the highest of all stored messages. In this case
   * the implementation would typically have to read the actual highest sequence number but can skip
   * replay of messages.
   *
   * Another special case is `fromSequenceNr` of -1, which means that only the last message if any
   * should be replayed.
   *
   * This call is NOT protected with a circuit-breaker because it may take long time
   * to replay all events. The plugin implementation itself must protect against
   * an unresponsive backend store and make sure that the returned Future is
   * completed with success or failure within reasonable time. It is not allowed
   * to ignore completing the future.
   *
   * Please also note that requests to `replayMessages` may be made concurrently
   * to writes executing for the same `persistenceId`, in particular it is possible that
   * a restarting actor tries to recover before its outstanding writes have completed.
   *
   * @param persistenceId persistent actor id.
   * @param fromSequenceNr sequence number where replay should start (inclusive).
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param max maximum number of messages to be replayed.
   * @param recoveryCallback called to replay a single message. Can be called from any
   *                       thread.
   * @return highest sequence number
   *
   * @see [[AsyncWriteJournal]]
   */
  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Long]

}
