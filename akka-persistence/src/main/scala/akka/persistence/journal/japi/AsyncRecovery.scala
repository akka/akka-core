/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.japi

import java.util.function.Consumer

import scala.concurrent.Future

import akka.actor.Actor
import akka.persistence.PersistentRepr
import akka.persistence.journal.{ AsyncRecovery => SAsyncReplay }

/**
 * Java API: asynchronous message replay and sequence number recovery interface.
 */
abstract class AsyncRecovery extends SAsyncReplay with AsyncRecoveryPlugin { this: Actor =>
  import context.dispatcher

  final def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit) =
    doAsyncReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, new Consumer[PersistentRepr] {
      def accept(p: PersistentRepr) = replayCallback(p)
    }).map(_ => ())

  final def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    doAsyncReadHighestSequenceNr(persistenceId, fromSequenceNr: Long).map(_.longValue)
}
