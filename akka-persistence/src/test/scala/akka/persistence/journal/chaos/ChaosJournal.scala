/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.chaos

import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

import akka.persistence._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.inmem.InmemMessages

class WriteFailedException(ps: Seq[PersistentRepr])
    extends TestException(s"write failed for payloads = [${ps.map(_.payload)}]")

class ReplayFailedException(ps: Seq[PersistentRepr])
    extends TestException(s"recovery failed after replaying payloads = [${ps.map(_.payload)}]")

class ReadHighestFailedException extends TestException(s"recovery failed when reading highest sequence number")

/**
 * Keep [[ChaosJournal]] state in an external singleton so that it survives journal restarts.
 * The journal itself uses a dedicated dispatcher, so there won't be any visibility issues.
 */
private object ChaosJournalMessages extends InmemMessages

class ChaosJournal extends AsyncWriteJournal {
  import ChaosJournalMessages.{ delete => del, _ }

  val config = context.system.settings.config.getConfig("akka.persistence.journal.chaos")
  val writeFailureRate = config.getDouble("write-failure-rate")
  val deleteFailureRate = config.getDouble("delete-failure-rate")
  val replayFailureRate = config.getDouble("replay-failure-rate")
  val readHighestFailureRate = config.getDouble("read-highest-failure-rate")

  def random = ThreadLocalRandom.current

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    try Future.successful {
      if (shouldFail(writeFailureRate)) throw new WriteFailedException(messages.flatMap(_.payload))
      else
        for (a <- messages) yield {
          a.payload.foreach(add)
          AsyncWriteJournal.successUnit
        }
    } catch {
      case NonFatal(e) => Future.failed(e)
    }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    try Future.successful {
      (1L to toSequenceNr).foreach { snr =>
        del(persistenceId, snr)
      }
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    if (shouldFail(replayFailureRate)) {
      val rm = read(persistenceId, fromSequenceNr, toSequenceNr, max)
      val sm = rm.take(random.nextInt(rm.length + 1)).map(_._1)
      sm.foreach(replayCallback)
      Future.failed(new ReplayFailedException(sm))
    } else {
      read(persistenceId, fromSequenceNr, toSequenceNr, max).map(_._1).foreach(replayCallback)
      Future.successful(())
    }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    if (shouldFail(readHighestFailureRate)) Future.failed(new ReadHighestFailedException)
    else Future.successful(highestSequenceNr(persistenceId))

  def shouldFail(rate: Double): Boolean =
    random.nextDouble() < rate
}
