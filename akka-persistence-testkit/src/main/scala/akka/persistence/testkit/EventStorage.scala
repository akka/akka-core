/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import java.util.{ List => JList }

import scala.collection.immutable
import scala.util.{ Failure, Success, Try }

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.persistence.testkit.ProcessingPolicy.DefaultPolicies
import akka.persistence.testkit.internal.TestKitStorage
import akka.stream.scaladsl.Source
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] trait EventStorage extends TestKitStorage[JournalOperation, PersistentRepr] {
  import EventStorage._

  def addAny(key: String, elem: Any): Unit =
    addAny(key, immutable.Seq(elem))

  def addAny(key: String, elems: immutable.Seq[Any]): Unit =
    // need to use `updateExisting` because `mapAny` reads latest seqnum
    // and therefore must be done at the same time with the update, not before
    updateOrSetNew(key, v => v ++ mapAny(key, elems).toVector)

  override def reprToSeqNum(repr: (PersistentRepr)): Long = repr.sequenceNr

  def add(elems: immutable.Seq[PersistentRepr]): Unit =
    elems.groupBy(_.persistenceId).foreach { gr =>
      add(gr._1, gr._2)
    }

  override protected val DefaultPolicy = JournalPolicies.PassAll

  /**
   * @throws Exception from StorageFailure in the current writing policy
   */
  def tryAdd(elems: immutable.Seq[PersistentRepr]): Try[Unit] = {
    val grouped = elems.groupBy(_.persistenceId)

    val processed = grouped.map {
      case (pid, els) =>
        currentPolicy.tryProcess(pid, WriteEvents(els.map(_.payload match {
          case Tagged(payload, _) => payload
          case nonTagged          => nonTagged
        })))
    }

    val reduced: ProcessingResult =
      processed.foldLeft[ProcessingResult](ProcessingSuccess)((left: ProcessingResult, right: ProcessingResult) =>
        (left, right) match {
          case (ProcessingSuccess, ProcessingSuccess) => ProcessingSuccess
          case (f: StorageFailure, _)                 => f
          case (_, f: StorageFailure)                 => f
          case (r: Reject, _)                         => r
          case (_, r: Reject)                         => r
        })

    reduced match {
      case ProcessingSuccess =>
        add(elems)
        Success(())
      case Reject(ex)         => Failure(ex)
      case StorageFailure(ex) => throw ex
    }
  }

  def tryRead(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long): immutable.Seq[PersistentRepr] = {
    val batch = read(persistenceId, fromSequenceNr, toSequenceNr, max)
    currentPolicy.tryProcess(persistenceId, ReadEvents(batch)) match {
      case ProcessingSuccess  => batch
      case Reject(ex)         => throw ex
      case StorageFailure(ex) => throw ex
    }
  }

  def tryReadByTag(tag: String): immutable.Seq[PersistentRepr] = {
    val batch = readAll()
      .filter(repr =>
        repr.payload match {
          case Tagged(_, tags) => tags.contains(tag)
          case _               => false
        })
      .toVector
      .sortBy(_.timestamp)

    currentPolicy.tryProcess(tag, ReadEvents(batch)) match {
      case ProcessingSuccess  => batch
      case Reject(ex)         => throw ex
      case StorageFailure(ex) => throw ex
    }
  }

  def tryRead(processId: String, predicate: PersistentRepr => Boolean): immutable.Seq[PersistentRepr] = {
    import EventStorage.persistentReprOrdering
    val batch = readAll().filter(predicate).toVector.sorted

    currentPolicy.tryProcess(processId, ReadEvents(batch)) match {
      case ProcessingSuccess  => batch
      case Reject(ex)         => throw ex
      case StorageFailure(ex) => throw ex
    }
  }

  def tryReadSeqNumber(persistenceId: String): Long = {
    currentPolicy.tryProcess(persistenceId, ReadSeqNum) match {
      case ProcessingSuccess  => getHighestSeqNumber(persistenceId)
      case Reject(ex)         => throw ex
      case StorageFailure(ex) => throw ex
    }
  }

  def tryDelete(persistenceId: String, toSeqNumber: Long): Unit = {
    currentPolicy.tryProcess(persistenceId, DeleteEvents(toSeqNumber)) match {
      case ProcessingSuccess  => deleteToSeqNumber(persistenceId, toSeqNumber)
      case Reject(ex)         => throw ex
      case StorageFailure(ex) => throw ex
    }
  }

  def currentPersistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    afterId match {
      case Some(id) =>
        keys().sorted.dropWhile(_ != id) match {
          case s if s.size < 2 => Source.empty
          case s               => Source(s.tail).take(limit)
        }
      case None =>
        Source(keys().sorted).take(limit)
    }

  }

  private def mapAny(key: String, elems: immutable.Seq[Any]): immutable.Seq[PersistentRepr] = {
    val sn = getHighestSeqNumber(key) + 1
    elems.zipWithIndex.map(p => PersistentRepr(p._1, p._2 + sn, key))
  }

}

object EventStorage {
  object JournalPolicies extends DefaultPolicies[JournalOperation]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] implicit val persistentReprOrdering: Ordering[PersistentRepr] =
    Ordering.fromLessThan[PersistentRepr] { (a, b) =>
      if (a eq b) false
      else if (a.timestamp != b.timestamp) a.timestamp < b.timestamp
      else if (a.persistenceId != b.persistenceId) a.persistenceId.compareTo(b.persistenceId) < 0
      else if (a.sequenceNr != b.sequenceNr) a.sequenceNr < b.sequenceNr
      else false
    }
}

/**
 * INTERNAL API
 *
 * Persistent journal operations.
 */
@InternalApi
sealed trait JournalOperation

/**
 * Read from journal operation with events that were read.
 */
final case class ReadEvents(batch: immutable.Seq[Any]) extends JournalOperation {

  def getBatch(): JList[Any] = batch.asJava

}

/**
 * Write in journal operation with events to be written.
 */
final case class WriteEvents(batch: immutable.Seq[Any]) extends JournalOperation {

  def getBatch(): JList[Any] = batch.asJava

}

/**
 * Read persistent actor's sequence number operation.
 */
case object ReadSeqNum extends JournalOperation {

  /**
   * Java API: the singleton instance.
   */
  def getInstance() = this

}

/**
 * Delete events in the journal up to `toSeqNumber` operation.
 */
final case class DeleteEvents(toSeqNumber: Long) extends JournalOperation {

  def getToSeqNumber() = toSeqNumber

}
