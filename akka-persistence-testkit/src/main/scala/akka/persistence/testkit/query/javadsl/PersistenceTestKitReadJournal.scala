/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.javadsl.CurrentEventsByTagQuery
import akka.persistence.query.javadsl.EventsByPersistenceIdQuery
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.{ EventEnvelope => TypedEventEnvelope }
import akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.javadsl.EventsByPersistenceIdTypedQuery
import akka.persistence.testkit.query.scaladsl
import akka.stream.javadsl.Source

object PersistenceTestKitReadJournal {
  val Identifier = "akka.persistence.testkit.query"
}

final class PersistenceTestKitReadJournal(delegate: scaladsl.PersistenceTestKitReadJournal)
    extends ReadJournal
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdTypedQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with CurrentEventsByTagQuery
    with CurrentEventsBySliceQuery {

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    delegate.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[TypedEventEnvelope[Event], NotUsed] =
    delegate.eventsByPersistenceIdTyped(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[TypedEventEnvelope[Event], NotUsed] =
    delegate.currentEventsByPersistenceIdTyped(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByTag(tag, offset).asJava

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[TypedEventEnvelope[Event], NotUsed] =
    delegate.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]] = {
    import scala.jdk.CollectionConverters._
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }
}
