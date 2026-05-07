/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl
import akka.stream.javadsl.Source

object EventsBySliceFirehoseQuery {
  val Identifier: String = scaladsl.EventsBySliceFirehoseQuery.Identifier
}

/**
 * This wrapper of [[EventsBySliceQuery]] gives better scalability when many consumers retrieve the
 * same events, for example many Projections of the same entity type. The purpose is to share
 * the stream of events from the database and fan out to connected consumer streams. Thereby fewer
 * queries and loading of events from the database.
 *
 * It is retrieved with:
 * {{{
 * EventsBySliceQuery queries =
 *   PersistenceQuery.get(system).getReadJournalFor(EventsBySliceQuery.class, EventsBySliceFirehoseQuery.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[akka.persistence.query.typed.scaladsl.EventsBySliceFirehoseQuery]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"akka.persistence.query.events-by-slice-firehose"`
 * for the default [[EventsBySliceFirehoseQuery#Identifier]]. See `reference.conf`.
 */
final class EventsBySliceFirehoseQuery(delegate: scaladsl.EventsBySliceFirehoseQuery)
    extends ReadJournal
    with EventsBySliceQuery
    with EventsBySliceStartingFromSnapshotsQuery
    with EventTimestampQuery
    with LoadEventQuery
    with LatestEventTimestampQuery {

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot(_)).asJava

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import scala.jdk.CollectionConverters._
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate.timestampOf(persistenceId, sequenceNr).map(_.toJava)(ExecutionContext.parasitic).asJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).asJava

  override def latestEventTimestamp(
      entityType: String,
      minSlice: Int,
      maxSlice: Int): CompletionStage[Optional[Instant]] =
    delegate.latestEventTimestamp(entityType, minSlice, maxSlice).map(_.toJava)(ExecutionContext.parasitic).asJava

}
