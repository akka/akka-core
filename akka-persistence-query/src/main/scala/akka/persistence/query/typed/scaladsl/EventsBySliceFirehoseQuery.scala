/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import java.time.Instant

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Future

import com.typesafe.config.Config

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.internal.EventsBySliceFirehose
import akka.stream.scaladsl.Source

object EventsBySliceFirehoseQuery {
  val Identifier = "akka.persistence.query.events-by-slice-firehose"

}

/**
 * This wrapper of [[EventsBySliceQuery]] gives better scalability when many consumers retrieve the
 * same events, for example many Projections of the same entity type. The purpose is to share
 * the stream of events from the database and fan out to connected consumer streams. Thereby fewer
 * queries and loading of events from the database.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[EventsBySliceQuery](EventsBySliceFirehoseQuery.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.query.typed.javadsl.EventsBySliceFirehoseQuery]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"akka.persistence.query.events-by-slice-firehose"`
 * for the default [[EventsBySliceFirehoseQuery#Identifier]]. See `reference.conf`.
 */
@nowarn("msg=never used")
final class EventsBySliceFirehoseQuery(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with EventsBySliceQuery
    with EventsBySliceStartingFromSnapshotsQuery
    with EventTimestampQuery
    with LoadEventQuery {

  private lazy val persistenceExt = Persistence(system)
  private lazy val settings = EventsBySliceFirehose.Settings(system, cfgPath)

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    EventsBySliceFirehose(system).eventsBySlices(cfgPath, entityType, minSlice, maxSlice, offset)

  override def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] =
    EventsBySliceFirehose(system).eventsBySlicesStartingFromSnapshots(
      cfgPath,
      entityType,
      minSlice,
      maxSlice,
      offset,
      transformSnapshot)

  override def sliceForPersistenceId(persistenceId: String): Int =
    persistenceExt.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    eventsBySliceQuery match {
      case q: EventTimestampQuery => q.timestampOf(persistenceId, sequenceNr)
      case _ =>
        throw new IllegalArgumentException(
          s"Underlying ReadJournal [${settings.delegateQueryPluginId}] doesn't implement EventTimestampQuery")
    }
  }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] =
    eventsBySliceQuery match {
      case q: LoadEventQuery => q.loadEnvelope(persistenceId, sequenceNr)
      case _ =>
        throw new IllegalArgumentException(
          s"Underlying ReadJournal [${settings.delegateQueryPluginId}] " +
          "doesn't implement LoadEventQuery")
    }

  private def eventsBySliceQuery: EventsBySliceQuery = {
    val delegateQueryPluginId =
      EventsBySliceFirehose.Settings.delegateQueryPluginId(system.settings.config.getConfig(cfgPath))
    PersistenceQuery(system).readJournalFor[EventsBySliceQuery](delegateQueryPluginId)
  }

}
