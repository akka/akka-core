/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import scala.collection.immutable

import akka.NotUsed
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.stream.scaladsl.Source

/**
 * A plugin may optionally support this query by implementing this trait.
 */
trait CurrentEventsBySliceStartingFromSnapshotsQuery extends ReadJournal {

  /**
   * Same as [[EventsBySliceStartingFromSnapshotsQuery]] but with the purpose to use snapshots as starting points
   * and thereby reducing number of events that have to be loaded. This can be useful if the consumer start
   * from zero without any previously processed offset or if it has been disconnected for a long while and
   * its offset is far behind.
   *
   * Same type of query as [[EventsBySliceStartingFromSnapshotsQuery.eventsBySlicesStartingFromSnapshots]] but
   * the event stream is completed immediately when it reaches the end of the "result set".
   */
  def currentEventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed]

  def sliceForPersistenceId(persistenceId: String): Int

  def sliceRanges(numberOfRanges: Int): immutable.Seq[Range]
}
