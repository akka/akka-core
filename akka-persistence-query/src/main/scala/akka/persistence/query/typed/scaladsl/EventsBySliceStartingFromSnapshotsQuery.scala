/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import scala.collection.immutable

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.stream.scaladsl.Source

/**
 * A plugin may optionally support this query by implementing this trait.
 *
 * `EventsBySliceStartingFromSnapshotsQuery` that is using a timestamp based offset should also implement [[EventTimestampQuery]] and
 * [[LoadEventQuery]].
 *
 * See also [[EventsBySliceFirehoseQuery]].
 *
 * API May Change
 */
@ApiMayChange
trait EventsBySliceStartingFromSnapshotsQuery extends ReadJournal {

  /**
   * Same as [[EventsBySliceQuery]] but with the purpose to use snapshots as starting points and thereby reducing number of
   * events that have to be loaded. This can be useful if the consumer start from zero without any previously processed
   * offset or if it has been disconnected for a long while and its offset is far behind.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed]

  def sliceForPersistenceId(persistenceId: String): Int

  def sliceRanges(numberOfRanges: Int): immutable.Seq[Range]

}
