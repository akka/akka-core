/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.stream.javadsl.Source

/**
 * A plugin may optionally support this query by implementing this trait.
 *
 * `EventsBySliceQuery` that is using a timestamp based offset should also implement [[EventTimestampQuery]] and
 * [[LoadEventQuery]].
 *
 * See also [[EventsBySliceFirehoseQuery]].
 */
trait EventsBySliceQuery extends ReadJournal {

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The exact meaning of the `offset` depends on the journal and must be documented by the read journal plugin. It may
   * be a sequential id number that uniquely identifies the position of each event within the event stream. Distributed
   * data stores cannot easily support those semantics and they may use a weaker meaning. For example it may be a
   * timestamp (taken when the event was created or stored). Timestamps are not unique and not strictly ordered, since
   * clocks on different machines may not be synchronized.
   *
   * In strongly consistent stores, where the `offset` is unique and strictly ordered, the stream should start from the
   * next event after the `offset`. Otherwise, the read journal should ensure that between an invocation that returned
   * an event with the given `offset`, and this invocation, no events are missed. Depending on the journal
   * implementation, this may mean that this invocation will return events that were already returned by the previous
   * invocation, including the event with the passed in `offset`.
   *
   * The returned event stream should be ordered by `offset` if possible, but this can also be difficult to fulfill for
   * a distributed data store. The order must be documented by the read journal plugin.
   *
   * The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
   * events when new events are persisted. Corresponding query that is completed when it reaches the end of the
   * currently stored events is provided by [[CurrentEventsBySliceQuery.currentEventsBySlices]].
   */
  def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed]

  def sliceForPersistenceId(persistenceId: String): Int

  def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]]

}
