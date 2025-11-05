/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

import akka.NotUsed
import akka.persistence.query.{ EventEnvelope, Offset }
import akka.stream.javadsl.Source

/**
 * A plugin may optionally support this query by implementing this interface.
 */
trait CurrentEventsByTagQuery extends ReadJournal {

  /**
   * Same type of query as [[EventsByTagQuery#eventsByTag]] but the event stream
   * is completed immediately when it reaches the end of the "result set". Depending
   * on journal implementation, this may mean all events up to when the query is
   * started, or it may include events that are persisted while the query is still
   * streaming results. For eventually consistent stores, it may only include all
   * events up to some point before the query is started.
   */
  def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed]
}
