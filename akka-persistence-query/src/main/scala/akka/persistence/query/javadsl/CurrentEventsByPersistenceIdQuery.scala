/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.stream.javadsl.Source

/**
 * A plugin may optionally support this query by implementing this interface.
 */
trait CurrentEventsByPersistenceIdQuery extends ReadJournal {

  /**
   * Same type of query as [[EventsByPersistenceIdQuery#eventsByPersistenceId]]
   * but the event stream is completed immediately when it reaches the end of
   * the "result set". Events that are stored after the query is completed are
   * not included in the event stream.
   */
  def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed]

}
