/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.stream.scaladsl.Source

/**
 * A plugin may optionally support this query by implementing this trait.
 */
@ApiMayChange
trait CurrentEventsByPersistenceIdStartingFromSnapshotQuery extends ReadJournal {

  /**
   * Same as [[CurrentEventsByPersistenceIdTypedQuery]] but with the purpose to use snapshot as starting point
   * and thereby reducing number of events that have to be loaded.
   */
  def currentEventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed]

}
