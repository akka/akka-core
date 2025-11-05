/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

import java.util.Optional

import akka.NotUsed
import akka.stream.javadsl.Source

/**
 * A ReadJournal may optionally support this query by implementing this trait.
 */
trait PagedPersistenceIdsQuery extends ReadJournal {

  /**
   * Get the current persistence ids.
   *
   * Not all plugins may support in database paging, and may simply use drop/take Akka streams operators
   * to manipulate the result set according to the paging parameters.
   *
   * @param afterId The ID to start returning results from, or empty to return all ids. This should be an id returned
   *                from a previous invocation of this command. Callers should not assume that ids are returned in
   *                sorted order.
   * @param limit The maximum results to return. Use Long.MAX_VALUE to return all results. Must be greater than zero.
   * @return A source containing all the persistence ids, limited as specified.
   */
  def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed]

}
