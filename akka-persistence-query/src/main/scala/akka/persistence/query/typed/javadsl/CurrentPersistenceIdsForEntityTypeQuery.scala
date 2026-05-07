/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source

import java.util.Optional

/**
 * A plugin may optionally support this query by implementing this trait.
 */
trait CurrentPersistenceIdsForEntityTypeQuery {

  /**
   * Get the current persistence ids for a given entity type.
   *
   * The persistenceIds must start with the entity type followed by default separator ("|") from
   * [[akka.persistence.typed.PersistenceId]].
   *
   * @param entityType
   *   The entity type name.
   * @param afterId
   *   The ID to start returning results from, or empty to return all ids. This should be an id returned from a previous
   *   invocation of this command. Callers should not assume that ids are returned in sorted order.
   * @param limit
   *   The maximum results to return. Use Long.MAX_VALUE to return all results. Must be greater than zero.
   * @return
   *   A source containing all the persistence ids, limited as specified.
   */
  def currentPersistenceIds(entityType: String, afterId: Optional[String], limit: Long): Source[String, NotUsed]
}
