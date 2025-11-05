/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source

/**
 * A plugin may optionally support this query by implementing this interface.
 */
trait CurrentPersistenceIdsQuery extends ReadJournal {

  /**
   * Same type of query as [[PersistenceIdsQuery#persistenceIds]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   */
  def currentPersistenceIds(): Source[String, NotUsed]

}
