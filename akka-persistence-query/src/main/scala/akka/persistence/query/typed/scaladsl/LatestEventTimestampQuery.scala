/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import java.time.Instant

import scala.concurrent.Future

import akka.persistence.query.scaladsl.ReadJournal

/**
 * Retrieve the latest timestamp for an entity type and slice range.
 */
trait LatestEventTimestampQuery extends ReadJournal {

  def latestEventTimestamp(entityType: String, minSlice: Int, maxSlice: Int): Future[Option[Instant]]

}
