/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.persistence.query.javadsl.ReadJournal

/**
 * Retrieve the latest timestamp for an entity type and slice range.
 */
trait LatestEventTimestampQuery extends ReadJournal {

  def latestEventTimestamp(entityType: String, minSlice: Int, maxSlice: Int): CompletionStage[Optional[Instant]]

}
