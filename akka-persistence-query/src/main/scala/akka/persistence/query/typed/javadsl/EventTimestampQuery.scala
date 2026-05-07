/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.persistence.query.javadsl.ReadJournal

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 */
trait EventTimestampQuery extends ReadJournal {

  def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]]

}
