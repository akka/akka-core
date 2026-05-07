/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import java.util.concurrent.CompletionStage

import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 */
trait LoadEventQuery extends ReadJournal {

  /**
   * Load a single event on demand. The `CompletionStage` is completed with a `NoSuchElementException` if
   * the event for the given `persistenceId` and `sequenceNr` doesn't exist.
   */
  def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]]
}
