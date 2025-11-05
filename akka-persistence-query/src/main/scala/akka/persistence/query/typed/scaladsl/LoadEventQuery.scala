/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.query.typed.scaladsl

import scala.concurrent.Future

import akka.annotation.ApiMayChange
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 *
 * API May Change
 */
@ApiMayChange
trait LoadEventQuery extends ReadJournal {

  /**
   * Load a single event on demand. The `Future` is completed with a `NoSuchElementException` if
   * the event for the given `persistenceId` and `sequenceNr` doesn't exist.
   */
  def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]]
}
