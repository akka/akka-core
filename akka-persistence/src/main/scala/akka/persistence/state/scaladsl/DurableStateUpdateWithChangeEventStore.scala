/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.state.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.annotation.ApiMayChange

/**
 * API May Change: API for updating durable state objects and storing additional change event.
 *
 * For Java API see [[akka.persistence.state.javadsl.DurableStateUpdateWithChangeEventStore]].
 */
@ApiMayChange
trait DurableStateUpdateWithChangeEventStore[A] extends DurableStateUpdateStore[A] {

  /**
   * The `changeEvent` is written to the event journal.
   * Same `persistenceId` is used in the journal and the `revision` is used as `sequenceNr`.
   *
   * @param revision sequence number for optimistic locking. starts at 1.
   */
  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String, changeEvent: Any): Future[Done]

  def deleteObject(persistenceId: String, revision: Long, changeEvent: Any): Future[Done]
}
