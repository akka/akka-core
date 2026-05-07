/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.state.javadsl

import java.util.concurrent.CompletionStage

import akka.Done

/**
 * API for updating durable state objects.
 *
 * For Scala API see [[akka.persistence.state.scaladsl.DurableStateUpdateStore]].
 *
 * See also [[DurableStateUpdateWithChangeEventStore]]
 */
trait DurableStateUpdateStore[A] extends DurableStateStore[A] {

  /**
   * @param revision sequence number for optimistic locking. starts at 1.
   */
  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done]

  @deprecated(message = "Use the deleteObject overload with revision instead.", since = "2.6.20")
  def deleteObject(persistenceId: String): CompletionStage[Done]

  def deleteObject(persistenceId: String, revision: Long): CompletionStage[Done]
}
