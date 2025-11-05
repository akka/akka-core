/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import akka.annotation.InternalApi
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 *
 * Used for store failures. Private to akka as only internal supervision strategies should use it.
 */
@InternalApi
final private[akka] class DurableStateStoreException(msg: String, cause: Throwable)
    extends RuntimeException(msg, cause) {
  def this(persistenceId: PersistenceId, sequenceNr: Long, cause: Throwable) =
    this(s"Failed to persist state with sequence number [$sequenceNr] for persistenceId [${persistenceId.id}]", cause)
}
