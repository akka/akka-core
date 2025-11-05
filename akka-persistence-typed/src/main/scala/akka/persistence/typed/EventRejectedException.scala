/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

/**
 * Thrown if a journal rejects an event e.g. due to a serialization error.
 */
final class EventRejectedException(persistenceId: PersistenceId, sequenceNr: Long, cause: Throwable)
    extends RuntimeException(s"Rejected event, persistenceId [${persistenceId.id}], sequenceNr [$sequenceNr]", cause)
