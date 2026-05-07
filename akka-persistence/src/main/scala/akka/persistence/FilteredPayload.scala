/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

/**
 * In some use cases with projections and events by slice filtered events needs to be stored in the journal
 * to keep the sequence numbers for a given persistence id gap free. This placeholder payload is used for those
 * cases and serialized down to a 0-byte representation when stored in the database.
 *
 * This payload is not in general expected to show up for users but in some scenarios/queries it may.
 *
 * In the typed queries `EventEnvelope` this should be flagged as `filtered` and turned into a non-present payload
 * by the query plugin implementations.
 */
case object FilteredPayload
