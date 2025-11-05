/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

/**
 * Identifies a replica in Replicated Event Sourcing, could be a datacenter name or a logical identifier.
 */
final case class ReplicaId(id: String)
