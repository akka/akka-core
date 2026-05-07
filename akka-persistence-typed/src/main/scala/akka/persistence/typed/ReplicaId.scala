/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

object ReplicaId extends scala.runtime.AbstractFunction1[String, ReplicaId] with java.io.Serializable {

  /**
   * When migrating from non-replicated to replicated the ReplicaId of where the original entity
   * was located should be empty.
   */
  val empty: ReplicaId = ReplicaId("")

  override def apply(id: String): ReplicaId =
    new ReplicaId(id)
}

/**
 * Identifies a replica in Replicated Event Sourcing, could be a datacenter name or a logical identifier.
 */
final case class ReplicaId(id: String) {
  override def toString: String = id
}
