/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

object ReplicationId {
  private[akka] val Separator = '|'
  def fromString(id: String): ReplicationId = {
    val split = id.split("\\|")
    if (split.length == 3)
      ReplicationId(split(0), split(1), ReplicaId(split(2)))
    else if (split.length == 2)
      ReplicationId(split(0), split(1), ReplicaId.empty) // migration from non-replicated
    else
      throw new IllegalArgumentException(s"invalid replication id [$id]")
  }

  def isReplicationId(id: String): Boolean = {
    id.count(_ == Separator) == 2
  }

  /**
   * @param typeName The name of the entity type e.g. account, user. Made part of the persistence id so that entity ids don't need to be unique across different replicated entities
   * @param entityId The unique entity id
   * @param replicaId The unique identity for this entity. The underlying persistence id will include the replica,
   *                  unless the replicaId is `ReplicaId.empty`
   */
  def apply(typeName: String, entityId: String, replicaId: ReplicaId): ReplicationId =
    new ReplicationId(typeName, entityId, replicaId)
}

/**
 * @param typeName The name of the entity type e.g. account, user. Made part of the persistence id so that entity ids don't need to be unique across different replicated entities
 * @param entityId The unique entity id
 * @param replicaId The unique identity for this entity. The underlying persistence id will include the replica,
 *                  unless the replicaId is `ReplicaId.empty`
 */
final class ReplicationId(val typeName: String, val entityId: String, val replicaId: ReplicaId) {
  import ReplicationId._
  if (typeName.contains(Separator))
    throw new IllegalArgumentException(
      s"entityTypeHint [$typeName] contains [$Separator] which is a reserved character")

  if (entityId.contains(Separator))
    throw new IllegalArgumentException(s"entityId [$entityId] contains [$Separator] which is a reserved character")

  if (replicaId.id.contains(Separator))
    throw new IllegalArgumentException(
      s"replicaId [${replicaId.id}] contains [$Separator] which is a reserved character")

  private val id: String = {
    if (replicaId.id.isEmpty) s"$typeName$Separator$entityId"
    else s"$typeName$Separator$entityId$Separator${replicaId.id}"
  }

  def persistenceId: PersistenceId = PersistenceId.ofUniqueId(id)

  override def toString: String = s"ReplicationId($typeName, $entityId, $replicaId)"

  def withReplica(newReplica: ReplicaId): ReplicationId = {
    new ReplicationId(typeName, entityId, newReplica)
  }
}
