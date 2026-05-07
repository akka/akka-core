/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.util.{ Set => JSet }

import scala.collection.immutable
import scala.reflect.ClassTag

import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.internal.EntityTypeKeyImpl
import akka.cluster.sharding.typed.javadsl.{ Entity => JEntity, EntityTypeKey => JEntityTypeKey }
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.ReplicationId.Separator
import scala.jdk.CollectionConverters._

object ReplicatedEntityProvider {

  /**
   * Java API:
   *
   * Provides full control over the [[ReplicatedEntity]] and the [[Entity]]
   * Most use cases can use the [[createPerDataCenter]] and [[createPerRole]]
   *
   * @tparam M The type of messages the replicated entity accepts
   */
  def create[M](
      messageClass: Class[M],
      typeName: String,
      allReplicaIds: JSet[ReplicaId],
      settingsPerReplicaFactory: akka.japi.function.Function2[JEntityTypeKey[M], ReplicaId, ReplicatedEntity[M]])
      : ReplicatedEntityProvider[M] = {
    implicit val classTag: ClassTag[M] = ClassTag(messageClass)
    apply[M](typeName, allReplicaIds.asScala.toSet)((key, replica) =>
      settingsPerReplicaFactory(key.asInstanceOf[EntityTypeKeyImpl[M]], replica))
  }

  /**
   * Scala API:
   *
   * Provides full control over the [[ReplicatedEntity]] and the [[Entity]]
   * Most use cases can use the [[perDataCenter]] and [[perRole]]
   *
   * @param typeName The type name used in the [[EntityTypeKey]]
   * @tparam M The type of messages the replicated entity accepts
   */
  def apply[M: ClassTag](typeName: String, allReplicaIds: Set[ReplicaId])(
      settingsPerReplicaFactory: (EntityTypeKey[M], ReplicaId) => ReplicatedEntity[M]): ReplicatedEntityProvider[M] = {
    new ReplicatedEntityProvider(allReplicaIds.map { replicaId =>
      if (typeName.contains(Separator))
        throw new IllegalArgumentException(s"typeName [$typeName] contains [$Separator] which is a reserved character")

      val typeKey = EntityTypeKey[M](s"$typeName${Separator}${replicaId.id}")
      (settingsPerReplicaFactory(typeKey, replicaId), typeName)
    }.toVector, directReplication = true)
  }

  /**
   * Scala API
   *
   * Create a [[ReplicatedEntityProvider]] that uses the defaults for [[Entity]] when running in
   * ClusterSharding. A replica will be run per data center.
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def perDataCenter[M: ClassTag, E](typeName: String, allReplicaIds: Set[ReplicaId])(
      create: ReplicationId => Behavior[M]): ReplicatedEntityProvider[M] = {
    apply(typeName, allReplicaIds) { (typeKey, replicaId) =>
      ReplicatedEntity(replicaId, Entity(typeKey) { entityContext =>
        create(ReplicationId.fromString(entityContext.entityId))
      }.withDataCenter(replicaId.id))
    }
  }

  /**
   * Scala API
   *
   * Create a [[ReplicatedEntityProvider]] that uses the defaults for [[Entity]] when running in
   * ClusterSharding. The replicas in allReplicaIds should be roles used by nodes. A replica for each
   * entity will run on each role.
   */
  def perRole[M: ClassTag, E](typeName: String, allReplicaIds: Set[ReplicaId])(
      create: ReplicationId => Behavior[M]): ReplicatedEntityProvider[M] = {
    apply(typeName, allReplicaIds) { (typeKey, replicaId) =>
      ReplicatedEntity(replicaId, Entity(typeKey) { entityContext =>
        create(ReplicationId.fromString(entityContext.entityId))
      }.withRole(replicaId.id))
    }
  }

  /**
   * Java API
   *
   * Create a [[ReplicatedEntityProvider]] that uses the defaults for [[Entity]] when running in
   * ClusterSharding. A replica will be run per data center.
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def createPerDataCenter[M](
      messageClass: Class[M],
      typeName: String,
      allReplicaIds: JSet[ReplicaId],
      createBehavior: java.util.function.Function[ReplicationId, Behavior[M]]): ReplicatedEntityProvider[M] = {
    implicit val classTag: ClassTag[M] = ClassTag(messageClass)
    apply(typeName, allReplicaIds.asScala.toSet) { (typeKey, replicaId) =>
      ReplicatedEntity(replicaId, Entity(typeKey) { entityContext =>
        createBehavior(ReplicationId.fromString(entityContext.entityId))
      }.withDataCenter(replicaId.id))
    }
  }

  /**
   * Java API
   *
   * Create a [[ReplicatedEntityProvider]] that uses the defaults for [[Entity]] when running in
   * ClusterSharding.
   *
   * Map replicas to roles and then there will be a replica per role e.g. to match to availability zones/racks
   */
  def createPerRole[M](
      messageClass: Class[M],
      typeName: String,
      allReplicaIds: JSet[ReplicaId],
      createBehavior: akka.japi.function.Function[ReplicationId, Behavior[M]]): ReplicatedEntityProvider[M] = {
    implicit val classTag: ClassTag[M] = ClassTag(messageClass)
    apply(typeName, allReplicaIds.asScala.toSet) { (typeKey, replicaId) =>
      ReplicatedEntity(replicaId, Entity(typeKey) { entityContext =>
        createBehavior(ReplicationId.fromString(entityContext.entityId))
      }.withRole(replicaId.id))
    }
  }
}

/**
 *
 * @tparam M The type of messages the replicated entity accepts
 */
final class ReplicatedEntityProvider[M] private (
    val replicas: immutable.Seq[(ReplicatedEntity[M], String)],
    val directReplication: Boolean) {

  /**
   * Start direct replication over sharding when replicated sharding starts up, requires the entities
   * to also have it enabled through [[akka.persistence.typed.scaladsl.EventSourcedBehavior.withEventPublishing]]
   * or [[akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior.withEventPublishing]]
   * to work.
   *
   */
  def withDirectReplication(enabled: Boolean): ReplicatedEntityProvider[M] =
    new ReplicatedEntityProvider(replicas, directReplication = enabled)

}

object ReplicatedEntity {

  /**
   * Java API: Defines the [[akka.cluster.sharding.typed.javadsl.Entity]] to use for a given replica, note that the behavior
   * can be a [[akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior]] or an arbitrary non persistent
   * [[akka.actor.typed.Behavior]] but must never be a regular [[akka.persistence.typed.javadsl.EventSourcedBehavior]]
   * as that requires a single writer and that would cause it to have multiple writers.
   */
  def create[M](replicaId: ReplicaId, entity: JEntity[M, ShardingEnvelope[M]]): ReplicatedEntity[M] =
    apply(replicaId, entity.toScala)

  /**
   * Scala API: Defines the [[akka.cluster.sharding.typed.scaladsl.Entity]] to use for a given replica, note that the behavior
   * can be a behavior created with [[akka.persistence.typed.scaladsl.ReplicatedEventSourcing]] or an arbitrary non persistent
   * [[akka.actor.typed.Behavior]] but must never be a regular [[akka.persistence.typed.scaladsl.EventSourcedBehavior]]
   * as that requires a single writer and that would cause it to have multiple writers.
   */
  def apply[M](replicaId: ReplicaId, entity: Entity[M, ShardingEnvelope[M]]): ReplicatedEntity[M] =
    new ReplicatedEntity(replicaId, entity)
}

/**
 * Settings for a specific replica id in replicated sharding
 * Currently only Entity's with ShardingEnvelope are supported but this may change in the future
 */
final class ReplicatedEntity[M] private (val replicaId: ReplicaId, val entity: Entity[M, ShardingEnvelope[M]])
