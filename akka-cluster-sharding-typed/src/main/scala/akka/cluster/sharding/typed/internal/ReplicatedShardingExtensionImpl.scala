/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import java.util.{ Map => JMap }
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.sharding.typed.ReplicatedEntityProvider
import akka.cluster.sharding.typed.ReplicatedSharding
import akka.cluster.sharding.typed.ReplicatedShardingExtension
import akka.cluster.sharding.typed.ShardingDirectReplication
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ReplicatedShardingExtensionImpl(system: ActorSystem[_]) extends ReplicatedShardingExtension {

  private val counter = new AtomicLong(0)

  private val logger = LoggerFactory.getLogger(getClass)

  override def init[M](settings: ReplicatedEntityProvider[M]): ReplicatedSharding[M] =
    initInternal(None, settings)

  override def init[M](thisReplica: ReplicaId, settings: ReplicatedEntityProvider[M]): ReplicatedSharding[M] =
    initInternal(Some(thisReplica), settings)

  private def initInternal[M](
      thisReplica: Option[ReplicaId],
      settings: ReplicatedEntityProvider[M]): ReplicatedSharding[M] = {
    require(settings.replicas.nonEmpty, "Replicas must not be empty")
    val sharding = ClusterSharding(system)
    val initializedReplicas = settings.replicas.map {
      case (replicaSettings, typeName) =>
        // start up a sharding instance per replica id
        logger.info(
          "Starting Replicated Event Sourcing sharding for replica [{}] (ShardType: [{}], typeName [{}])",
          replicaSettings.replicaId.id,
          replicaSettings.entity.typeKey.name)
        val regionOrProxy = sharding.init(replicaSettings.entity)
        (
          typeName,
          replicaSettings.replicaId,
          replicaSettings.entity.typeKey,
          regionOrProxy,
          replicaSettings.entity.dataCenter)
    }
    if (settings.directReplication) {
      val replicaToRegionOrProxy = initializedReplicas.map {
        case (_, replicaId, _, regionOrProxy, _) => replicaId -> regionOrProxy
      }.toMap
      val typeNameWithoutReplicaId = settings.replicas.head._2
      logger.info("Starting Replicated Event Sourcing Direct Replication")
      system.systemActorOf(
        ShardingDirectReplication(typeNameWithoutReplicaId, thisReplica, replicaToRegionOrProxy),
        s"directReplication-${counter.incrementAndGet()}")
    }

    val replicaToTypeKey = initializedReplicas.map {
      case (typeName, id, typeKey, _, dc) => id -> ((typeKey, dc, typeName))
    }.toMap
    new ReplicatedShardingImpl(sharding, replicaToTypeKey)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
@nowarn("msg=Use Akka Distributed Cluster")
private[akka] final class ReplicatedShardingImpl[M](
    sharding: ClusterSharding,
    replicaTypeKeys: Map[ReplicaId, (EntityTypeKey[M], Option[DataCenter], String)])
    extends ReplicatedSharding[M] {

  override def entityRefsFor(entityId: String): Map[ReplicaId, EntityRef[M]] =
    replicaTypeKeys.map {
      case (replicaId, (typeKey, dc, typeName)) =>
        replicaId -> (dc match {
          case None => sharding.entityRefFor(typeKey, ReplicationId(typeName, entityId, replicaId).persistenceId.id)
          case Some(dc) =>
            sharding.entityRefFor(typeKey, ReplicationId(typeName, entityId, replicaId).persistenceId.id, dc)
        })
    }

  override def getEntityRefsFor(entityId: String): JMap[ReplicaId, akka.cluster.sharding.typed.javadsl.EntityRef[M]] =
    entityRefsFor(entityId).transform((_, v) => v.asJava).asJava

}
