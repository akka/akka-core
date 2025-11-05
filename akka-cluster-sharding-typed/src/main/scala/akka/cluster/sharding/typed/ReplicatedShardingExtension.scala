/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.util.{ Map => JMap }

import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.internal.ReplicatedShardingExtensionImpl
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.persistence.typed.ReplicaId

/**
 * Extension for running Replicated Event Sourcing in sharding by starting one separate instance of sharding per replica.
 * The sharding instances can be confined to datacenters or cluster roles or run on the same set of cluster nodes.
 */
object ReplicatedShardingExtension extends ExtensionId[ReplicatedShardingExtension] {

  override def createExtension(system: ActorSystem[_]): ReplicatedShardingExtension =
    new ReplicatedShardingExtensionImpl(system)

  def get(system: ActorSystem[_]): ReplicatedShardingExtension = apply(system)

}

/**
 * Not for user extension.
 */
@DoNotInherit
trait ReplicatedShardingExtension extends Extension {

  /**
   * Init one instance sharding per replica in the given settings and return a [[ReplicatedSharding]] representing those.
   *
   * @tparam M The type of messages the replicated event sourced actor accepts
   *
   * Note, multiple calls on the same node will not start new sharding instances but will return a new instance of [[ReplicatedSharding]]
   */
  def init[M](settings: ReplicatedEntityProvider[M]): ReplicatedSharding[M]

  /**
   * Init one instance sharding per replica in the given settings and return a [[ReplicatedSharding]] representing those.
   *
   * @param thisReplica If provided saves messages being forwarded to sharding for this replica
   * @tparam M The type of messages the replicated event sourced actor accepts
   *
   * Note, multiple calls on the same node will not start new sharding instances but will return a new instance of [[ReplicatedSharding]]
   */
  def init[M](thisReplica: ReplicaId, settings: ReplicatedEntityProvider[M]): ReplicatedSharding[M]
}

/**
 * Represents the sharding instances for the replicas of one Replicated Event Sourcing entity type
 *
 * Not for user extension.
 */
@DoNotInherit
trait ReplicatedSharding[M] {

  /**
   * Scala API: Returns the entity ref for each replica for user defined routing/replica selection
   */
  def entityRefsFor(entityId: String): Map[ReplicaId, EntityRef[M]]

  /**
   * Java API: Returns the entity ref for each replica for user defined routing/replica selection
   */
  def getEntityRefsFor(entityId: String): JMap[ReplicaId, javadsl.EntityRef[M]]
}
