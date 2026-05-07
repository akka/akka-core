/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.ReplicatedEntityProvider
import akka.cluster.sharding.typed.ReplicatedSharding
import akka.cluster.sharding.typed.ReplicatedShardingExtension
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import scala.annotation.nowarn

@nowarn("msg=Use Akka Distributed Cluster")
object ReplicatedShardingCompileOnlySpec {

  sealed trait Command

  val system: ActorSystem[_] = ???

  object MyEventSourcedBehavior {
    def apply(replicationId: ReplicationId): Behavior[Command] = ???
  }

  //#bootstrap
  ReplicatedEntityProvider[Command]("MyEntityType", Set(ReplicaId("DC-A"), ReplicaId("DC-B"))) {
    (entityTypeKey, replicaId) =>
      ReplicatedEntity(replicaId, Entity(entityTypeKey) { entityContext =>
        // the sharding entity id contains the business entityId, entityType, and replica id
        // which you'll need to create a ReplicatedEventSourcedBehavior
        val replicationId = ReplicationId.fromString(entityContext.entityId)
        MyEventSourcedBehavior(replicationId)
      })
  }
  //#bootstrap

  //#bootstrap-dc
  ReplicatedEntityProvider.perDataCenter("MyEntityType", Set(ReplicaId("DC-A"), ReplicaId("DC-B"))) { replicationId =>
    MyEventSourcedBehavior(replicationId)
  }
  //#bootstrap-dc

  //#bootstrap-role
  val provider = ReplicatedEntityProvider.perRole("MyEntityType", Set(ReplicaId("DC-A"), ReplicaId("DC-B"))) {
    replicationId =>
      MyEventSourcedBehavior(replicationId)
  }
  //#bootstrap-role

  //#sending-messages
  val myReplicatedSharding: ReplicatedSharding[Command] =
    ReplicatedShardingExtension(system).init(provider)

  val entityRefs: Map[ReplicaId, EntityRef[Command]] = myReplicatedSharding.entityRefsFor("myEntityId")
  //#sending-messages
}
