/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.ConsistentHashingShardAllocationStrategy
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

class ConsistentHashingShardAllocationCompileOnlySpec {

  // #building
  object Building {
    val TypeKey = EntityTypeKey[Command]("Building")

    val NumberOfShards = 100

    final class MessageExtractor extends ShardingMessageExtractor[ShardingEnvelope[Command], Command] {

      override def entityId(envelope: ShardingEnvelope[Command]): String =
        envelope.entityId

      override def shardId(entityId: String): String =
        math.abs(entityId.hashCode % NumberOfShards).toString

      override def unwrapMessage(envelope: ShardingEnvelope[Command]): Command =
        envelope.message
    }

    sealed trait Command

    def apply(entityId: String): Behavior[Command] = ???
  }

  // #building

  // #device
  object Device {
    val TypeKey = EntityTypeKey[Command]("Device")

    final class MessageExtractor extends ShardingMessageExtractor[ShardingEnvelope[Command], Command] {

      override def entityId(envelope: ShardingEnvelope[Command]): String =
        envelope.entityId

      override def shardId(entityId: String): String = {
        // Use same shardId as the Building to colocate Building and Device
        // we have the buildingId as prefix in the entityId
        val buildingId = entityId.split(':').head
        math.abs(buildingId.hashCode % Building.NumberOfShards).toString
      }

      override def unwrapMessage(envelope: ShardingEnvelope[Command]): Command =
        envelope.message
    }

    sealed trait Command

    def apply(entityId: String): Behavior[Command] = ???
  }

  // #device

  val system: ActorSystem[_] = ???

  // #init
  ClusterSharding(system).init(
    Entity(Building.TypeKey)(createBehavior = entityContext => Building(entityContext.entityId))
      .withMessageExtractor(new Building.MessageExtractor)
      .withAllocationStrategy(new ConsistentHashingShardAllocationStrategy(rebalanceLimit = 10)))

  ClusterSharding(system).init(
    Entity(Device.TypeKey)(createBehavior = entityContext => Device(entityContext.entityId))
      .withMessageExtractor(new Device.MessageExtractor)
      .withAllocationStrategy(new ConsistentHashingShardAllocationStrategy(rebalanceLimit = 10)))
  // #init

}
