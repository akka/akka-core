/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.Done
import akka.actor.Address
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.external.ExternalShardAllocation
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.external.scaladsl.ExternalShardAllocationClient
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import docs.akka.cluster.sharding.typed.ShardingCompileOnlySpec.Basics.Counter

import scala.concurrent.Future

class ExternalShardAllocationCompileOnlySpec {
  val system: ActorSystem[_] = ???

  val sharding = ClusterSharding(system)

  // #entity
  val TypeKey = EntityTypeKey[Counter.Command]("Counter")

  val entity = Entity(TypeKey)(createBehavior = entityContext => Counter(entityContext.entityId))
    .withAllocationStrategy(ExternalShardAllocationStrategy(system, TypeKey.name))
  // #entity

  val shardRegion: ActorRef[ShardingEnvelope[Counter.Command]] =
    sharding.init(entity)

  // #client
  val client: ExternalShardAllocationClient = ExternalShardAllocation(system).clientFor(TypeKey.name)
  val done: Future[Done] = client.updateShardLocation("shard-id-1", Address("akka", "system", "127.0.0.1", 2552))
  // #client

}
