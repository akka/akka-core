/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import static jdocs.akka.cluster.sharding.typed.ShardingCompileOnlyTest.Counter;

import akka.Done;
import akka.actor.Address;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.external.ExternalShardAllocation;
import akka.cluster.sharding.external.ExternalShardAllocationStrategy;
import akka.cluster.sharding.external.javadsl.ExternalShardAllocationClient;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import java.util.concurrent.CompletionStage;

public class ExternalShardAllocationCompileOnlyTest {

  void example() {
    ActorSystem<?> system = null;

    ClusterSharding sharding = ClusterSharding.get(system);

    // #entity
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");

    ActorRef<ShardingEnvelope<Counter.Command>> shardRegion =
        sharding.init(
            Entity.of(typeKey, ctx -> Counter.create(ctx.getEntityId()))
                .withAllocationStrategy(
                    ExternalShardAllocationStrategy.create(system, typeKey.name())));
    // #entity

    // #client
    ExternalShardAllocationClient client =
        ExternalShardAllocation.get(system).getClient(typeKey.name());
    CompletionStage<Done> done =
        client.setShardLocation("shard-id-1", new Address("akka", "system", "127.0.0.1", 2552));
    // #client

  }
}
