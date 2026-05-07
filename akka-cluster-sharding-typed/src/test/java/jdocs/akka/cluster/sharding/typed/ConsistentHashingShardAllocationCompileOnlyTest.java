/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.ConsistentHashingShardAllocationStrategy;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.ShardingMessageExtractor;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

public class ConsistentHashingShardAllocationCompileOnlyTest {

  // #building
  public static class Building extends AbstractBehavior<Building.Command> {

    static int NUMBER_OF_SHARDS = 100;

    static final class MessageExtractor
        extends ShardingMessageExtractor<ShardingEnvelope<Building.Command>, Building.Command> {
      @Override
      public String entityId(ShardingEnvelope<Command> envelope) {
        return envelope.entityId();
      }

      @Override
      public String shardId(String entityId) {
        return String.valueOf(Math.abs(entityId.hashCode() % NUMBER_OF_SHARDS));
      }

      @Override
      public Command unwrapMessage(ShardingEnvelope<Command> envelope) {
        return envelope.message();
      }
    }

    static EntityTypeKey<Building.Command> typeKey =
        EntityTypeKey.create(Building.Command.class, "Building");

    public interface Command {}

    public static Behavior<Building.Command> create(String entityId) {
      return Behaviors.setup(context -> new Building(context, entityId));
    }

    private Building(ActorContext<Building.Command> context, String entityId) {
      super(context);
    }

    @Override
    public Receive<Building.Command> createReceive() {
      return newReceiveBuilder().build();
    }
  }

  // #building

  // #device
  public static class Device extends AbstractBehavior<Device.Command> {

    static final class MessageExtractor
        extends ShardingMessageExtractor<ShardingEnvelope<Device.Command>, Device.Command> {
      @Override
      public String entityId(ShardingEnvelope<Command> envelope) {
        return envelope.entityId();
      }

      @Override
      public String shardId(String entityId) {
        // Use same shardId as the Building to colocate Building and Device
        // we have the buildingId as prefix in the entityId
        String buildingId = entityId.split(":")[0];
        return String.valueOf(Math.abs(buildingId.hashCode() % Building.NUMBER_OF_SHARDS));
      }

      @Override
      public Command unwrapMessage(ShardingEnvelope<Command> envelope) {
        return envelope.message();
      }
    }

    static EntityTypeKey<Device.Command> typeKey =
        EntityTypeKey.create(Device.Command.class, "Device");

    public interface Command {}

    public static Behavior<Device.Command> create(String entityId) {
      return Behaviors.setup(context -> new Device(context, entityId));
    }

    private Device(ActorContext<Device.Command> context, String entityId) {
      super(context);
    }

    @Override
    public Receive<Device.Command> createReceive() {
      return newReceiveBuilder().build();
    }
  }

  // #device

  void example() {
    ActorSystem<?> system = null;

    // #init
    int rebalanceLimit = 10;

    ClusterSharding.get(system)
        .init(
            Entity.of(Building.typeKey, ctx -> Building.create(ctx.getEntityId()))
                .withMessageExtractor(new Building.MessageExtractor())
                .withAllocationStrategy(
                    new ConsistentHashingShardAllocationStrategy(rebalanceLimit)));

    ClusterSharding.get(system)
        .init(
            Entity.of(Device.typeKey, ctx -> Device.create(ctx.getEntityId()))
                .withMessageExtractor(new Device.MessageExtractor())
                .withAllocationStrategy(
                    new ConsistentHashingShardAllocationStrategy(rebalanceLimit)));
    // #init
  }
}
