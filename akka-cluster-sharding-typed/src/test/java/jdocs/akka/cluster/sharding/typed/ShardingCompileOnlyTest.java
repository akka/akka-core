/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import java.time.Duration;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

// #import
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.persistence.typed.PersistenceId;

// #import

// #get-shard-region-state
import akka.cluster.sharding.typed.GetShardRegionState;
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState;

// #get-shard-region-state
// #get-cluster-sharding-stats
import akka.cluster.sharding.typed.GetClusterShardingStats;
import akka.cluster.sharding.ShardRegion.ClusterShardingStats;

// #get-cluster-sharding-stats

import jdocs.akka.persistence.typed.BlogPostEntity;

interface ShardingCompileOnlyTest {

  // #counter
  public class Counter extends AbstractBehavior<Counter.Command> {

    public interface Command {}

    public enum Increment implements Command {
      INSTANCE
    }

    public static class GetValue implements Command {
      private final ActorRef<Integer> replyTo;

      public GetValue(ActorRef<Integer> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static Behavior<Command> create(String entityId) {
      return Behaviors.setup(context -> new Counter(context, entityId));
    }

    private final String entityId;
    private int value = 0;

    private Counter(ActorContext<Command> context, String entityId) {
      super(context);
      this.entityId = entityId;
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Increment.class, msg -> onIncrement())
          .onMessage(GetValue.class, this::onGetValue)
          .build();
    }

    private Behavior<Command> onIncrement() {
      value++;
      return this;
    }

    private Behavior<Command> onGetValue(GetValue msg) {
      msg.replyTo.tell(value);
      return this;
    }
  }
  // #counter

  // #counter-passivate
  public class Counter2 extends AbstractBehavior<Counter2.Command> {

    public interface Command {}

    private enum Idle implements Command {
      INSTANCE
    }

    public enum GoodByeCounter implements Command {
      INSTANCE
    }

    public enum Increment implements Command {
      INSTANCE
    }

    public static class GetValue implements Command {
      private final ActorRef<Integer> replyTo;

      public GetValue(ActorRef<Integer> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static Behavior<Command> create(
        ActorRef<ClusterSharding.ShardCommand> shard, String entityId) {
      return Behaviors.setup(
          ctx -> {
            ctx.setReceiveTimeout(Duration.ofSeconds(30), Idle.INSTANCE);
            return new Counter2(ctx, shard, entityId);
          });
    }

    private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final String entityId;
    private int value = 0;

    private Counter2(
        ActorContext<Command> context,
        ActorRef<ClusterSharding.ShardCommand> shard,
        String entityId) {
      super(context);
      this.shard = shard;
      this.entityId = entityId;
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Increment.class, msg -> onIncrement())
          .onMessage(GetValue.class, this::onGetValue)
          .onMessage(Idle.class, msg -> onIdle())
          .onMessage(GoodByeCounter.class, msg -> onGoodByeCounter())
          .build();
    }

    private Behavior<Command> onIncrement() {
      value++;
      return this;
    }

    private Behavior<Command> onGetValue(GetValue msg) {
      msg.replyTo.tell(value);
      return this;
    }

    private Behavior<Command> onIdle() {
      // after receive timeout
      shard.tell(new ClusterSharding.Passivate<>(getContext().getSelf()));
      return this;
    }

    private Behavior<Command> onGoodByeCounter() {
      // the stopMessage, used for rebalance and passivate
      return Behaviors.stopped();
    }
  }
  // #counter-passivate

  public static void initPassivateExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    ClusterSharding sharding = ClusterSharding.get(system);

    // #counter-passivate-init

    EntityTypeKey<Counter2.Command> typeKey =
        EntityTypeKey.create(Counter2.Command.class, "Counter");

    sharding.init(
        Entity.of(typeKey, ctx -> Counter2.create(ctx.getShard(), ctx.getEntityId()))
            .withStopMessage(Counter2.GoodByeCounter.INSTANCE));
    // #counter-passivate-init
  }

  public static void example() {

    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");

    // #sharding-extension
    ClusterSharding sharding = ClusterSharding.get(system);
    // #sharding-extension

    // #init
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");

    ActorRef<ShardingEnvelope<Counter.Command>> shardRegion =
        sharding.init(Entity.of(typeKey, ctx -> Counter.create(ctx.getEntityId())));
    // #init

    // #send
    EntityRef<Counter.Command> counterOne = sharding.entityRefFor(typeKey, "counter-1");
    counterOne.tell(Counter.Increment.INSTANCE);

    shardRegion.tell(new ShardingEnvelope<>("counter-1", Counter.Increment.INSTANCE));
    // #send

    ActorRef<ClusterSharding.ShardCommand> shard = sharding.shard(typeKey);
  }

  public static void roleExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    ClusterSharding sharding = ClusterSharding.get(system);

    // #roles
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");

    ActorRef<ShardingEnvelope<Counter.Command>> shardRegionOrProxy =
        sharding.init(
            Entity.of(typeKey, ctx -> Counter.create(ctx.getEntityId())).withRole("backend"));
    // #roles
  }

  public static void persistenceExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    ClusterSharding sharding = ClusterSharding.get(system);

    // #persistence
    EntityTypeKey<BlogPostEntity.Command> blogTypeKey =
        EntityTypeKey.create(BlogPostEntity.Command.class, "BlogPost");

    sharding.init(
        Entity.of(
            blogTypeKey,
            entityContext ->
                BlogPostEntity.create(
                    entityContext.getEntityId(),
                    PersistenceId.of(
                        entityContext.getEntityTypeKey().name(), entityContext.getEntityId()))));
    // #persistence
  }

  public static void dataCenterExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");
    String entityId = "a";

    // #proxy-dc
    ActorRef<ShardingEnvelope<Counter.Command>> proxy =
        ClusterSharding.get(system)
            .init(
                Entity.of(typeKey, ctx -> Counter.create(ctx.getEntityId())).withDataCenter("dc2"));
    // #proxy-dc

    // #proxy-dc-entityref
    // it must still be started before usage
    ClusterSharding.get(system)
        .init(Entity.of(typeKey, ctx -> Counter.create(ctx.getEntityId())).withDataCenter("dc2"));

    EntityRef<Counter.Command> entityRef =
        ClusterSharding.get(system).entityRefFor(typeKey, entityId, "dc2");
    // #proxy-dc-entityref
  }

  public static void shardRegionQqueryExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    ActorRef<CurrentShardRegionState> replyMessageAdapter = null;
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");

    // #get-shard-region-state
    ActorRef<CurrentShardRegionState> replyTo = replyMessageAdapter;

    ClusterSharding.get(system).shardState().tell(new GetShardRegionState(typeKey, replyTo));
    // #get-shard-region-state
  }

  public static void shardingStatsQqueryExample() {
    ActorSystem system = ActorSystem.create(Behaviors.empty(), "ShardingExample");
    ActorRef<ClusterShardingStats> replyMessageAdapter = null;
    EntityTypeKey<Counter.Command> typeKey = EntityTypeKey.create(Counter.Command.class, "Counter");

    // #get-cluster-sharding-stats
    ActorRef<ClusterShardingStats> replyTo = replyMessageAdapter;
    Duration timeout = Duration.ofSeconds(5);

    ClusterSharding.get(system)
        .shardState()
        .tell(new GetClusterShardingStats(typeKey, timeout, replyTo));
    // #get-cluster-sharding-stats
  }
}
