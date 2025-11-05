/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.javadsl;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import org.junit.ClassRule;
import org.junit.Rule;

public class ShardingEventSourcedEntityWithEnforcedRepliesCompileOnlyTest {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  interface Command {}

  static class Append implements Command {
    public final String s;
    public final ActorRef<String> replyTo;

    Append(String s, ActorRef<String> replyTo) {
      this.s = s;
      this.replyTo = replyTo;
    }
  }

  static class TestPersistentEntityWithEnforcedReplies
      extends EventSourcedBehaviorWithEnforcedReplies<Command, String, String> {

    public static final EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "HelloWorld");

    public static Behavior<Command> create(String entityId, PersistenceId persistenceId) {
      return new TestPersistentEntityWithEnforcedReplies(entityId, persistenceId);
    }

    private TestPersistentEntityWithEnforcedReplies(String entityId, PersistenceId persistenceId) {
      super(persistenceId);
    }

    @Override
    public String emptyState() {
      return "";
    }

    @Override
    public CommandHandlerWithReply<Command, String, String> commandHandler() {
      return newCommandHandlerWithReplyBuilder()
          .forAnyState()
          .onCommand(Append.class, this::add)
          .build();
    }

    private ReplyEffect<String, String> add(String state, Append cmd) {
      return Effect().persist(cmd.s).thenReply(cmd.replyTo, s -> "Ok");
    }

    @Override
    public EventHandler<String, String> eventHandler() {
      return newEventHandlerBuilder().forAnyState().onEvent(String.class, this::applyEvent).build();
    }

    private String applyEvent(String state, String evt) {
      if (state.trim().isEmpty()) return evt;
      else return state + "|" + evt;
    }
  }

  private void shardingForEventSourcedEntityWithReplies() {

    // initialize first time only
    Cluster cluster = Cluster.get(testKit.system());
    cluster.manager().tell(new Join(cluster.selfMember().address()));

    ClusterSharding sharding = ClusterSharding.get(testKit.system());

    sharding.init(
        Entity.of(
            TestPersistentEntityWithEnforcedReplies.ENTITY_TYPE_KEY,
            entityContext ->
                TestPersistentEntityWithEnforcedReplies.create(
                    entityContext.getEntityId(),
                    PersistenceId.of(
                        entityContext.getEntityTypeKey().name(), entityContext.getEntityId()))));
  }
}
