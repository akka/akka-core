/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.MemberStatus;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicatedEventSourcing;
import akka.persistence.typed.javadsl.ReplicationContext;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

// format: OFF
import static akka.cluster.sharding.typed.ReplicatedShardingTest.ProxyActor.ALL_REPLICAS;
import static org.junit.Assert.assertEquals;
// format: ON

public class ReplicatedShardingTest extends JUnitSuite {

  static class MyReplicatedStringSet
      extends ReplicatedEventSourcedBehavior<MyReplicatedStringSet.Command, String, Set<String>> {
    interface Command {}

    static class Add implements Command {
      public final String text;

      public Add(String text) {
        this.text = text;
      }
    }

    static class GetTexts implements Command {
      public final ActorRef<Texts> replyTo;

      public GetTexts(ActorRef<Texts> replyTo) {
        this.replyTo = replyTo;
      }
    }

    static class Texts {
      public final Set<String> texts;

      public Texts(Set<String> texts) {
        this.texts = texts;
      }
    }

    static Behavior<Command> create(ReplicationId replicationId) {
      return ReplicatedEventSourcing.commonJournalConfig(
          replicationId,
          ALL_REPLICAS,
          PersistenceTestKitReadJournal.Identifier(),
          MyReplicatedStringSet::new);
    }

    private MyReplicatedStringSet(ReplicationContext replicationContext) {
      super(replicationContext);
    }

    @Override
    public Set<String> emptyState() {
      return new HashSet<>();
    }

    @Override
    public CommandHandler<Command, String, Set<String>> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(Add.class, add -> Effect().persist(add.text))
          .onCommand(
              GetTexts.class,
              (state, get) -> {
                // protective copy
                get.replyTo.tell(new Texts(new HashSet<>(state)));
                return Effect().none();
              })
          .build();
    }

    @Override
    public EventHandler<Set<String>, String> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onAnyEvent(
              (state, text) -> {
                state.add(text);
                return state;
              });
    }
  }

  public static class ProxyActor extends AbstractBehavior<ProxyActor.Command> {
    interface Command {}

    public static final class ForwardToRandom implements Command {
      public final String entityId;
      public final MyReplicatedStringSet.Command message;

      public ForwardToRandom(String entityId, MyReplicatedStringSet.Command message) {
        this.entityId = entityId;
        this.message = message;
      }
    }

    public static final class ForwardToAll implements Command {
      public final String entityId;
      public final MyReplicatedStringSet.Command message;

      public ForwardToAll(String entityId, MyReplicatedStringSet.Command message) {
        this.entityId = entityId;
        this.message = message;
      }
    }

    public static Behavior<Command> create() {
      return Behaviors.setup(ProxyActor::new);
    }

    public static final Set<ReplicaId> ALL_REPLICAS =
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    new ReplicaId("DC-A"), new ReplicaId("DC-B"), new ReplicaId("DC-C"))));

    private final ReplicatedSharding<MyReplicatedStringSet.Command> replicatedSharding;

    private ProxyActor(ActorContext<Command> context) {
      super(context);

      // #bootstrap
      ReplicatedEntityProvider<MyReplicatedStringSet.Command> replicatedEntityProvider =
          ReplicatedEntityProvider.create(
              MyReplicatedStringSet.Command.class,
              "StringSet",
              ALL_REPLICAS,
              // factory for replicated entity for a given replica
              (entityTypeKey, replicaId) ->
                  ReplicatedEntity.create(
                      replicaId,
                      // use the replica id as typekey for sharding to get one sharding instance
                      // per replica
                      Entity.of(
                              entityTypeKey,
                              entityContext ->
                                  // factory for the entity for a given entity in that replica
                                  MyReplicatedStringSet.create(
                                      ReplicationId.fromString(entityContext.getEntityId())))
                          // potentially use replica id as role or dc in Akka multi dc for the
                          // sharding instance
                          // to control where replicas will live
                          // .withDataCenter(replicaId.id()))
                          .withRole(replicaId.id())));

      ReplicatedShardingExtension extension =
          ReplicatedShardingExtension.get(getContext().getSystem());
      ReplicatedSharding<MyReplicatedStringSet.Command> replicatedSharding =
          extension.init(replicatedEntityProvider);
      // #bootstrap

      this.replicatedSharding = replicatedSharding;
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(ForwardToRandom.class, this::onForwardToRandom)
          .onMessage(ForwardToAll.class, this::onForwardToAll)
          .build();
    }

    private Behavior<Command> onForwardToRandom(ForwardToRandom forwardToRandom) {
      Map<ReplicaId, EntityRef<MyReplicatedStringSet.Command>> refs =
          replicatedSharding.getEntityRefsFor(forwardToRandom.entityId);
      int chosenIdx = ThreadLocalRandom.current().nextInt(refs.size());
      new ArrayList<>(refs.values()).get(chosenIdx).tell(forwardToRandom.message);
      return this;
    }

    private Behavior<Command> onForwardToAll(ForwardToAll forwardToAll) {
      // #all-entity-refs
      Map<ReplicaId, EntityRef<MyReplicatedStringSet.Command>> refs =
          replicatedSharding.getEntityRefsFor(forwardToAll.entityId);
      refs.forEach((replicaId, ref) -> ref.tell(forwardToAll.message));
      // #all-entity-refs
      return this;
    }
  }

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(
          ConfigFactory.parseString(
                  " akka.loglevel = DEBUG\n"
                      + "      akka.loggers = [\"akka.testkit.SilenceAllTestEventListener\"]\n"
                      + "      akka.actor.provider = \"cluster\"\n"
                      + "      # pretend we're a node in all dc:s\n"
                      + "      akka.cluster.roles = [\"DC-A\", \"DC-B\", \"DC-C\"]\n"
                      + "      akka.remote.artery.canonical.port = 0")
              .withFallback(PersistenceTestKitPlugin.getInstance().config()));

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void formClusterAndInteractWithReplicas() {
    // join ourselves to form a one node cluster
    Cluster node = Cluster.get(testKit.system());
    node.manager().tell(new Join(node.selfMember().address()));
    TestProbe<Object> testProbe = testKit.createTestProbe();
    testProbe.awaitAssert(
        () -> {
          assertEquals(MemberStatus.up(), node.selfMember().status());
          return null;
        });

    // forward messages to replicas
    ActorRef<ProxyActor.Command> proxy = testKit.spawn(ProxyActor.create());

    proxy.tell(new ProxyActor.ForwardToAll("id1", new MyReplicatedStringSet.Add("to-all")));
    proxy.tell(new ProxyActor.ForwardToRandom("id1", new MyReplicatedStringSet.Add("to-random")));

    testProbe.awaitAssert(
        () -> {
          TestProbe<MyReplicatedStringSet.Texts> responseProbe = testKit.createTestProbe();
          proxy.tell(
              new ProxyActor.ForwardToAll(
                  "id1", new MyReplicatedStringSet.GetTexts(responseProbe.ref())));
          List<MyReplicatedStringSet.Texts> responses = responseProbe.receiveSeveralMessages(3);
          Set<String> uniqueTexts =
              responses.stream().flatMap(res -> res.texts.stream()).collect(Collectors.toSet());
          assertEquals(new HashSet<>(Arrays.asList("to-all", "to-random")), uniqueTexts);
          return null;
        });
  }
}
