/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed;

import static akka.Done.done;
import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.javadsl.*;
import com.typesafe.config.ConfigFactory;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ReplicatedEventSourcingTest extends JUnitSuite {

  static final class TestBehavior
      extends ReplicatedEventSourcedBehavior<TestBehavior.Command, String, Set<String>> {
    interface Command {}

    static final class GetState implements Command {
      final ActorRef<State> replyTo;

      public GetState(ActorRef<State> replyTo) {
        this.replyTo = replyTo;
      }
    }

    static final class StoreMe implements Command {
      final String text;
      final ActorRef<Done> replyTo;

      public StoreMe(String text, ActorRef<Done> replyTo) {
        this.text = text;
        this.replyTo = replyTo;
      }
    }

    static final class StoreUs implements Command {
      final List<String> texts;
      final ActorRef<Done> replyTo;

      public StoreUs(List<String> texts, ActorRef<Done> replyTo) {
        this.texts = texts;
        this.replyTo = replyTo;
      }
    }

    static final class GetReplica implements Command {
      final ActorRef<ReplicaId> replyTo;

      public GetReplica(ActorRef<ReplicaId> replyTo) {
        this.replyTo = replyTo;
      }
    }

    static final class State {
      final Set<String> texts;

      public State(Set<String> texts) {
        this.texts = texts;
      }
    }

    enum Stop implements Command {
      INSTANCE
    }

    public static Behavior<Command> create(
        String entityId, ReplicaId replicaId, Set<ReplicaId> allReplicas) {
      return ReplicatedEventSourcing.commonJournalConfig(
          new ReplicationId("ReplicatedEventSourcingTest", entityId, replicaId),
          allReplicas,
          PersistenceTestKitReadJournal.Identifier(),
          TestBehavior::new);
    }

    private TestBehavior(ReplicationContext replicationContext) {
      super(replicationContext);
    }

    @Override
    public String journalPluginId() {
      return PersistenceTestKitPlugin.PluginId();
    }

    @Override
    public Set<String> emptyState() {
      return Collections.emptySet();
    }

    @Override
    public CommandHandler<Command, String, Set<String>> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(
              StoreMe.class,
              (StoreMe cmd) -> Effect().persist(cmd.text).thenRun(__ -> cmd.replyTo.tell(done())))
          .onCommand(
              StoreUs.class,
              (StoreUs cmd) -> Effect().persist(cmd.texts).thenRun(__ -> cmd.replyTo.tell(done())))
          .onCommand(
              GetState.class,
              (GetState get) ->
                  Effect()
                      .none()
                      .thenRun(state -> get.replyTo.tell(new State(new HashSet<>(state)))))
          .onCommand(
              GetReplica.class,
              (GetReplica cmd) ->
                  Effect()
                      .none()
                      .thenRun(() -> cmd.replyTo.tell(getReplicationContext().replicaId())))
          .onCommand(Stop.class, __ -> Effect().stop())
          .build();
    }

    @Override
    public EventHandler<Set<String>, String> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onAnyEvent(
              (state, text) -> {
                // FIXME mutable - state I don't remember if we support or not so defensive copy for
                // now
                Set<String> newSet = new HashSet<>(state);
                newSet.add(text);
                return newSet;
              });
    }
  }

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(
          ConfigFactory.parseString(
                  "akka.loglevel = INFO\n" + "akka.loggers = [\"akka.testkit.TestEventListener\"]")
              .withFallback(PersistenceTestKitPlugin.getInstance().config()));

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  // minimal test, full coverage over in ReplicatedEventSourcingSpec
  @Test
  public void replicatedEventSourcingReplicationTest() {
    ReplicaId dcA = new ReplicaId("DC-A");
    ReplicaId dcB = new ReplicaId("DC-B");
    ReplicaId dcC = new ReplicaId("DC-C");
    Set<ReplicaId> allReplicas = new HashSet<>(Arrays.asList(dcA, dcB, dcC));

    ActorRef<TestBehavior.Command> replicaA =
        testKit.spawn(TestBehavior.create("id1", dcA, allReplicas));
    ActorRef<TestBehavior.Command> replicaB =
        testKit.spawn(TestBehavior.create("id1", dcB, allReplicas));
    ActorRef<TestBehavior.Command> replicaC =
        testKit.spawn(TestBehavior.create("id1", dcC, allReplicas));

    TestProbe<Object> probe = testKit.createTestProbe();
    replicaA.tell(new TestBehavior.GetReplica(probe.ref().narrow()));
    assertEquals("DC-A", probe.expectMessageClass(ReplicaId.class).id());

    replicaA.tell(new TestBehavior.StoreMe("stored-to-a", probe.ref().narrow()));
    replicaB.tell(new TestBehavior.StoreMe("stored-to-b", probe.ref().narrow()));
    replicaC.tell(new TestBehavior.StoreMe("stored-to-c", probe.ref().narrow()));
    probe.receiveSeveralMessages(3);

    probe.awaitAssert(
        () -> {
          replicaA.tell(new TestBehavior.GetState(probe.ref().narrow()));
          TestBehavior.State reply = probe.expectMessageClass(TestBehavior.State.class);
          assertEquals(
              new HashSet<>(Arrays.asList("stored-to-a", "stored-to-b", "stored-to-c")),
              reply.texts);
          return null;
        });
    probe.awaitAssert(
        () -> {
          replicaB.tell(new TestBehavior.GetState(probe.ref().narrow()));
          TestBehavior.State reply = probe.expectMessageClass(TestBehavior.State.class);
          assertEquals(
              new HashSet<>(Arrays.asList("stored-to-a", "stored-to-b", "stored-to-c")),
              reply.texts);
          return null;
        });
    probe.awaitAssert(
        () -> {
          replicaC.tell(new TestBehavior.GetState(probe.ref().narrow()));
          TestBehavior.State reply = probe.expectMessageClass(TestBehavior.State.class);
          assertEquals(
              new HashSet<>(Arrays.asList("stored-to-a", "stored-to-b", "stored-to-c")),
              reply.texts);
          return null;
        });
  }
}
