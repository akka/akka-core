/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs21.akka.persistence.typed.javadsl;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.event.slf4j.Logger;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.javadsl.*;
import com.typesafe.config.ConfigFactory;
import jdocs.akka.persistence.typed.MyReplicatedBehavior;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static akka.Done.done;
import static org.junit.Assert.assertEquals;

public class ReplicatedEventSourcingTest extends JUnitSuite {

  static final class TestBehavior
      extends ReplicatedEventSourcedOnCommandBehavior<TestBehavior.Command, String, TestBehavior.State> {
    sealed interface Command {}

    public record GetState(ActorRef<State> replyTo) implements Command {}
    public record StoreMe(String text, ActorRef<Done> replyTo) implements Command {}
    public record StoreUs(List<String> texts, ActorRef<Done> replyTo) implements Command {}
    public record GetReplica(ActorRef<ReplicaId> replyTo) implements Command {}

    public record State(Set<String> texts) {}
    enum Stop implements Command {
      INSTANCE
    }

    public static Behavior<Command> create(
        String entityId, ReplicaId replicaId, Set<ReplicaId> allReplicas) {
      return ReplicatedEventSourcing.commonJournalConfigForEventSourcedOnCommandBehavior(
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
    public State emptyState() {
      return new State(Collections.emptySet());
    }


    @Override
    public Effect<String, State> onCommand(State state, Command command) {
      return switch (command) {
        case StoreMe storeMe -> Effect().persist(storeMe.text).thenRun(__ -> storeMe.replyTo.tell(done()));
        case StoreUs storeUs -> Effect().persist(storeUs.texts).thenRun(__ -> storeUs.replyTo.tell(done()));
        case GetState get ->
                // Note the defensive copy
                Effect().reply(get.replyTo, new State(new HashSet<>(state.texts)));
        case GetReplica getReplica -> Effect().reply(getReplica.replyTo, getReplicationContext().replicaId());
        case Stop ignored -> Effect().stop();
      };
    }

    @Override
    public State onEvent(State state, String event) {
      var updatedSet = new HashSet<>(state.texts);
      updatedSet.add(event);
      return new State(updatedSet);
    }


    @Override
    public Optional<ReplicationInterceptor<String, State>> replicationInterceptor() {
      return Optional.of((originReplica, sequenceNumber, state, event) -> {
        var logger = LoggerFactory.getLogger(TestBehavior.class);
        logger.info("Intercept side effect for replicated event {}", event);
        return CompletableFuture.completedFuture(Done.done());
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
