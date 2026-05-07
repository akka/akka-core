/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.Done;
import akka.actor.typed.Behavior;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.javadsl.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// #factory
public class MyReplicatedBehavior
    extends ReplicatedEventSourcedBehavior<
        MyReplicatedBehavior.Command, MyReplicatedBehavior.Event, MyReplicatedBehavior.State> {
  // #factory
  interface Command {}

  interface State {}

  interface Event {}

  // #replicas
  public static final ReplicaId DCA = new ReplicaId("DCA");
  public static final ReplicaId DCB = new ReplicaId("DCB");

  public static final Set<ReplicaId> ALL_REPLICAS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DCA, DCB)));
  // #replicas

  // #factory-shared
  public static Behavior<Command> create(
      String entityId, ReplicaId replicaId, String queryPluginId) {
    return ReplicatedEventSourcing.commonJournalConfig(
        new ReplicationId("MyReplicatedEntity", entityId, replicaId),
        ALL_REPLICAS,
        queryPluginId,
        MyReplicatedBehavior::new);
  }
  // #factory-shared

  // #factory
  public static Behavior<Command> create(String entityId, ReplicaId replicaId) {
    Map<ReplicaId, String> allReplicasAndQueryPlugins = new HashMap<>();
    allReplicasAndQueryPlugins.put(DCA, "journalForDCA");
    allReplicasAndQueryPlugins.put(DCB, "journalForDCB");

    return ReplicatedEventSourcing.perReplicaJournalConfig(
        new ReplicationId("MyReplicatedEntity", entityId, replicaId),
        allReplicasAndQueryPlugins,
        MyReplicatedBehavior::new);
  }

  private MyReplicatedBehavior(ReplicationContext replicationContext) {
    super(replicationContext);
  }
  // #factory

  @Override
  public State emptyState() {
    throw new UnsupportedOperationException("dummy for example");
  }

  @Override
  public CommandHandler<Command, Event, State> commandHandler() {
    throw new UnsupportedOperationException("dummy for example");
  }

  @Override
  public EventHandler<State, Event> eventHandler() {
    throw new UnsupportedOperationException("dummy for example");
  }

  @Override
  public Optional<ReplicationInterceptor<Event, State>> replicationInterceptor() {
    return Optional.of(
        (originReplica, sequenceNumber, state, event) -> {
          Logger logger = LoggerFactory.getLogger(MyReplicatedBehavior.class);
          logger.info("Intercept side effect for replicated event {}", event);
          return CompletableFuture.completedFuture(Done.done());
        });
  }
}
