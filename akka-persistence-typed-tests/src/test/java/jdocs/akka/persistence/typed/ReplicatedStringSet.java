/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.actor.typed.Behavior;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.javadsl.*;
import java.util.HashSet;
import java.util.Set;

public final class ReplicatedStringSet
    extends ReplicatedEventSourcedBehavior<ReplicatedStringSet.Command, String, Set<String>> {
  interface Command {}

  public static final class AddString implements Command {
    final String string;

    public AddString(String string) {
      this.string = string;
    }
  }

  public static Behavior<Command> create(
      String entityId, ReplicaId replicaId, Set<ReplicaId> allReplicas) {
    return ReplicatedEventSourcing.commonJournalConfig(
        new ReplicationId("StringSet", entityId, replicaId),
        allReplicas,
        PersistenceTestKitReadJournal.Identifier(),
        ReplicatedStringSet::new);
  }

  private ReplicatedStringSet(ReplicationContext replicationContext) {
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
        .onCommand(
            AddString.class,
            (state, cmd) -> {
              if (!state.contains(cmd.string)) return Effect().persist(cmd.string);
              else return Effect().none();
            })
        .build();
  }

  @Override
  public EventHandler<Set<String>, String> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onAnyEvent(
            (set, string) -> {
              HashSet<String> newState = new HashSet<>(set);
              newState.add(string);
              return newState;
            });
  }

  // #tagging
  @Override
  public Set<String> tagsFor(String event) {
    // don't apply tags if event was replicated here, it already will appear in queries by tag
    // as the origin replica would have tagged it already
    if (getReplicationContext().replicaId() != getReplicationContext().origin()) {
      return new HashSet<>();
    } else {
      Set<String> tags = new HashSet<>();
      tags.add("strings");
      if (event.length() > 10) tags.add("long-strings");
      return tags;
    }
  }
  // #tagging
}
