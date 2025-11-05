/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.ReplicationId;
import akka.persistence.typed.crdt.ORSet;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicatedEventSourcing;
import akka.persistence.typed.javadsl.ReplicationContext;
import java.util.Collections;
import java.util.Set;

interface ReplicatedMovieExample {

  // #movie-entity
  public final class MovieWatchList
      extends ReplicatedEventSourcedBehavior<MovieWatchList.Command, ORSet.DeltaOp, ORSet<String>> {

    interface Command {}

    public static class AddMovie implements Command {
      public final String movieId;

      public AddMovie(String movieId) {
        this.movieId = movieId;
      }
    }

    public static class RemoveMovie implements Command {
      public final String movieId;

      public RemoveMovie(String movieId) {
        this.movieId = movieId;
      }
    }

    public static class GetMovieList implements Command {
      public final ActorRef<MovieList> replyTo;

      public GetMovieList(ActorRef<MovieList> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static class MovieList {
      public final Set<String> movieIds;

      public MovieList(Set<String> movieIds) {
        this.movieIds = Collections.unmodifiableSet(movieIds);
      }
    }

    public static Behavior<Command> create(
        String entityId, ReplicaId replicaId, Set<ReplicaId> allReplicas) {
      return ReplicatedEventSourcing.commonJournalConfig(
          new ReplicationId("movies", entityId, replicaId),
          allReplicas,
          PersistenceTestKitReadJournal.Identifier(),
          MovieWatchList::new);
    }

    private MovieWatchList(ReplicationContext replicationContext) {
      super(replicationContext);
    }

    @Override
    public ORSet<String> emptyState() {
      return ORSet.empty(getReplicationContext().replicaId());
    }

    @Override
    public CommandHandler<Command, ORSet.DeltaOp, ORSet<String>> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(
              AddMovie.class, (state, command) -> Effect().persist(state.add(command.movieId)))
          .onCommand(
              RemoveMovie.class,
              (state, command) -> Effect().persist(state.remove(command.movieId)))
          .onCommand(
              GetMovieList.class,
              (state, command) -> {
                command.replyTo.tell(new MovieList(state.getElements()));
                return Effect().none();
              })
          .build();
    }

    @Override
    public EventHandler<ORSet<String>, ORSet.DeltaOp> eventHandler() {
      return newEventHandlerBuilder().forAnyState().onAnyEvent(ORSet::applyOperation);
    }
  }
  // #movie-entity
}
