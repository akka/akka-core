/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MovieWatchList
    extends EventSourcedBehavior<
        MovieWatchList.Command, MovieWatchList.Event, MovieWatchList.MovieList> {

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

  interface Event {}

  public static class MovieAdded implements Event {
    public final String movieId;

    public MovieAdded(String movieId) {
      this.movieId = movieId;
    }
  }

  public static class MovieRemoved implements Event {
    public final String movieId;

    public MovieRemoved(String movieId) {
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

    public MovieList add(String movieId) {
      Set<String> newSet = new HashSet<>(movieIds);
      newSet.add(movieId);
      return new MovieList(newSet);
    }

    public MovieList remove(String movieId) {
      Set<String> newSet = new HashSet<>(movieIds);
      newSet.remove(movieId);
      return new MovieList(newSet);
    }
  }

  public static Behavior<Command> behavior(String userId) {
    return new MovieWatchList(PersistenceId.ofUniqueId("movies-" + userId));
  }

  public MovieWatchList(PersistenceId persistenceId) {
    super(persistenceId);
  }

  @Override
  public MovieList emptyState() {
    return new MovieList(Collections.emptySet());
  }

  @Override
  public CommandHandler<Command, Event, MovieList> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(
            AddMovie.class,
            (state, cmd) -> {
              return Effect().persist(new MovieAdded(cmd.movieId));
            })
        .onCommand(
            RemoveMovie.class,
            (state, cmd) -> {
              return Effect().persist(new MovieRemoved(cmd.movieId));
            })
        .onCommand(
            GetMovieList.class,
            (state, cmd) -> {
              cmd.replyTo.tell(state);
              return Effect().none();
            })
        .build();
  }

  @Override
  public EventHandler<MovieList, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(MovieAdded.class, (state, event) -> state.add(event.movieId))
        .onEvent(MovieRemoved.class, (state, event) -> state.remove(event.movieId))
        .build();
  }
}
