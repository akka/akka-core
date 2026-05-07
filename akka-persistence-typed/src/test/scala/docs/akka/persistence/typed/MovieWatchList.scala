/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler

object MovieWatchList {
  sealed trait Command
  final case class AddMovie(movieId: String) extends Command
  final case class RemoveMovie(movieId: String) extends Command
  final case class GetMovieList(replyTo: ActorRef[MovieList]) extends Command

  sealed trait Event
  final case class MovieAdded(movieId: String) extends Event
  final case class MovieRemoved(movieId: String) extends Event

  final case class MovieList(movieIds: Set[String]) {
    def applyEvent(event: Event): MovieList = {
      event match {
        case MovieAdded(movieId)   => copy(movieIds = movieIds + movieId)
        case MovieRemoved(movieId) => copy(movieIds = movieIds + movieId)
      }
    }
  }

  private val commandHandler: CommandHandler[Command, Event, MovieList] = { (state, cmd) =>
    cmd match {
      case AddMovie(movieId) =>
        Effect.persist(MovieAdded(movieId))
      case RemoveMovie(movieId) =>
        Effect.persist(MovieRemoved(movieId))
      case GetMovieList(replyTo) =>
        replyTo ! state
        Effect.none
    }
  }

  def behavior(userId: String): Behavior[Command] = {
    EventSourcedBehavior[Command, Event, MovieList](
      persistenceId = PersistenceId("movies", userId),
      emptyState = MovieList(Set.empty),
      commandHandler,
      eventHandler = (state, event) => state.applyEvent(event))
  }

}
