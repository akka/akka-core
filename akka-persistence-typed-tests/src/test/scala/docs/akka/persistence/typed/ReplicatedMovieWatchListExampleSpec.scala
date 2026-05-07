/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.crdt.ORSet
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing

object ReplicatedMovieWatchListExampleSpec {
  //#movie-entity
  object MovieWatchList {
    sealed trait Command
    final case class AddMovie(movieId: String) extends Command
    final case class RemoveMovie(movieId: String) extends Command
    final case class GetMovieList(replyTo: ActorRef[MovieList]) extends Command
    final case class MovieList(movieIds: Set[String])

    def apply(entityId: String, replicaId: ReplicaId, allReplicaIds: Set[ReplicaId]): Behavior[Command] = {
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("movies", entityId, replicaId),
        allReplicaIds,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, ORSet.DeltaOp, ORSet[String]](
          replicationContext.persistenceId,
          ORSet.empty(replicationContext.replicaId),
          (state, cmd) => commandHandler(state, cmd),
          (state, event) => eventHandler(state, event))
      }
    }

    private def commandHandler(state: ORSet[String], cmd: Command): Effect[ORSet.DeltaOp, ORSet[String]] = {
      cmd match {
        case AddMovie(movieId) =>
          Effect.persist(state + movieId)
        case RemoveMovie(movieId) =>
          Effect.persist(state - movieId)
        case GetMovieList(replyTo) =>
          replyTo ! MovieList(state.elements)
          Effect.none
      }
    }

    private def eventHandler(state: ORSet[String], event: ORSet.DeltaOp): ORSet[String] = {
      state.applyOperation(event)
    }

  }
  //#movie-entity

}

class ReplicatedMovieWatchListExampleSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReplicatedMovieWatchListExampleSpec._

  "MovieWatchList" must {
    "demonstrate ORSet" in {
      import MovieWatchList._

      val Replicas = Set(ReplicaId("DC-A"), ReplicaId("DC-B"))

      val dcAReplica: ActorRef[Command] = spawn(MovieWatchList("mylist", ReplicaId("DC-A"), Replicas))
      val dcBReplica: ActorRef[Command] = spawn(MovieWatchList("mylist", ReplicaId("DC-B"), Replicas))

      val probeA = createTestProbe[MovieList]()
      val probeB = createTestProbe[MovieList]()

      dcAReplica ! AddMovie("movie-15")
      dcAReplica ! AddMovie("movie-17")
      dcBReplica ! AddMovie("movie-20")

      eventually {
        dcAReplica ! GetMovieList(probeA.ref)
        probeA.expectMessage(MovieList(Set("movie-15", "movie-17", "movie-20")))
        dcBReplica ! GetMovieList(probeB.ref)
        probeB.expectMessage(MovieList(Set("movie-15", "movie-17", "movie-20")))
      }

      dcBReplica ! RemoveMovie("movie-17")
      eventually {
        dcAReplica ! GetMovieList(probeA.ref)
        probeA.expectMessage(MovieList(Set("movie-15", "movie-20")))
        dcBReplica ! GetMovieList(probeB.ref)
        probeB.expectMessage(MovieList(Set("movie-15", "movie-20")))
      }
    }

  }

}
