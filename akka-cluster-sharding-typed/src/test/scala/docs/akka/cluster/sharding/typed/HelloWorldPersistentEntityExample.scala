/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.serialization.jackson.CborSerializable

object HelloWorldPersistentEntityExample {

  //#persistent-entity-usage
  import akka.cluster.sharding.typed.scaladsl.ClusterSharding
  import akka.cluster.sharding.typed.scaladsl.Entity
  import akka.util.Timeout

  class HelloWorldService(system: ActorSystem[_]) {
    import system.executionContext

    private val sharding = ClusterSharding(system)

    // registration at startup
    sharding.init(Entity(typeKey = HelloWorld.TypeKey) { entityContext =>
      HelloWorld(entityContext.entityId, PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId))
    })

    private implicit val askTimeout: Timeout = Timeout(5.seconds)

    def greet(worldId: String, whom: String): Future[Int] = {
      val entityRef = sharding.entityRefFor(HelloWorld.TypeKey, worldId)
      val greeting = entityRef ? HelloWorld.Greet(whom)
      greeting.map(_.numberOfPeople)
    }

  }
  //#persistent-entity-usage

  //#persistent-entity
  import akka.actor.typed.Behavior
  import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
  import akka.persistence.typed.scaladsl.Effect

  object HelloWorld {

    // Command
    sealed trait Command extends CborSerializable
    final case class Greet(whom: String)(val replyTo: ActorRef[Greeting]) extends Command
    // Response
    final case class Greeting(whom: String, numberOfPeople: Int) extends CborSerializable

    // Event
    final case class Greeted(whom: String) extends CborSerializable

    // State
    final case class KnownPeople(names: Set[String]) extends CborSerializable {
      def add(name: String): KnownPeople = copy(names = names + name)

      def numberOfPeople: Int = names.size
    }

    private val commandHandler: (KnownPeople, Command) => Effect[Greeted, KnownPeople] = { (_, cmd) =>
      cmd match {
        case cmd: Greet => greet(cmd)
      }
    }

    private def greet(cmd: Greet): Effect[Greeted, KnownPeople] =
      Effect.persist(Greeted(cmd.whom)).thenRun(state => cmd.replyTo ! Greeting(cmd.whom, state.numberOfPeople))

    private val eventHandler: (KnownPeople, Greeted) => KnownPeople = { (state, evt) =>
      state.add(evt.whom)
    }

    val TypeKey: EntityTypeKey[Command] =
      EntityTypeKey[Command]("HelloWorld")

    def apply(entityId: String, persistenceId: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        context.log.info("Starting HelloWorld {}", entityId)
        EventSourcedBehavior(persistenceId, emptyState = KnownPeople(Set.empty), commandHandler, eventHandler)
      }
    }

  }
  //#persistent-entity

}
