/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.receptionist

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

object ReceptionistApiSpec {

  def compileOnlySpec(): Unit = {
    // some dummy prerequisites
    implicit val timeout: Timeout = 3.seconds
    val service: ActorRef[String] = ???
    val key: ServiceKey[String] = ServiceKey[String]("id")
    implicit val system: ActorSystem[Void] = ???
    import system.executionContext

    // registration from outside, without ack, should be rare
    system.receptionist ! Receptionist.Register(key, service)

    // registration from outside with ack, should be rare
    // needs the explicit type on the future and the extra parenthesises
    // to work
    val registered: Future[Receptionist.Registered] =
      system.receptionist.ask(Receptionist.Register(key, service, _))
    registered.foreach {
      case key.Registered(ref) =>
        // ref is the right type here
        ref ! "woho"
      case _ => ()
    }

    // one-off ask outside of actor, should be uncommon but not rare
    val found: Future[Receptionist.Listing] =
      system.receptionist.ask(Receptionist.Find(key, _))
    found.foreach {
      case key.Listing(instances) =>
        instances.foreach(_ ! "woho")
      case _ => ()
    }

    Behaviors.setup[Any] { context =>
      // oneoff ask inside of actor, this should be a rare use case
      context.ask(system.receptionist, Receptionist.Find(key)) {
        case Success(key.Listing(services)) => services // Set[ActorRef[String]] !!
        case _                              => "unexpected"
      }

      // this is a more "normal" use case which is clean
      context.system.receptionist ! Receptionist.Subscribe(key, context.self.narrow)

      // another more "normal" is subscribe using an adapter
      // FIXME inference doesn't work with partial function
      val adapter = context.spawnMessageAdapter { (listing: Receptionist.Listing) =>
        listing.serviceInstances(key) // Set[ActorRef[String]] !!
      }
      context.system.receptionist ! Receptionist.Subscribe(key, adapter)

      // ofc this doesn't make sense to do in the same actor, this is just
      // to cover as much of the API as possible
      context.system.receptionist ! Receptionist.Register(key, context.self.narrow, context.self.narrow)

      Behaviors.receiveMessagePartial {
        case key.Listing(services) =>
          services.foreach(_ ! "woho")
          Behaviors.same
        case key.Registered(service2) => // ack on Register above
          service2 ! "woho"
          Behaviors.same
      }

    }
  }

}
