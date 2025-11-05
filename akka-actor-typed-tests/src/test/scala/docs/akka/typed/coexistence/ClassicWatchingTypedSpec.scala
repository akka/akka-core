/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.coexistence

import akka.actor.ActorLogging
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.testkit.TestKit
//#adapter-import
// adds support for actors to a classic actor system and context
import akka.actor.typed.scaladsl.adapter._
//#adapter-import
import akka.testkit.TestProbe
//#import-alias
import akka.{ actor => classic }
//#import-alias
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

object ClassicWatchingTypedSpec {
  object Classic {
    def props() = classic.Props(new Classic)
  }

  //#classic-watch
  class Classic extends classic.Actor with ActorLogging {
    // context.spawn is an implicit extension method
    val second: ActorRef[Typed.Command] =
      context.spawn(Typed(), "second")

    // context.watch is an implicit extension method
    context.watch(second)

    // self can be used as the `replyTo` parameter here because
    // there is an implicit conversion from akka.actor.ActorRef to
    // akka.actor.typed.ActorRef
    // An equal alternative would be `self.toTyped`
    second ! Typed.Ping(self)

    override def receive = {
      case Typed.Pong =>
        log.info(s"$self got Pong from ${sender()}")
        // context.stop is an implicit extension method
        context.stop(second)
      case classic.Terminated(ref) =>
        log.info(s"$self observed termination of $ref")
        context.stop(self)
    }
  }
  //#classic-watch

  //#typed
  object Typed {
    sealed trait Command
    final case class Ping(replyTo: ActorRef[Pong.type]) extends Command
    case object Pong

    def apply(): Behavior[Command] =
      Behaviors.receive { (context, message) =>
        message match {
          case Ping(replyTo) =>
            context.log.info(s"${context.self} got Ping from $replyTo")
            // replyTo is a classic actor that has been converted for coexistence
            replyTo ! Pong
            Behaviors.same
        }
      }
  }
  //#typed
}

class ClassicWatchingTypedSpec extends AnyWordSpec with LogCapturing {

  import ClassicWatchingTypedSpec._

  "Classic -> Typed" must {
    "support creating, watching and messaging" in {
      val system = classic.ActorSystem("Coexistence")
      //#create-classic
      val classicActor = system.actorOf(Classic.props())
      //#create-classic
      val probe = TestProbe()(system)
      probe.watch(classicActor)
      probe.expectTerminated(classicActor, 200.millis)
      TestKit.shutdownActorSystem(system)
    }

    "support converting a classic actor system to an actor system" in {
      //#convert-classic

      val system = akka.actor.ActorSystem("ClassicToTypedSystem")
      val typedSystem: ActorSystem[Nothing] = system.toTyped
      //#convert-classic
      typedSystem.scheduler // remove compile warning
      TestKit.shutdownActorSystem(system)
    }
  }
}
