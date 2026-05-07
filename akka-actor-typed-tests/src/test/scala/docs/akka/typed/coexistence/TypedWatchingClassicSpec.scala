/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.coexistence

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.testkit.TestKit
//#adapter-import
// adds support for typed actors to a classic actor system and context
import akka.actor.typed.scaladsl.adapter._
//#adapter-import
import akka.testkit.TestProbe
//#import-alias
import akka.{ actor => classic }
//#import-alias
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

object TypedWatchingClassicSpec {

  //#typed
  object Typed {
    final case class Ping(replyTo: akka.actor.typed.ActorRef[Pong.type])
    sealed trait Command
    case object Pong extends Command

    val behavior: Behavior[Command] =
      Behaviors.setup { context =>
        // context.actorOf is an implicit extension method
        val classic = context.actorOf(Classic.props(), "second")

        // context.watch is an implicit extension method
        context.watch(classic)

        // illustrating how to pass sender, toClassic is an implicit extension method
        classic.tell(Typed.Ping(context.self), context.self.toClassic)

        Behaviors
          .receiveMessagePartial[Command] {
            case Pong =>
              // it's not possible to get the sender, that must be sent in message
              // context.stop is an implicit extension method
              context.stop(classic)
              Behaviors.same
          }
          .receiveSignal {
            case (_, akka.actor.typed.Terminated(_)) =>
              Behaviors.stopped
          }
      }
  }
  //#typed

  //#classic
  object Classic {
    def props(): classic.Props = classic.Props(new Classic)
  }
  class Classic extends classic.Actor {
    override def receive = {
      case Typed.Ping(replyTo) =>
        replyTo ! Typed.Pong
    }
  }
  //#classic
}

class TypedWatchingClassicSpec extends AnyWordSpec with LogCapturing {

  import TypedWatchingClassicSpec._

  "Typed -> Classic" must {
    "support creating, watching and messaging" in {
      //#create
      val system = classic.ActorSystem("TypedWatchingClassic")
      val typed = system.spawn(Typed.behavior, "Typed")
      //#create
      val probe = TestProbe()(system)
      probe.watch(typed.toClassic)
      probe.expectTerminated(typed.toClassic, 200.millis)
      TestKit.shutdownActorSystem(system)
    }
  }
}
