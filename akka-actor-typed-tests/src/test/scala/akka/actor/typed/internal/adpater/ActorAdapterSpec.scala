/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adpater

import scala.concurrent.duration.DurationInt
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._

class ActorAdapterSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  sealed trait Command
  case object Begin extends Command
  case object End extends Command
  case object Timeout extends Command

  sealed trait Event
  case object Ended extends Event

  def evidence(monitor: ActorRef[Event]): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage[Command] {
        case Begin =>
          context.setReceiveTimeout(100.milliseconds, Timeout)
          context.cancelReceiveTimeout()
          context.self.toClassic ! akka.actor.ReceiveTimeout
          context.self ! End
          Behaviors.same

        case Timeout =>
          Behaviors.stopped

        case End =>
          monitor ! Ended
          Behaviors.stopped
      }
    }

  "An ActorAdapter" must {
    "not cause actor crash after recieve timeout was cancelled with ReceiveTimeout waiting in mailbox" in {
      val probe = TestProbe[Event]("evt")
      val behavior = evidence(probe.ref)

      val ref = spawn(behavior)
      ref ! Begin
      probe.expectMessage(Ended)
    }
  }
}
