/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.{ FishingOutcomes, LogCapturing, ScalaTestWithActorTestKit, TestProbe }
import akka.actor.typed.{ ActorRef, Behavior }

object ActorContextDelegateSpec {
  sealed trait PingPongCommand
  case object Ping extends PingPongCommand
  case object Pong extends PingPongCommand
  case object UnPingable extends PingPongCommand

  sealed trait BehaviorTag
  case object PingTag extends BehaviorTag
  case object PongTag extends BehaviorTag

  sealed trait Event
  final case class ResponseFrom(from: BehaviorTag, cmd: PingPongCommand) extends Event
  final case class ForwardTo(to: BehaviorTag) extends Event

  def ping(monitor: ActorRef[Event])(implicit context: ActorContext[PingPongCommand]): Behavior[PingPongCommand] =
    Behaviors.receiveMessagePartial[PingPongCommand] {
      case Ping =>
        monitor ! ResponseFrom(PingTag, Ping)
        Behaviors.same
      case Pong =>
        monitor ! ForwardTo(PongTag)
        context.delegate(pong(monitor), Pong)
    }

  def pong(monitor: ActorRef[Event])(implicit context: ActorContext[PingPongCommand]): Behavior[PingPongCommand] =
    Behaviors.receiveMessage[PingPongCommand] {
      case Ping =>
        monitor ! ForwardTo(PingTag)
        context.delegate(ping(monitor), Ping)
      case Pong =>
        monitor ! ResponseFrom(PongTag, Pong)
        Behaviors.same
      case UnPingable =>
        monitor ! ForwardTo(PingTag)
        context.delegate(ping(monitor), UnPingable)
    }
}

class ActorContextDelegateSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import ActorContextDelegateSpec._
  implicit val testSettings: TestKitSettings = TestKitSettings(system)

  "The Scala DSL ActorContext delegate" must {
    "delegate message by given behavior and handle resulting Behavior.same properly" in {
      val probe = TestProbe[Event]()
      val behv = Behaviors.setup[PingPongCommand] { implicit context =>
        ping(probe.ref)
      }
      val ref = spawn(behv)
      ref ! Pong
      probe.expectMessage(ForwardTo(PongTag))
      probe.expectMessage(ResponseFrom(PongTag, Pong))
      ref ! Pong
      probe.expectMessage(ResponseFrom(PongTag, Pong))
      ref ! Ping
      probe.expectMessage(ForwardTo(PingTag))
      probe.expectMessage(ResponseFrom(PingTag, Ping))
    }

    "publish unhandled message to eventStream as UnhandledMessage and switch to delegator behavior" in {
      val deadLetters = testKit.createUnhandledMessageProbe()

      val probe = TestProbe[Event]()
      val behv = Behaviors.setup[PingPongCommand] { implicit context =>
        pong(probe.ref)
      }
      val ref = spawn(behv)

      ref ! UnPingable
      probe.expectMessage(ForwardTo(PingTag))
      deadLetters.fishForMessage(deadLetters.remainingOrDefault) {
        case UnhandledMessage(UnPingable, _, _) => FishingOutcomes.complete
        case _                                  => FishingOutcomes.fail("unexpected message")
      }
      ref ! Ping
      probe.expectMessage(ResponseFrom(PingTag, Ping))
    }
  }

}
