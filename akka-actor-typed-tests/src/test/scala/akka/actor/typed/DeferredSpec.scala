/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.util.control.NoStackTrace

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{ AnyWordSpec, AnyWordSpecLike }

import akka.actor.ActorInitializationException
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl._
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.Behaviors

object DeferredSpec {
  sealed trait Command
  case object Ping extends Command

  sealed trait Event
  case object Pong extends Event
  case object Started extends Event

  def target(monitor: ActorRef[Event]): Behavior[Command] =
    Behaviors.receive((_, cmd) =>
      cmd match {
        case Ping =>
          monitor ! Pong
          Behaviors.same
      })
}

class DeferredSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import DeferredSpec._
  implicit val testSettings: TestKitSettings = TestKitSettings(system)

  "Deferred behavior" must {
    "must create underlying" in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.setup[Command] { _ =>
        probe.ref ! Started
        target(probe.ref)
      }
      probe.expectNoMessage() // not yet
      spawn(behv)
      // it's supposed to be created immediately (not waiting for first message)
      probe.expectMessage(Started)
    }

    "must stop when exception from factory" in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.setup[Command] { context =>
        val child = context.spawnAnonymous(Behaviors.setup[Command] { _ =>
          probe.ref ! Started
          throw new RuntimeException("simulated exc from factory") with NoStackTrace
        })
        context.watch(child)
        Behaviors.receive[Command]((_, _) => Behaviors.same).receiveSignal {
          case (_, Terminated(`child`)) =>
            probe.ref ! Pong
            Behaviors.stopped
        }
      }
      LoggingTestKit.error[ActorInitializationException].expect {
        spawn(behv)
        probe.expectMessage(Started)
        probe.expectMessage(Pong)
      }
    }

    "must stop when deferred result it Stopped" in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.setup[Command] { context =>
        val child = context.spawnAnonymous(Behaviors.setup[Command](_ => Behaviors.stopped))
        context.watch(child)
        Behaviors.receive[Command]((_, _) => Behaviors.same).receiveSignal {
          case (_, Terminated(`child`)) =>
            probe.ref ! Pong
            Behaviors.stopped
        }
      }
      spawn(behv)
      probe.expectMessage(Pong)
    }

    "must create underlying when nested" in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.setup[Command] { _ =>
        Behaviors.setup[Command] { _ =>
          probe.ref ! Started
          target(probe.ref)
        }
      }
      spawn(behv)
      probe.expectMessage(Started)
    }

    "must un-defer underlying when wrapped by transformMessages" in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors
        .setup[Command] { _ =>
          probe.ref ! Started
          target(probe.ref)
        }
        .transformMessages[Command] {
          case m => m
        }
      probe.expectNoMessage() // not yet
      val ref = spawn(behv)
      // it's supposed to be created immediately (not waiting for first message)
      probe.expectMessage(Started)
      ref ! Ping
      probe.expectMessage(Pong)
    }

    "must un-defer underlying when wrapped by monitor" in {
      // monitor is implemented with tap, so this is testing both
      val probe = TestProbe[Event]("evt")
      val monitorProbe = TestProbe[Command]("monitor")
      val behv = Behaviors.monitor(monitorProbe.ref, Behaviors.setup[Command] { _ =>
        probe.ref ! Started
        target(probe.ref)
      })
      probe.expectNoMessage() // not yet
      val ref = spawn(behv)
      // it's supposed to be created immediately (not waiting for first message)
      probe.expectMessage(Started)
      ref ! Ping
      monitorProbe.expectMessage(Ping)
      probe.expectMessage(Pong)
    }

    "must not allow setup(same)" in {
      val probe = TestProbe[Any]()
      val behv = Behaviors.setup[Command] { _ =>
        Behaviors.setup[Command] { _ =>
          Behaviors.same
        }
      }
      LoggingTestKit.error[ActorInitializationException].expect {
        val ref = spawn(behv)
        probe.expectTerminated(ref, probe.remainingOrDefault)
      }
    }
  }
}

class DeferredStubbedSpec extends AnyWordSpec with Matchers with LogCapturing {

  import DeferredSpec._

  "must create underlying deferred behavior immediately" in {
    val inbox = TestInbox[Event]("evt")
    val behv = Behaviors.setup[Command] { _ =>
      inbox.ref ! Started
      target(inbox.ref)
    }
    BehaviorTestKit(behv)
    // it's supposed to be created immediately (not waiting for first message)
    inbox.receiveMessage() should ===(Started)
  }

  "must stop when exception from factory" in {
    val inbox = TestInbox[Event]("evt")
    val exc = new RuntimeException("simulated exc from factory") with NoStackTrace
    val behv = Behaviors.setup[Command] { _ =>
      inbox.ref ! Started
      throw exc
    }
    intercept[RuntimeException] {
      BehaviorTestKit(behv)
    } should ===(exc)
    inbox.receiveMessage() should ===(Started)
  }

}
