/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe

final class OnSignalSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "An Actor.OnSignal behavior" must {
    "must correctly install the signal handler" in {
      val probe = TestProbe[Done]("probe")
      val behavior =
        Behaviors.setup[Nothing] { context =>
          val stoppedChild = context.spawn(Behaviors.stopped, "stopped-child")
          context.watch(stoppedChild)
          Behaviors.receiveSignal[Nothing] {
            case (_, Terminated(`stoppedChild`)) =>
              probe.ref ! Done
              Behaviors.stopped
          }
        }
      spawn[Nothing](behavior)
      probe.expectMessage(Done)
    }

    def stopper(probe: TestProbe[Done], children: Int) = Behaviors.setup[String] { ctx =>
      (0 until children).foreach { i =>
        ctx.spawn(Behaviors.receiveMessage[String] { _ =>
          Behaviors.same
        }, s"$i")
      }
      Behaviors
        .receiveMessagePartial[String] {
          case "stop" =>
            Behaviors.stopped
        }
        .receiveSignal {
          case (_, PostStop) =>
            probe.ref ! Done
            Behaviors.same
        }
    }

    List(0, 2).foreach { nrChildren =>
      s"execute post stop for child $nrChildren" in {
        val probe = createTestProbe[Done]("post-stop-child")
        val stopperRef = spawn(stopper(probe, nrChildren))
        stopperRef ! "stop"
        probe.expectMessage(Done)

      }

      s"execute post stop for guardian behavior $nrChildren" in {
        val probe = createTestProbe[Done]("post-stop-probe")
        val system = ActorSystem(stopper(probe, nrChildren), "work")
        try {
          system ! "stop"
          probe.expectMessage(Done)
        } finally {
          ActorTestKit.shutdown(system)
        }
      }
    }
  }
}
