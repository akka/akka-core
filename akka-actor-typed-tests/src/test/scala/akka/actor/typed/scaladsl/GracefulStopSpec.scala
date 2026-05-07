/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe

final class GracefulStopSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "Graceful stop" must {

    "properly stop the children and perform the cleanup" in {
      val probe = TestProbe[String]("probe")

      val behavior =
        Behaviors.setup[akka.NotUsed] { context =>
          context.spawn[NotUsed](Behaviors.receiveSignal {
            case (_, PostStop) =>
              probe.ref ! "child-done"
              Behaviors.stopped
          }, "child1")

          context.spawn[NotUsed](Behaviors.receiveSignal {
            case (_, PostStop) =>
              probe.ref ! "child-done"
              Behaviors.stopped
          }, "child2")

          Behaviors.stopped { () =>
            // cleanup function body
            probe.ref ! "parent-done"
          }

        }

      spawn(behavior)
      probe.expectMessage("child-done")
      probe.expectMessage("child-done")
      probe.expectMessage("parent-done")
    }

    "properly perform the cleanup and stop itself for no children case" in {
      val probe = TestProbe[Done]("probe")

      val behavior =
        Behaviors.setup[akka.NotUsed] { _ =>
          // do not spawn any children
          Behaviors.stopped { () =>
            // cleanup function body
            probe.ref ! Done
          }
        }

      spawn(behavior)
      probe.expectMessage(Done)
    }
  }

}
