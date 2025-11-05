/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors

class MonitorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "The monitor behavior" should {

    "monitor messages" in {
      val probe = TestProbe[String]()

      val beh: Behavior[String] = Behaviors.monitor(probe.ref, Behaviors.receiveMessage(_ => Behaviors.same))
      val ref: ActorRef[String] = spawn(beh)

      ref ! "message"

      probe.expectMessage("message")
    }

    "monitor messages once per ref initially" in {
      val probe = TestProbe[String]()

      def monitor(beh: Behavior[String]): Behavior[String] =
        Behaviors.monitor(probe.ref, beh)

      val beh: Behavior[String] =
        monitor(monitor(Behaviors.receiveMessage(_ => Behaviors.same)))
      val ref: ActorRef[String] = spawn(beh)

      ref ! "message 1"
      probe.expectMessage("message 1")
      ref ! "message 2"
      probe.expectMessage("message 2")
    }

    "monitor messages once per ref recursively" in {
      val probe = TestProbe[String]()

      def monitor(beh: Behavior[String]): Behavior[String] =
        Behaviors.monitor(probe.ref, beh)

      def next: Behavior[String] =
        monitor(Behaviors.receiveMessage(_ => next))
      val ref: ActorRef[String] = spawn(next)

      ref ! "message 1"
      probe.expectMessage("message 1")
      ref ! "message 2"
      probe.expectMessage("message 2")
      ref ! "message 3"
      probe.expectMessage("message 3")
    }

  }

}
