/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.actor.testkit.typed.scaladsl

import docs.akka.actor.testkit.typed.scaladsl.AsyncTestingExampleSpec.Echo

//#log-capturing
import akka.actor.testkit.typed.scaladsl.LogCapturing
//#scalatest-integration
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike

//#scalatest-integration
//#log-capturing

//#scalatest-integration
class ScalaTestIntegrationExampleSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "Something" must {
    "behave correctly" in {
      val pinger = testKit.spawn(Echo(), "ping")
      val probe = testKit.createTestProbe[Echo.Pong]()
      pinger ! Echo.Ping("hello", probe.ref)
      probe.expectMessage(Echo.Pong("hello"))
    }
  }
}
//#scalatest-integration

//#log-capturing
class LogCapturingExampleSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "Something" must {
    "behave correctly" in {
      val pinger = testKit.spawn(Echo(), "ping")
      val probe = testKit.createTestProbe[Echo.Pong]()
      pinger ! Echo.Ping("hello", probe.ref)
      probe.expectMessage(Echo.Pong("hello"))
    }
  }
}
//#log-capturing
