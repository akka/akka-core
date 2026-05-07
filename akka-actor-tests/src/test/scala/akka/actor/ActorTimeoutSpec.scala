/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.pattern.{ ask, AskTimeoutException }
import akka.testkit._
import akka.testkit.TestEvent._
import akka.util.Timeout

class ActorTimeoutSpec extends AkkaSpec {

  val testTimeout = 200.millis.dilated
  val leeway = 500.millis.dilated

  system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*unhandled message from.*hallo")))

  "An Actor-based Future" must {

    "use implicitly supplied timeout" in {
      implicit val timeout = Timeout(testTimeout)
      val echo = system.actorOf(Props.empty)
      val f = (echo ? "hallo")
      intercept[AskTimeoutException] { Await.result(f, testTimeout + leeway) }
    }

    "use explicitly supplied timeout" in {
      val echo = system.actorOf(Props.empty)
      val f = echo.?("hallo")(testTimeout)
      intercept[AskTimeoutException] { Await.result(f, testTimeout + leeway) }
    }
  }
}
