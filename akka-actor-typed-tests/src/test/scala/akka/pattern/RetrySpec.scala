/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.concurrent.Futures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RetrySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers with Futures {

  "The RetrySettings based retry api" should {

    "work with typed actor system" in {

      // just a successful try to cover API and implicit system provider
      val retried = retry(RetrySettings(5).withFixedDelay(40.millis)) { () =>
        Future.successful(5)
      }

      retried.futureValue should ===(5)
    }
  }

}
