/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.RetryFlow
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

final class Bug30201ReproducerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "The GraphInterpreter" should {

    "not invoke finalizeStage twice causing a NPE" in {

      LoggingTestKit.error[java.lang.NullPointerException].withOccurrences(0).expect {
        // 3 failures, one success
        @volatile var n = 0
        val flow = Flow[String].mapAsyncUnordered(1) { _ =>
          n += 1
          if (n < 4) Future.successful("bad")
          else Future.successful("ok")
        }

        val source = Source
          .single("in")
          .via(RetryFlow.withBackoff(50.millis, 100.millis, 0.1d, 4, flow) {
            case (request, status) if status == "bad" =>
              Some(request)
            case _ =>
              None
          })

        source.runWith(TestSink()).requestNext("ok").expectComplete()
      }
    }
  }

}
