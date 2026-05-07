/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.event.Logging
import akka.event.LoggingAdapter
import akka.stream.Attributes
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.{ EventFilter, TestProbe }

class RecipeLoggingElements extends RecipeSpec {

  "Simple logging recipe" must {

    "work with println" in {
      val printProbe = TestProbe()
      def println(s: String): Unit = printProbe.ref ! s

      val mySource = Source(List("1", "2", "3"))

      //#println-debug
      val loggedSource = mySource.map { elem =>
        println(elem); elem
      }
      //#println-debug

      loggedSource.runWith(Sink.ignore)
      printProbe.expectMsgAllOf("1", "2", "3")
    }

    val mySource = Source(List("1", "2", "3"))
    def analyse(s: String) = s
    "use log()" in {
      //#log-custom
      // customise log levels
      mySource
        .log("before-map")
        .withAttributes(Attributes
          .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
        .map(analyse)
      //#log-custom
    }

    "use log() with custom adapter" in {
      //#log-custom
      // or provide custom logging adapter
      implicit val adapter: LoggingAdapter = Logging(system, "customLogger")
      mySource.log("custom")
      //#log-custom

      val loggedSource = mySource.log("custom")
      EventFilter.debug(start = "[custom] Element: ").intercept {
        loggedSource.runWith(Sink.ignore)
      }
    }

    "use log() for error logging" in {
      //#log-error
      Source(-5 to 5)
        .map(1 / _) //throwing ArithmeticException: / by zero
        .log("error logging")
        .runWith(Sink.ignore)
      //#log-error
    }
  }

}
