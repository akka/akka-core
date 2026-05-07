/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.testkit.typed.TestException

class TestAppenderSpec
    extends ScalaTestWithActorTestKit(
      """
  # increase to avoid spurious failures in "find unexpected async events withOccurrences(0)"
  akka.actor.testkit.typed.expect-no-message-default = 1000 ms
  """)
    with AnyWordSpecLike
    with LogCapturing {

  class AnotherLoggerClass

  private val log = LoggerFactory.getLogger(getClass)

  "TestAppender and LoggingEventFilter" must {
    "filter errors without cause" in {
      LoggingTestKit.error("an error").withOccurrences(2).expect {
        log.error("an error")
        log.error("an error")
      }
    }

    "filter errors with cause" in {
      LoggingTestKit.error("err").withCause[TestException].expect {
        log.error("err", TestException("an error"))
      }
    }

    "filter warnings" in {
      LoggingTestKit.warn("a warning").withOccurrences(2).expect {
        log.error("an error")
        log.warn("a warning")
        log.error("an error")
        log.warn("a warning")
      }
    }

    "find excess messages" in {
      intercept[AssertionError] {
        LoggingTestKit.warn("a warning").withOccurrences(2).expect {
          log.error("an error")
          log.warn("a warning")
          log.warn("a warning")
          log.error("an error")
          // since this logging is synchronous it will notice 3 occurrences but expecting 2,
          // but note that it will not look for asynchronous excess messages when occurrences > 0 and it has
          // already found expected number
          log.warn("a warning") // 3rd
        }
      }.getMessage should include("Received 1 excess messages")
    }

    "only filter events for given logger name" in {
      val count = new AtomicInteger
      LoggingTestKit
        .custom({
          case logEvent =>
            count.incrementAndGet()
            logEvent.message == "Hello from right logger" && logEvent.loggerName == classOf[AnotherLoggerClass].getName
        })
        .withOccurrences(2)
        .withLoggerName(classOf[AnotherLoggerClass].getName)
        .expect {
          LoggerFactory.getLogger(classOf[AnotherLoggerClass]).info("Hello from right logger")
          log.info("Hello wrong logger")
          LoggerFactory.getLogger(classOf[AnotherLoggerClass]).info("Hello from right logger")
        }
      count.get should ===(2)
    }

    "find unexpected events withOccurrences(0)" in {
      LoggingTestKit.warn("a warning").withOccurrences(0).expect {
        log.error("an error")
        log.warn("another warning")
      }

      intercept[AssertionError] {
        LoggingTestKit.warn("a warning").withOccurrences(0).expect {
          log.error("an error")
          log.warn("a warning")
          log.warn("another warning")
        }
      }.getMessage should include("Received 1 excess messages")

      intercept[AssertionError] {
        LoggingTestKit.warn("a warning").withOccurrences(0).expect {
          log.warn("a warning")
          log.warn("a warning")
        }
      }.getMessage should include("Received 2 excess messages")

    }

    "find unexpected async events withOccurrences(0)" in {
      // expect-no-message-default = 1000 ms
      intercept[AssertionError] {
        LoggingTestKit.warn("a warning").withOccurrences(0).expect {
          Future {
            Thread.sleep(20)
            log.warn("a warning")
            log.warn("a warning")
          }(system.executionContext)
        }
      }
    }

  }

}
