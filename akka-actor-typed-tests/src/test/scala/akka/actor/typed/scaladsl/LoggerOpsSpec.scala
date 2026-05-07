/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit

object LoggerOpsSpec {
  case class Value1(i: Int)
  case class Value2(i: Int)
  case class Value3(i: Int)
}

class LoggerOpsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import LoggerOpsSpec._

  val log = LoggerFactory.getLogger(getClass)

  "LoggerOps" must {

    "provide extension method for 2 arguments" in {
      LoggingTestKit.info("[template a b]").expect {
        log.info("[template {} {}]", "a", "b")
      }
      LoggingTestKit.info("[template a 2]").expect {
        log.info("[template {} {}]", "a", 2)
      }
      LoggingTestKit.info("[template 1 2]").expect {
        log.info("[template {} {}]", 1, 2)
      }
      LoggingTestKit.info("[template 1 b]").expect {
        log.info("[template {} {}]", 1, "b")
      }
      LoggingTestKit.info("[template a Value2(2)]").expect {
        log.info("[template {} {}]", "a", Value2(2))
      }
      LoggingTestKit.info("[template Value1(1) Value1(1)]").expect {
        log.info("[template {} {}]", Value1(1), Value1(1))
      }
      LoggingTestKit.info("[template Value1(1) Value2(2)]").expect {
        log.info("[template {} {}]", Value1(1), Value2(2))
      }
    }

    "provide extension method for vararg arguments" in {
      LoggingTestKit.info("[template a b c]").expect {
        log.info("[template {} {} {}]", "a", "b", "c")
      }
      LoggingTestKit.info("[template a b 3]").expect {
        log.info("[template {} {} {}]", "a", "b", 3)
      }
      LoggingTestKit.info("[template a 2 c]").expect {
        log.info("[template {} {} {}]", "a", 2, "c")
      }
      LoggingTestKit.info("[template 1 2 3]").expect {
        log.info("[template {} {} {}]", 1, 2, 3)
      }
      LoggingTestKit.info("[template 1 b c]").expect {
        log.info("[template {} {} {}]", 1, "b", "c")
      }
      LoggingTestKit.info("[template a Value2(2) Value3(3)]").expect {
        log.info("[template {} {} {}]", "a", Value2(2), Value3(3))
      }
      LoggingTestKit.info("[template Value1(1) Value1(1) Value1(1)]").expect {
        log.info("[template {} {} {}]", Value1(1), Value1(1), Value1(1))
      }
      LoggingTestKit.info("[template Value1(1) Value2(2) Value3(3)]").expect {
        log.info("[template {} {} {}]", Value1(1), Value2(2), Value3(3))
      }
    }
  }
}
