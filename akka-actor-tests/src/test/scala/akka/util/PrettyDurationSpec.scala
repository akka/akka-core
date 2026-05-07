/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PrettyDurationSpec extends AnyWordSpec with Matchers {

  import scala.concurrent.duration._

  import akka.util.PrettyDuration._

  val cases: Seq[(Duration, String)] =
    9.nanos -> "9.000 ns" ::
    95.nanos -> "95.00 ns" ::
    999.nanos -> "999.0 ns" ::
    1000.nanos -> "1.000 μs" ::
    9500.nanos -> "9.500 μs" ::
    9500.micros -> "9.500 ms" ::
    9500.millis -> "9.500 s" ::
    95.seconds -> "1.583 min" ::
    95.minutes -> "1.583 h" ::
    95.hours -> "3.958 d" ::
    Nil

  "PrettyDuration" should {

    cases.foreach {
      case (d, expectedValue) =>
        s"print $d nanos as $expectedValue" in {
          d.pretty should ===(expectedValue)
        }
    }

    "work with infinity" in {
      Duration.Inf.pretty should include("infinity")
    }

    "work with -infinity" in {
      Duration.MinusInf.pretty should include("minus infinity")
    }

    "work with undefined" in {
      Duration.Undefined.pretty should include("undefined")
    }
  }
}
