/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration._

import language.postfixOps

import akka.testkit.AkkaSpec

class DurationSpec extends AkkaSpec {

  "Duration" must {

    "form a one-dimensional vector field" in {
      val zero = 0 seconds
      val one = 1 second
      val two = one + one
      val three = 3 * one
      (0 * one) should ===(zero)
      (2 * one) should ===(two)
      (three - two) should ===(one)
      (three / 3) should ===(one)
      (two / one) should ===(2d)
      (one + zero) should ===(one)
      (one / 1000000) should ===(1.micro)
    }

    "respect correct treatment of infinities" in {
      val one = 1.second
      val inf = Duration.Inf
      val minf = Duration.MinusInf
      val undefined = Duration.Undefined
      (-inf) should ===(minf)
      (minf + inf) should ===(undefined)
      (inf - inf) should ===(undefined)
      (inf + minf) should ===(undefined)
      (minf - minf) should ===(undefined)
      (inf + inf) should ===(inf)
      (inf - minf) should ===(inf)
      (minf - inf) should ===(minf)
      (minf + minf) should ===(minf)
      assert(inf == inf)
      assert(minf == minf)
      inf.compareTo(inf) should ===(0)
      inf.compareTo(one) should ===(1)
      minf.compareTo(minf) should ===(0)
      minf.compareTo(one) should ===(-1)
      assert(inf != minf)
      assert(minf != inf)
      assert(one != inf)
      assert(minf != one)
    }

    /*"check its range" in {
      for (unit <- Seq(DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS)) {
        val x = unit.convert(Long.MaxValue, NANOSECONDS)
        val dur = Duration(x, unit)
        val mdur = Duration(-x, unit)
        -mdur should ===(dur)
        intercept[IllegalArgumentException] { Duration(x + 10000000d, unit) }
        intercept[IllegalArgumentException] { Duration(-x - 10000000d, unit) }
        if (unit != NANOSECONDS) {
          intercept[IllegalArgumentException] { Duration(x + 1, unit) }
          intercept[IllegalArgumentException] { Duration(-x - 1, unit) }
        }
        intercept[IllegalArgumentException] { dur + 1.day }
        intercept[IllegalArgumentException] { mdur - 1.day }
        intercept[IllegalArgumentException] { dur * 1.1 }
        intercept[IllegalArgumentException] { mdur * 1.1 }
        intercept[IllegalArgumentException] { dur * 2.1 }
        intercept[IllegalArgumentException] { mdur * 2.1 }
        intercept[IllegalArgumentException] { dur / 0.9 }
        intercept[IllegalArgumentException] { mdur / 0.9 }
        intercept[IllegalArgumentException] { dur / 0.4 }
        intercept[IllegalArgumentException] { mdur / 0.4 }
        Duration(x + unit.toString.toLowerCase)
        Duration("-" + x + unit.toString.toLowerCase)
        intercept[IllegalArgumentException] { Duration("%.0f".format(x + 10000000d) + unit.toString.toLowerCase) }
        intercept[IllegalArgumentException] { Duration("-%.0f".format(x + 10000000d) + unit.toString.toLowerCase) }
      }
    }*/

    "support fromNow" in {
      val dead = 2.seconds.fromNow
      val dead2 = 2.seconds(fromNow)
      // view bounds vs. very local type inference vs. operator precedence: sigh
      dead.timeLeft should be > (1 second: Duration)
      dead2.timeLeft should be > (1 second: Duration)
      Thread.sleep(1.second.toMillis)
      dead.timeLeft should be < (1 second: Duration)
      dead2.timeLeft should be < (1 second: Duration)
    }

  }

}
