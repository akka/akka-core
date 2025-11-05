/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration._

import akka.testkit.AkkaSpec
import akka.testkit.TimingTest

class ScheduledClockSpec extends AkkaSpec {

  "ScheduledClock" must {

    "update at given interval" taggedAs TimingTest in {
      val interval = 100.millis
      val clock = new ScheduledClock(interval, system.scheduler, system.dispatcher)
      val t1 = clock.currentTime()
      // interval is much smaller but to avoid flaky test due to pauses we sleep longer
      Thread.sleep(2000)
      val t2 = clock.currentTime()
      (t2 - t1) shouldBe >=(interval.toNanos)

      Thread.sleep(2000)
      val t3 = clock.currentTime()
      (t3 - t2) shouldBe >=(interval.toNanos)
    }

    "increment each call to to currentTime" in {
      val interval = 10.seconds // don't update
      val clock = new ScheduledClock(interval, system.scheduler, system.dispatcher)
      val t1 = clock.currentTime()
      val t2 = clock.currentTime()
      val t3 = clock.currentTime()
      (t2 - t1) shouldBe 1L
      (t3 - t2) shouldBe 1L
    }

    "not go backwards" taggedAs TimingTest in {
      val interval = 20.millis
      val clock = new ScheduledClock(interval, system.scheduler, system.dispatcher)

      // run a few threads updating in the background
      val backgroundTasks =
        (1 to 3).map { _ =>
          system.scheduler.scheduleWithFixedDelay(10.millis, 10.millis) { () =>
            (1 to 1000).foreach { _ =>
              clock.currentTime()
            }
          }(system.dispatcher)
        }
      try {
        var t = clock.currentTime()
        (1 to 1000000).foreach { n =>
          val t2 = clock.currentTime()
          (t2 - t) shouldBe >=(0L)
          t = t2
          if (n % 100000 == 0)
            Thread.sleep(10)
        }
      } finally {
        backgroundTasks.foreach(_.cancel())
      }
    }

  }
}
