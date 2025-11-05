/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RestartCounterSpec extends AnyWordSpec with Matchers {

  "RestartCounter" must {

    "count max restarts within duration" in {
      val counter = new RestartCounter(3, 3.seconds)
      counter.restart() should ===(true)
      counter.restart() should ===(true)
      counter.restart() should ===(true)
      counter.restart() should ===(false)
      counter.count() should ===(4)
    }

    "allow sporadic restarts" in {
      val counter = new RestartCounter(3, 10.millis)
      for (_ <- 1 to 10) {
        counter.restart() should ===(true)
        Thread.sleep(20)
      }
    }

    "reset count after timeout" in {
      val counter = new RestartCounter(3, 500.millis)
      counter.restart()
      counter.restart()
      counter.count() should ===(2)
      Thread.sleep(600)
      counter.restart()
      counter.count() should ===(1)
    }
  }
}
