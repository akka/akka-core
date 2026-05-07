/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import akka.remote.FailureDetector.Clock
import akka.testkit.AkkaSpec

class DeadlineFailureDetectorSpec extends AkkaSpec {

  "A DeadlineFailureDetector" must {

    def fakeTimeGenerator(timeIntervals: Seq[Long]): Clock = new Clock {
      @volatile var times =
        timeIntervals.tail.foldLeft(List[Long](timeIntervals.head))((acc, c) => acc ::: List[Long](acc.last + c))
      override def apply(): Long = {
        val currentTime = times.head
        times = times.tail
        currentTime
      }
    }

    def createFailureDetector(acceptableLostDuration: FiniteDuration, clock: Clock) =
      new DeadlineFailureDetector(acceptableLostDuration, heartbeatInterval = 1.second)(clock = clock)

    "mark node as monitored after a series of successful heartbeats" in {
      val timeInterval = List[Long](0, 1000, 100, 100)
      val fd = createFailureDetector(acceptableLostDuration = 4.seconds, clock = fakeTimeGenerator(timeInterval))
      fd.isMonitoring should ===(false)

      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()

      fd.isMonitoring should ===(true)
      fd.isAvailable should ===(true)
    }

    "mark node as dead if heartbeat are missed" in {
      val timeInterval = List[Long](0, 1000, 100, 100, 7000)
      val fd = createFailureDetector(acceptableLostDuration = 4.seconds, clock = fakeTimeGenerator(timeInterval))

      fd.heartbeat() //0
      fd.heartbeat() //1000
      fd.heartbeat() //1100

      fd.isAvailable should ===(true) //1200
      fd.isAvailable should ===(false) //8200
    }

    "mark node as available if it starts heartbeat again after being marked dead due to detection of failure" in {
      // 1000 regular intervals, 5 minute pause, and then a short pause again that should trigger unreachable again
      val regularIntervals = 0L +: Vector.fill(999)(1000L)
      val timeIntervals = regularIntervals :+ (5 * 60 * 1000L) :+ 100L :+ 900L :+ 100L :+ 7000L :+ 100L :+ 900L :+ 100L :+ 900L
      val fd = createFailureDetector(acceptableLostDuration = 4.seconds, clock = fakeTimeGenerator(timeIntervals))

      for (_ <- 0 until 1000) fd.heartbeat()
      fd.isAvailable should ===(false) // after the long pause
      fd.heartbeat()
      fd.isAvailable should ===(true)
      fd.heartbeat()
      fd.isAvailable should ===(false) // after the 7 seconds pause
      fd.heartbeat()
      fd.isAvailable should ===(true)
      fd.heartbeat()
      fd.isAvailable should ===(true)
    }

    "accept some configured missing heartbeats" in {
      val timeInterval = List[Long](0, 1000, 1000, 1000, 4000, 1000, 1000)
      val fd = createFailureDetector(acceptableLostDuration = 4.seconds, clock = fakeTimeGenerator(timeInterval))

      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.isAvailable should ===(true)
      fd.heartbeat()
      fd.isAvailable should ===(true)
    }

    "fail after configured acceptable missing heartbeats" in {
      val timeInterval = List[Long](0, 1000, 1000, 1000, 1000, 1000, 500, 500, 5000)
      val fd = createFailureDetector(acceptableLostDuration = 4.seconds, clock = fakeTimeGenerator(timeInterval))

      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()
      fd.isAvailable should ===(true)
      fd.heartbeat()
      fd.isAvailable should ===(false)
    }

  }

}
