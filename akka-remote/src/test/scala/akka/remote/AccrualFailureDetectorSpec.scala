/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.collection.immutable.TreeMap
import scala.concurrent.duration._

import akka.remote.FailureDetector.Clock
import akka.testkit.AkkaSpec

class AccrualFailureDetectorSpec extends AkkaSpec("akka.loglevel = INFO") {

  "An AccrualFailureDetector" must {

    def fakeTimeGenerator(timeIntervals: Seq[Long]): Clock = new Clock {
      @volatile var times =
        timeIntervals.tail.foldLeft(List[Long](timeIntervals.head))((acc, c) => acc ::: List[Long](acc.last + c))
      override def apply(): Long = {
        val currentTime = times.head
        times = times.tail
        currentTime
      }
    }

    def createFailureDetector(
        threshold: Double = 8.0,
        maxSampleSize: Int = 1000,
        minStdDeviation: FiniteDuration = 100.millis,
        acceptableLostDuration: FiniteDuration = Duration.Zero,
        firstHeartbeatEstimate: FiniteDuration = 1.second,
        clock: Clock = FailureDetector.defaultClock) =
      new PhiAccrualFailureDetector(
        threshold,
        maxSampleSize,
        minStdDeviation,
        acceptableLostDuration,
        firstHeartbeatEstimate = firstHeartbeatEstimate)(clock = clock)

    def cdf(phi: Double) = 1.0 - math.pow(10, -phi)

    "use good enough cumulative distribution function" in {
      val fd = createFailureDetector()
      cdf(fd.phi(0, 0, 10)) should ===(0.5 +- (0.001))
      cdf(fd.phi(6L, 0, 10)) should ===(0.7257 +- (0.001))
      cdf(fd.phi(15L, 0, 10)) should ===(0.9332 +- (0.001))
      cdf(fd.phi(20L, 0, 10)) should ===(0.97725 +- (0.001))
      cdf(fd.phi(25L, 0, 10)) should ===(0.99379 +- (0.001))
      cdf(fd.phi(35L, 0, 10)) should ===(0.99977 +- (0.001))
      cdf(fd.phi(40L, 0, 10)) should ===(0.99997 +- (0.0001))

      for (Seq(x, y) <- (0 to 40).toList.sliding(2)) {
        fd.phi(x, 0, 10) should be < (fd.phi(y, 0, 10))
      }

      cdf(fd.phi(22, 20.0, 3)) should ===(0.7475 +- (0.001))
    }

    "handle outliers without losing precision or hitting exceptions" in {
      val fd = createFailureDetector()
      fd.phi(10L, 0, 1) should ===(38.0 +- 1.0)
      fd.phi(-25L, 0, 1) should ===(0.0)
    }

    "return realistic phi values" in {
      val fd = createFailureDetector()
      val test = TreeMap(0 -> 0.0, 500 -> 0.1, 1000 -> 0.3, 1200 -> 1.6, 1400 -> 4.7, 1600 -> 10.8, 1700 -> 15.3)
      for ((timeDiff, expectedPhi) <- test) {
        fd.phi(timeDiff = timeDiff, mean = 1000.0, stdDeviation = 100.0) should ===(expectedPhi +- (0.1))
      }

      // larger stdDeviation results => lower phi
      fd.phi(timeDiff = 1100, mean = 1000.0, stdDeviation = 500.0) should be < (fd
        .phi(timeDiff = 1100, mean = 1000.0, stdDeviation = 100.0))
    }

    "return phi value of 0.0 on startup for each address, when no heartbeats" in {
      val fd = createFailureDetector()
      fd.phi should ===(0.0)
      fd.phi should ===(0.0)
    }

    "return phi based on guess when only one heartbeat" in {
      val timeInterval = List[Long](0, 1000, 1000, 1000, 1000)
      val fd = createFailureDetector(firstHeartbeatEstimate = 1.seconds, clock = fakeTimeGenerator(timeInterval))

      fd.heartbeat()
      fd.phi should ===(0.3 +- 0.2)
      fd.phi should ===(4.5 +- 0.3)
      fd.phi should be > (15.0)
    }

    "return phi value using first interval after second heartbeat" in {
      val timeInterval = List[Long](0, 100, 100, 100)
      val fd = createFailureDetector(clock = fakeTimeGenerator(timeInterval))

      fd.heartbeat()
      fd.phi should be > (0.0)
      fd.heartbeat()
      fd.phi should be > (0.0)
    }

    "mark node as monitored after a series of successful heartbeats" in {
      val timeInterval = List[Long](0, 1000, 100, 100)
      val fd = createFailureDetector(clock = fakeTimeGenerator(timeInterval))
      fd.isMonitoring should ===(false)

      fd.heartbeat()
      fd.heartbeat()
      fd.heartbeat()

      fd.isMonitoring should ===(true)
      fd.isAvailable should ===(true)
    }

    "mark node as dead if heartbeat are missed" in {
      val timeInterval = List[Long](0, 1000, 100, 100, 7000)
      val fd = createFailureDetector(threshold = 3, clock = fakeTimeGenerator(timeInterval))

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
      val fd = createFailureDetector(
        threshold = 8,
        acceptableLostDuration = 3.seconds,
        clock = fakeTimeGenerator(timeIntervals))

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
      val fd = createFailureDetector(acceptableLostDuration = 3.seconds, clock = fakeTimeGenerator(timeInterval))

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
      val fd = createFailureDetector(acceptableLostDuration = 3.seconds, clock = fakeTimeGenerator(timeInterval))

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

    "use maxSampleSize heartbeats" in {
      val timeInterval = List[Long](0, 100, 100, 100, 100, 600, 500, 500, 500, 500, 500)
      val fd = createFailureDetector(maxSampleSize = 3, clock = fakeTimeGenerator(timeInterval))

      // 100 ms interval
      fd.heartbeat() //0
      fd.heartbeat() //100
      fd.heartbeat() //200
      fd.heartbeat() //300
      val phi1 = fd.phi //400
      // 500 ms interval, should become same phi when 100 ms intervals have been dropped
      fd.heartbeat() //1000
      fd.heartbeat() //1500
      fd.heartbeat() //2000
      fd.heartbeat() //2500
      val phi2 = fd.phi //3000
      phi2 should ===(phi1 +- (0.001))
    }

  }

  "Statistics for heartbeats" must {

    "calculate correct mean and variance" in {
      val samples = Seq(100, 200, 125, 340, 130)
      val stats = samples.foldLeft(HeartbeatHistory(maxSampleSize = 20)) { (stats, value) =>
        stats :+ value
      }
      stats.mean should ===(179.0 +- 0.00001)
      stats.variance should ===(7584.0 +- 0.00001)
    }

    "have 0.0 variance for one sample" in {
      (HeartbeatHistory(600) :+ 1000L).variance should ===(0.0 +- 0.00001)
    }

    "be capped by the specified maxSampleSize" in {
      val history3 = HeartbeatHistory(maxSampleSize = 3) :+ 100 :+ 110 :+ 90
      history3.mean should ===(100.0 +- 0.00001)
      history3.variance should ===(66.6666667 +- 0.00001)

      val history4 = history3 :+ 140
      history4.mean should ===(113.333333 +- 0.00001)
      history4.variance should ===(422.222222 +- 0.00001)

      val history5 = history4 :+ 80
      history5.mean should ===(103.333333 +- 0.00001)
      history5.variance should ===(688.88888889 +- 0.00001)
    }

  }
}
