/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import akka.remote.FailureDetector.Clock
import akka.testkit.AkkaSpec

class FailureDetectorRegistrySpec extends AkkaSpec("akka.loglevel = INFO") {

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
      minStdDeviation: FiniteDuration = 10.millis,
      acceptableLostDuration: FiniteDuration = Duration.Zero,
      firstHeartbeatEstimate: FiniteDuration = 1.second,
      clock: Clock = FailureDetector.defaultClock) =
    new PhiAccrualFailureDetector(
      threshold,
      maxSampleSize,
      minStdDeviation,
      acceptableLostDuration,
      firstHeartbeatEstimate = firstHeartbeatEstimate)(clock = clock)

  def createFailureDetectorRegistry(
      threshold: Double = 8.0,
      maxSampleSize: Int = 1000,
      minStdDeviation: FiniteDuration = 10.millis,
      acceptableLostDuration: FiniteDuration = Duration.Zero,
      firstHeartbeatEstimate: FiniteDuration = 1.second,
      clock: Clock = FailureDetector.defaultClock): FailureDetectorRegistry[String] = {
    new DefaultFailureDetectorRegistry[String](
      () =>
        createFailureDetector(
          threshold,
          maxSampleSize,
          minStdDeviation,
          acceptableLostDuration,
          firstHeartbeatEstimate,
          clock))
  }

  "mark node as available after a series of successful heartbeats" in {
    val timeInterval = List[Long](0, 1000, 100, 100)
    val fd = createFailureDetectorRegistry(clock = fakeTimeGenerator(timeInterval))

    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")

    fd.isAvailable("resource1") should ===(true)
  }

  "mark node as dead if heartbeat are missed" in {
    val timeInterval = List[Long](0, 1000, 100, 100, 4000, 3000)
    val fd = createFailureDetectorRegistry(threshold = 3, clock = fakeTimeGenerator(timeInterval))

    fd.heartbeat("resource1") //0
    fd.heartbeat("resource1") //1000
    fd.heartbeat("resource1") //1100

    fd.isAvailable("resource1") should ===(true) //1200
    fd.heartbeat("resource2") //5200, but unrelated resource
    fd.isAvailable("resource1") should ===(false) //8200
  }

  "accept some configured missing heartbeats" in {
    val timeInterval = List[Long](0, 1000, 1000, 1000, 4000, 1000, 1000)
    val fd = createFailureDetectorRegistry(acceptableLostDuration = 3.seconds, clock = fakeTimeGenerator(timeInterval))

    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.isAvailable("resource1") should ===(true)
    fd.heartbeat("resource1")
    fd.isAvailable("resource1") should ===(true)
  }

  "fail after configured acceptable missing heartbeats" in {
    val timeInterval = List[Long](0, 1000, 1000, 1000, 1000, 1000, 500, 500, 5000)
    val fd = createFailureDetectorRegistry(acceptableLostDuration = 3.seconds, clock = fakeTimeGenerator(timeInterval))

    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.isAvailable("resource1") should ===(true)
    fd.heartbeat("resource1")
    fd.isAvailable("resource1") should ===(false)
  }

  "mark node as available after explicit removal of connection" in {
    val timeInterval = List[Long](0, 1000, 100, 100, 100)
    val fd = createFailureDetectorRegistry(clock = fakeTimeGenerator(timeInterval))

    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.heartbeat("resource1")
    fd.isAvailable("resource1") should ===(true)
    fd.remove("resource1")

    fd.isAvailable("resource1") should ===(true)
  }

  "mark node as available after explicit removal of connection and receiving heartbeat again" in {
    val timeInterval = List[Long](0, 1000, 100, 1100, 1100, 1100, 1100, 1100, 100)
    val fd = createFailureDetectorRegistry(clock = fakeTimeGenerator(timeInterval))
    fd.isMonitoring("resource1") should ===(false)

    fd.heartbeat("resource1") //0

    fd.heartbeat("resource1") //1000
    fd.heartbeat("resource1") //1100

    fd.isAvailable("resource1") should ===(true) //2200
    fd.isMonitoring("resource1") should ===(true)

    fd.remove("resource1")

    fd.isMonitoring("resource1") should ===(false)
    fd.isAvailable("resource1") should ===(true) //3300

    // it receives heartbeat from an explicitly removed node
    fd.heartbeat("resource1") //4400
    fd.heartbeat("resource1") //5500
    fd.heartbeat("resource1") //6600

    fd.isAvailable("resource1") should ===(true) //6700
    fd.isMonitoring("resource1") should ===(true)
  }

}
