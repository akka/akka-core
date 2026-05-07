/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config

import akka.event.EventStream
import akka.event.Logging
import akka.event.Logging.Warning
import akka.remote.FailureDetector.Clock
import akka.util.Helpers.ConfigOps

/**
 * Implementation of 'The Phi Accrual Failure Detector' by Hayashibara et al. as defined in their paper:
 * [https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf]
 *
 * The suspicion level of failure is given by a value called φ (phi).
 * The basic idea of the φ failure detector is to express the value of φ on a scale that
 * is dynamically adjusted to reflect current network conditions. A configurable
 * threshold is used to decide if φ is considered to be a failure.
 *
 * The value of φ is calculated as:
 *
 * {{{
 * φ = -log10(1 - F(timeSinceLastHeartbeat)
 * }}}
 * where F is the cumulative distribution function of a normal distribution with mean
 * and standard deviation estimated from historical heartbeat inter-arrival times.
 *
 * @param threshold A low threshold is prone to generate many wrong suspicions but ensures a quick detection in the event
 *   of a real crash. Conversely, a high threshold generates fewer mistakes but needs more time to detect
 *   actual crashes
 * @param maxSampleSize Number of samples to use for calculation of mean and standard deviation of
 *   inter-arrival times.
 * @param minStdDeviation Minimum standard deviation to use for the normal distribution used when calculating phi.
 *   Too low standard deviation might result in too much sensitivity for sudden, but normal, deviations
 *   in heartbeat inter arrival times.
 * @param acceptableHeartbeatPause Duration corresponding to number of potentially lost/delayed
 *   heartbeats that will be accepted before considering it to be an anomaly.
 *   This margin is important to be able to survive sudden, occasional, pauses in heartbeat
 *   arrivals, due to for example garbage collect or network drop.
 * @param firstHeartbeatEstimate Bootstrap the stats with heartbeats that corresponds to
 *   to this duration, with a with rather high standard deviation (since environment is unknown
 *   in the beginning)
 * @param clock The clock, returning current time in milliseconds, but can be faked for testing
 *   purposes. It is only used for measuring intervals (duration).
 */
class PhiAccrualFailureDetector(
    val threshold: Double,
    val maxSampleSize: Int,
    val minStdDeviation: FiniteDuration,
    val acceptableHeartbeatPause: FiniteDuration,
    val firstHeartbeatEstimate: FiniteDuration,
    eventStream: Option[EventStream])(
    implicit
    clock: Clock)
    extends FailureDetector {

  /**
   * Constructor without eventStream to support backwards compatibility
   */
  def this(
      threshold: Double,
      maxSampleSize: Int,
      minStdDeviation: FiniteDuration,
      acceptableHeartbeatPause: FiniteDuration,
      firstHeartbeatEstimate: FiniteDuration)(implicit clock: Clock) =
    this(threshold, maxSampleSize, minStdDeviation, acceptableHeartbeatPause, firstHeartbeatEstimate, None)(clock)

  /**
   * Constructor that reads parameters from config.
   * Expecting config properties named `threshold`, `max-sample-size`,
   * `min-std-deviation`, `acceptable-heartbeat-pause` and
   * `heartbeat-interval`.
   */
  def this(config: Config, ev: EventStream) =
    this(
      threshold = config.getDouble("threshold"),
      maxSampleSize = config.getInt("max-sample-size"),
      minStdDeviation = config.getMillisDuration("min-std-deviation"),
      acceptableHeartbeatPause = config.getMillisDuration("acceptable-heartbeat-pause"),
      firstHeartbeatEstimate = config.getMillisDuration("heartbeat-interval"),
      Some(ev))

  require(threshold > 0.0, "failure-detector.threshold must be > 0")
  require(maxSampleSize > 0, "failure-detector.max-sample-size must be > 0")
  require(minStdDeviation > Duration.Zero, "failure-detector.min-std-deviation must be > 0")
  require(acceptableHeartbeatPause >= Duration.Zero, "failure-detector.acceptable-heartbeat-pause must be >= 0")
  require(firstHeartbeatEstimate > Duration.Zero, "failure-detector.heartbeat-interval must be > 0")

  // guess statistics for first heartbeat,
  // important so that connections with only one heartbeat becomes unavailable
  private val firstHeartbeat: HeartbeatHistory = {
    // bootstrap with 2 entries with rather high standard deviation
    val mean = firstHeartbeatEstimate.toMillis
    val stdDeviation = mean / 4
    HeartbeatHistory(maxSampleSize) :+ (mean - stdDeviation) :+ (mean + stdDeviation)
  }

  private val acceptableHeartbeatPauseMillis = acceptableHeartbeatPause.toMillis

  // address below was introduced as a var because of binary compatibility constraints
  private[akka] var address: String = "N/A"

  /**
   * Implement using optimistic lockless concurrency, all state is represented
   * by this immutable case class and managed by an AtomicReference.
   *
   * Cannot be final due to https://github.com/scala/bug/issues/4440
   */
  private case class State(history: HeartbeatHistory, timestamp: Option[Long])

  private val state = new AtomicReference[State](State(history = firstHeartbeat, timestamp = None))

  override def isAvailable: Boolean = isAvailable(clock())

  private def isAvailable(timestamp: Long): Boolean = phi(timestamp) < threshold

  override def isMonitoring: Boolean = state.get.timestamp.nonEmpty

  @tailrec
  final override def heartbeat(): Unit = {

    val timestamp = clock()
    val oldState = state.get

    val newHistory = oldState.timestamp match {
      case None =>
        // this is heartbeat from a new resource
        // add starter records for this new resource
        firstHeartbeat
      case Some(latestTimestamp) =>
        // this is a known connection
        val interval = timestamp - latestTimestamp
        // don't use the first heartbeat after failure for the history, since a long pause will skew the stats
        if (isAvailable(timestamp)) {
          if (interval >= (acceptableHeartbeatPauseMillis / 3 * 2) && eventStream.isDefined)
            eventStream.get.publish(
              Warning(
                this.toString,
                getClass,
                s"heartbeat interval is growing too large for address $address: $interval millis",
                Logging.emptyMDC,
                RemoteLogMarker.failureDetectorGrowing(address)))
          oldState.history :+ interval
        } else oldState.history
    }

    // record new timestamp and possibly-amended history
    val newState = oldState.copy(history = newHistory, timestamp = Some(timestamp))

    // if we won the race then update else try again
    if (!state.compareAndSet(oldState, newState)) heartbeat() // recur
  }

  /**
   * The suspicion level of the accrual failure detector.
   *
   * If a connection does not have any records in failure detector then it is
   * considered healthy.
   */
  def phi: Double = phi(clock())

  private def phi(timestamp: Long): Double = {
    val oldState = state.get
    val oldTimestamp = oldState.timestamp

    if (oldTimestamp.isEmpty) 0.0 // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections
    else {
      val timeDiff = timestamp - oldTimestamp.get

      val history = oldState.history
      val mean = history.mean
      val stdDeviation = ensureValidStdDeviation(history.stdDeviation)

      phi(timeDiff, mean + acceptableHeartbeatPauseMillis, stdDeviation)
    }
  }

  /**
   * Calculation of phi, derived from the Cumulative distribution function for
   * N(mean, stdDeviation) normal distribution, given by
   * 1.0 / (1.0 + math.exp(-y * (1.5976 + 0.070566 * y * y)))
   * where y = (x - mean) / standard_deviation
   * This is an approximation defined in β Mathematics Handbook (Logistic approximation).
   * Error is 0.00014 at +- 3.16
   * The calculated value is equivalent to -log10(1 - CDF(y))
   */
  private[akka] def phi(timeDiff: Long, mean: Double, stdDeviation: Double): Double = {
    val y = (timeDiff - mean) / stdDeviation
    val e = math.exp(-y * (1.5976 + 0.070566 * y * y))
    if (timeDiff > mean)
      -math.log10(e / (1.0 + e))
    else
      -math.log10(1.0 - 1.0 / (1.0 + e))
  }

  private val minStdDeviationMillis = minStdDeviation.toMillis.toDouble

  private def ensureValidStdDeviation(stdDeviation: Double): Double = math.max(stdDeviation, minStdDeviationMillis)

}

private[akka] object HeartbeatHistory {

  /**
   * Create an empty HeartbeatHistory, without any history.
   * Can only be used as starting point for appending intervals.
   * The stats (mean, variance, stdDeviation) are not defined for
   * for empty HeartbeatHistory, i.e. throws ArithmeticException.
   */
  def apply(maxSampleSize: Int): HeartbeatHistory =
    HeartbeatHistory(
      maxSampleSize = maxSampleSize,
      intervals = immutable.IndexedSeq.empty,
      intervalSum = 0L,
      squaredIntervalSum = 0L)

}

/**
 * Holds the heartbeat statistics for a specific node Address.
 * It is capped by the number of samples specified in `maxSampleSize`.
 *
 * The stats (mean, variance, stdDeviation) are not defined for
 * for empty HeartbeatHistory, i.e. throws ArithmeticException.
 */
private[akka] final case class HeartbeatHistory private (
    maxSampleSize: Int,
    intervals: immutable.IndexedSeq[Long],
    intervalSum: Long,
    squaredIntervalSum: Long) {

  // Heartbeat histories are created trough the firstHeartbeat variable of the PhiAccrualFailureDetector
  // which always have intervals.size > 0.
  if (maxSampleSize < 1)
    throw new IllegalArgumentException(s"maxSampleSize must be >= 1, got [$maxSampleSize]")
  if (intervalSum < 0L)
    throw new IllegalArgumentException(s"intervalSum must be >= 0, got [$intervalSum]")
  if (squaredIntervalSum < 0L)
    throw new IllegalArgumentException(s"squaredIntervalSum must be >= 0, got [$squaredIntervalSum]")

  def mean: Double = intervalSum.toDouble / intervals.size

  def variance: Double = (squaredIntervalSum.toDouble / intervals.size) - (mean * mean)

  def stdDeviation: Double = math.sqrt(variance)

  @tailrec
  final def :+(interval: Long): HeartbeatHistory = {
    if (intervals.size < maxSampleSize)
      HeartbeatHistory(
        maxSampleSize,
        intervals = intervals :+ interval,
        intervalSum = intervalSum + interval,
        squaredIntervalSum = squaredIntervalSum + pow2(interval))
    else
      dropOldest :+ interval // recur
  }

  private def dropOldest: HeartbeatHistory =
    HeartbeatHistory(
      maxSampleSize,
      intervals = intervals.drop(1),
      intervalSum = intervalSum - intervals.head,
      squaredIntervalSum = squaredIntervalSum - pow2(intervals.head))

  private def pow2(x: Long) = x * x
}
