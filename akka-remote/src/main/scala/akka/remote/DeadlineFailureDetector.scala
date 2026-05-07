/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config

import akka.event.EventStream
import akka.remote.FailureDetector.Clock
import akka.util.Helpers.ConfigOps

/**
 * Implementation of failure detector using an absolute timeout of missing heartbeats
 * to trigger unavailability.
 *
 * [[#isAvailable]] will return `false` if there is no [[#heartbeat]] within the duration
 * `heartbeatInterval + acceptableHeartbeatPause`.
 *
 * @param acceptableHeartbeatPause Duration corresponding to number of potentially lost/delayed
 *   heartbeats that will be accepted before considering it to be an anomaly.
 *
 * @param heartbeatInterval Expected heartbeat interval
 *
 * @param clock The clock, returning current time in milliseconds, but can be faked for testing
 *   purposes. It is only used for measuring intervals (duration).
 */
class DeadlineFailureDetector(val acceptableHeartbeatPause: FiniteDuration, val heartbeatInterval: FiniteDuration)(
    implicit
    clock: Clock)
    extends FailureDetector {

  /**
   * Constructor that reads parameters from config.
   * Expecting config properties named `acceptable-heartbeat-pause`.
   */
  def this(config: Config, ev: EventStream) =
    this(
      acceptableHeartbeatPause = config.getMillisDuration("acceptable-heartbeat-pause"),
      heartbeatInterval = config.getMillisDuration("heartbeat-interval"))

  require(acceptableHeartbeatPause >= Duration.Zero, "failure-detector.acceptable-heartbeat-pause must be >= 0 s")
  require(heartbeatInterval > Duration.Zero, "failure-detector.heartbeat-interval must be > 0 s")

  private val deadlineMillis = acceptableHeartbeatPause.toMillis + heartbeatInterval.toMillis
  @volatile private var heartbeatTimestamp = 0L //not used until active (first heartbeat)
  @volatile private var active = false

  override def isAvailable: Boolean = isAvailable(clock())

  private def isAvailable(timestamp: Long): Boolean =
    if (active) (heartbeatTimestamp + deadlineMillis) > timestamp
    else true // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections

  override def isMonitoring: Boolean = active

  final override def heartbeat(): Unit = {
    heartbeatTimestamp = clock()
    active = true
  }

}
