/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package akka.coordination.lease

import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigValueType }

import akka.util.JavaDurationConverters._

object TimeoutSettings {
  def apply(config: Config): TimeoutSettings = {
    val heartBeatTimeout = config.getDuration("heartbeat-timeout").asScala
    val heartBeatInterval = config.getValue("heartbeat-interval").valueType() match {
      case ConfigValueType.STRING if config.getString("heartbeat-interval").isEmpty =>
        (heartBeatTimeout / 10).max(5.seconds)
      case _ => config.getDuration("heartbeat-interval").asScala
    }
    require(heartBeatInterval < (heartBeatTimeout / 2), "heartbeat-interval must be less than half heartbeat-timeout")
    new TimeoutSettings(heartBeatInterval, heartBeatTimeout, config.getDuration("lease-operation-timeout").asScala)
  }

}

final class TimeoutSettings(
    val heartbeatInterval: FiniteDuration,
    val heartbeatTimeout: FiniteDuration,
    val operationTimeout: FiniteDuration) {

  /**
   * Java API
   */
  def getHeartbeatInterval(): java.time.Duration = heartbeatInterval.asJava

  /**
   * Java API
   */
  def getHeartbeatTimeout(): java.time.Duration = heartbeatTimeout.asJava

  /**
   * Java API
   */
  def getOperationTimeout(): java.time.Duration = operationTimeout.asJava

  /**
   * Java API
   */
  def withHeartbeatInterval(heartbeatInterval: java.time.Duration): TimeoutSettings = {
    copy(heartbeatInterval = heartbeatInterval.asScala)
  }

  /**
   * Java API
   */
  def withHeartbeatTimeout(heartbeatTimeout: java.time.Duration): TimeoutSettings = {
    copy(heartbeatTimeout = heartbeatTimeout.asScala)
  }

  /**
   * Java API
   */
  def withOperationTimeout(operationTimeout: java.time.Duration): TimeoutSettings = {
    copy(operationTimeout = operationTimeout.asScala)
  }

  def withHeartbeatInterval(heartbeatInterval: FiniteDuration): TimeoutSettings = {
    copy(heartbeatInterval = heartbeatInterval)
  }
  def withHeartbeatTimeout(heartbeatTimeout: FiniteDuration): TimeoutSettings = {
    copy(heartbeatTimeout = heartbeatTimeout)
  }
  def withOperationTimeout(operationTimeout: FiniteDuration): TimeoutSettings = {
    copy(operationTimeout = operationTimeout)
  }

  private def copy(
      heartbeatInterval: FiniteDuration = heartbeatInterval,
      heartbeatTimeout: FiniteDuration = heartbeatTimeout,
      operationTimeout: FiniteDuration = operationTimeout): TimeoutSettings = {
    new TimeoutSettings(heartbeatInterval, heartbeatTimeout, operationTimeout)
  }

  override def toString = s"TimeoutSettings($heartbeatInterval, $heartbeatTimeout, $operationTimeout)"
}
