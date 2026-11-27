/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.function.{ Consumer => JConsumer }

import scala.jdk.DurationConverters._
import scala.jdk.FunctionConverters._
import scala.concurrent.duration.FiniteDuration

import akka.event.Logging
import akka.event.Logging.LogLevel
import akka.event.LoggingAdapter
import akka.util.ConstantFun

final class RestartSettings private (
    val minBackoff: FiniteDuration,
    val maxBackoff: FiniteDuration,
    val randomFactor: Double,
    val maxRestarts: Int,
    val maxRestartsWithin: FiniteDuration,
    val logSettings: RestartSettings.LogSettings,
    val restartOn: Throwable => Boolean) {

  /** Scala API: minimum (initial) duration until the child actor will started again, if it is terminated */
  def withMinBackoff(value: FiniteDuration): RestartSettings = copy(minBackoff = value)

  /** Java API: minimum (initial) duration until the child actor will started again, if it is terminated */
  def withMinBackoff(value: java.time.Duration): RestartSettings = copy(minBackoff = value.toScala)

  /** Scala API: the exponential back-off is capped to this duration */
  def withMaxBackoff(value: FiniteDuration): RestartSettings = copy(maxBackoff = value)

  /** Java API: the exponential back-off is capped to this duration */
  def withMaxBackoff(value: java.time.Duration): RestartSettings = copy(maxBackoff = value.toScala)

  /**
   * After calculation of the exponential back-off an additional random delay based on this factor is added
   * e.g. `0.2` adds up to `20%` delay.  In order to skip this additional delay pass in `0`
   */
  def withRandomFactor(value: Double): RestartSettings = copy(randomFactor = value)

  /** Scala API: The amount of restarts is capped to `count` within a timeframe of `within` */
  def withMaxRestarts(count: Int, within: FiniteDuration): RestartSettings =
    copy(maxRestarts = count, maxRestartsWithin = within)

  /** Java API: The amount of restarts is capped to `count` within a timeframe of `within` */
  def withMaxRestarts(count: Int, within: java.time.Duration): RestartSettings =
    copy(maxRestarts = count, maxRestartsWithin = within.toScala)

  /** Decides whether the failure should restart the stream or make the surrounding stream fail */
  def withRestartOn(restartOn: java.util.function.Predicate[Throwable]): RestartSettings =
    copy(restartOn = restartOn.asScala)

  def withLogSettings(newLogSettings: RestartSettings.LogSettings): RestartSettings =
    copy(logSettings = newLogSettings)

  override def toString: String =
    "RestartSettings(" +
    s"minBackoff=$minBackoff," +
    s"maxBackoff=$maxBackoff," +
    s"randomFactor=$randomFactor," +
    s"maxRestarts=$maxRestarts," +
    s"maxRestartsWithin=$maxRestartsWithin)"

  private def copy(
      minBackoff: FiniteDuration = minBackoff,
      maxBackoff: FiniteDuration = maxBackoff,
      randomFactor: Double = randomFactor,
      maxRestarts: Int = maxRestarts,
      maxRestartsWithin: FiniteDuration = maxRestartsWithin,
      logSettings: RestartSettings.LogSettings = logSettings,
      restartOn: Throwable => Boolean = restartOn): RestartSettings =
    new RestartSettings(minBackoff, maxBackoff, randomFactor, maxRestarts, maxRestartsWithin, logSettings, restartOn)

}

object RestartSettings {

  /** Scala API */
  def apply(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double): RestartSettings =
    new RestartSettings(
      minBackoff = minBackoff,
      maxBackoff = maxBackoff,
      randomFactor = randomFactor,
      maxRestarts = Int.MaxValue,
      maxRestartsWithin = minBackoff,
      logSettings = LogSettings.defaultSettings,
      restartOn = ConstantFun.anyToTrue)

  /** Java API */
  def create(minBackoff: java.time.Duration, maxBackoff: java.time.Duration, randomFactor: Double): RestartSettings =
    new RestartSettings(
      minBackoff = minBackoff.toScala,
      maxBackoff = maxBackoff.toScala,
      randomFactor = randomFactor,
      maxRestarts = Int.MaxValue,
      maxRestartsWithin = minBackoff.toScala,
      logSettings = LogSettings.defaultSettings,
      restartOn = ConstantFun.anyToTrue)

  /** Java API */
  def createLogSettings(logLevel: LogLevel): LogSettings =
    LogSettings(logLevel)

  object LogSettings {
    private[akka] val defaultSettings =
      new LogSettings(Logging.WarningLevel, Logging.ErrorLevel, criticalLogLevelAfter = Int.MaxValue)

    def apply(logLevel: LogLevel): LogSettings = defaultSettings.copy(logLevel = logLevel)

  }

  final class LogSettings private (
      val logLevel: LogLevel,
      val criticalLogLevel: LogLevel,
      val criticalLogLevelAfter: Int,
      val verboseLogsAfter: Option[Int],
      val failureLogging: Option[JConsumer[RestartFailure]]) {

    // For source and binary compatibility
    def this(
        logLevel: LogLevel,
        criticalLogLevel: LogLevel,
        criticalLogLevelAfter: Int,
        verboseLogsAfter: Option[Int]) =
      this(logLevel, criticalLogLevel, criticalLogLevelAfter, verboseLogsAfter, None)

    // For source and binary compatibility
    def this(logLevel: LogLevel, criticalLogLevel: LogLevel, criticalLogLevelAfter: Int) =
      this(logLevel, criticalLogLevel, criticalLogLevelAfter, None)

    def withLogLevel(level: LogLevel): LogSettings =
      copy(logLevel = level)

    /**
     * Possibility to use another log level after a given number of errors.
     * The initial errors are logged at the level defined with [[LogSettings.withLogLevel]].
     * For example, the first 3 errors can be logged at INFO level and thereafter at ERROR level.
     *
     * The counter (and log level) is reset after the [[RestartSettings.maxRestartsWithin]]
     * duration.
     */
    def withCriticalLogLevel(criticalLevel: LogLevel, afterErrors: Int): LogSettings =
      copy(criticalLogLevel = criticalLevel, criticalLogLevelAfter = afterErrors)

    /**
     * If set, warning and error logs will use only the exception message up to `afterErrors` consecutive errors,
     * after reaching the number of consecutive errors, the entire exception with stacktrace is logged.
     *
     * The counter (and verbosity level) is reset after the [[RestartSettings.maxRestartsWithin]]
     * duration.
     */
    def withVerboseLogsAfter(afterErrors: Int): LogSettings =
      copy(verboseLogsAfter = Some(afterErrors))

    /**
     * Replace the logging of failures that restart the stream with a custom callback, for example to log some
     * failures to a different logger or at a different level.
     *
     * The callback is called for each failure that restarts the stream, also when logging is disabled for the
     * stream, and then is the only logging of the failure. [[RestartFailure.defaultLogLevel]] tells what the
     * default logging would have done, and [[RestartFailure.logDefault]] does it. The callback should be
     * lightweight, if it throws the stream fails.
     *
     * Restarts because of stream completion are not passed to the callback.
     */
    def withFailureLogging(logFailure: JConsumer[RestartFailure]): LogSettings =
      copy(failureLogging = Some(logFailure))

    private def copy(
        logLevel: LogLevel = logLevel,
        criticalLogLevel: LogLevel = criticalLogLevel,
        criticalLogLevelAfter: Int = criticalLogLevelAfter,
        verboseLogsAfter: Option[Int] = verboseLogsAfter,
        failureLogging: Option[JConsumer[RestartFailure]] = failureLogging): LogSettings =
      new LogSettings(
        logLevel = logLevel,
        criticalLogLevel = criticalLogLevel,
        criticalLogLevelAfter = criticalLogLevelAfter,
        verboseLogsAfter = verboseLogsAfter,
        failureLogging = failureLogging)
  }

  /**
   * A failure that restarts the stream, passed to the callback of [[LogSettings.withFailureLogging]].
   *
   * Instances are created by Akka streams. It is a trait so that a callback can be unit tested with
   * an instance created by the test, but it should not be implemented for other purposes, as members may be added
   * in future releases.
   */
  trait RestartFailure {

    /** The failure that restarts the stream */
    def cause: Throwable

    /** The number of consecutive failures, including this one, as shown in the default log message */
    def consecutiveFailures: Int

    /**
     * The level the default logging would log this failure at, as decided by the [[LogSettings]] and the stream
     * attributes. `Logging.OffLevel` if it would not be logged.
     */
    def defaultLogLevel: LogLevel

    /** If the default logging would include the stack trace of the failure */
    def defaultVerbose: Boolean

    /** The logging adapter of the restarting stage */
    def log: LoggingAdapter

    /**
     * Log the failure in the same way as the default logging, for failures the callback does not handle itself.
     * Must only be called from within the callback, on the thread that invokes it.
     */
    def logDefault(): Unit
  }

  private[akka] final class RestartFailureImpl(
      override val cause: Throwable,
      override val consecutiveFailures: Int,
      override val defaultLogLevel: LogLevel,
      override val defaultVerbose: Boolean,
      override val log: LoggingAdapter,
      logDefaultFunction: () => Unit)
      extends RestartFailure {

    override def logDefault(): Unit = logDefaultFunction()

    override def toString: String =
      s"RestartFailure(cause=$cause,consecutiveFailures=$consecutiveFailures,defaultLogLevel=$defaultLogLevel)"
  }
}
