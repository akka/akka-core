/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import org.slf4j.event.Level

import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

object SupervisorStrategy {

  /**
   * Resume means keeping the same state as before the exception was
   * thrown and is thus less safe than `restart`.
   *
   * If the actor behavior is deferred and throws an exception on startup the actor is stopped
   * (restarting would be dangerous as it could lead to an infinite restart-loop)
   */
  val resume: SupervisorStrategy = Resume(loggingEnabled = true, logLevel = Level.ERROR)

  /**
   * Restart immediately without any limit on number of restart retries. A limit can be
   * added with [[RestartSupervisorStrategy.withLimit]].
   *
   * If the actor behavior is deferred and throws an exception on startup the actor is stopped
   * (restarting would be dangerous as it could lead to an infinite restart-loop)
   */
  val restart: RestartSupervisorStrategy =
    Restart(maxRestarts = -1, withinTimeRange = Duration.Zero)

  /**
   * Stop the actor
   */
  val stop: SupervisorStrategy = Stop(loggingEnabled = true, logLevel = Level.ERROR)

  /**
   * Scala API: It supports exponential back-off between the given `minBackoff` and
   * `maxBackoff` durations. For example, if `minBackoff` is 3 seconds and
   * `maxBackoff` 30 seconds the start attempts will be delayed with
   * 3, 6, 12, 24, 30, 30 seconds. The exponential back-off counter is reset
   * if the actor is not terminated within the `minBackoff` duration.
   *
   * In addition to the calculated exponential back-off an additional
   * random delay based the given `randomFactor` is added, e.g. 0.2 adds up to 20%
   * delay. The reason for adding a random delay is to avoid that all failing
   * actors hit the backend resource at the same time.
   *
   * During the back-off incoming messages are dropped.
   *
   * If no new exception occurs within `(minBackoff + maxBackoff) / 2` the exponentially
   * increased back-off timeout is reset. This can be overridden by explicitly setting
   * `resetBackoffAfter` using `withResetBackoffAfter` on the returned strategy.
   *
   * The strategy is applied also if the actor behavior is deferred and throws an exception during
   * startup.
   *
   * A maximum number of restarts can be specified with [[BackoffSupervisorStrategy#withMaxRestarts]]
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   */
  def restartWithBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): BackoffSupervisorStrategy =
    Backoff(minBackoff, maxBackoff, randomFactor, resetBackoffAfter = (minBackoff + maxBackoff) / 2)

  /**
   * Java API: It supports exponential back-off between the given `minBackoff` and
   * `maxBackoff` durations. For example, if `minBackoff` is 3 seconds and
   * `maxBackoff` 30 seconds the start attempts will be delayed with
   * 3, 6, 12, 24, 30, 30 seconds. The exponential back-off counter is reset
   * if the actor is not terminated within the `minBackoff` duration.
   *
   * In addition to the calculated exponential back-off an additional
   * random delay based the given `randomFactor` is added, e.g. 0.2 adds up to 20%
   * delay. The reason for adding a random delay is to avoid that all failing
   * actors hit the backend resource at the same time.
   *
   * During the back-off incoming messages are dropped.
   *
   * If no new exception occurs within `(minBackoff + maxBackoff) / 2` the exponentially
   * increased back-off timeout is reset. This can be overridden by explicitly setting
   * `resetBackoffAfter` using `withResetBackoffAfter` on the returned strategy.
   *
   * The strategy is applied also if the actor behavior is deferred and throws an exception during
   * startup.
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   */
  def restartWithBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): BackoffSupervisorStrategy =
    restartWithBackoff(minBackoff.toScala, maxBackoff.toScala, randomFactor)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case class Resume(loggingEnabled: Boolean, logLevel: Level) extends SupervisorStrategy {
    override def withLoggingEnabled(enabled: Boolean): SupervisorStrategy =
      copy(loggingEnabled = enabled)
    override def withLogLevel(level: Level): SupervisorStrategy =
      copy(logLevel = level)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case class Stop(loggingEnabled: Boolean, logLevel: Level) extends SupervisorStrategy {
    override def withLoggingEnabled(enabled: Boolean) =
      copy(loggingEnabled = enabled)
    override def withLogLevel(level: Level): SupervisorStrategy =
      copy(logLevel = level)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] sealed trait RestartOrBackoff extends SupervisorStrategy {
    def maxRestarts: Int
    def stopChildren: Boolean
    def stashCapacity: Int
    def loggingEnabled: Boolean

    def unlimitedRestarts(): Boolean = maxRestarts == -1
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final case class Restart(
      maxRestarts: Int,
      withinTimeRange: FiniteDuration,
      loggingEnabled: Boolean = true,
      logLevel: Level = Level.ERROR,
      stopChildren: Boolean = true,
      stashCapacity: Int = -1)
      extends RestartSupervisorStrategy
      with RestartOrBackoff {

    override def withLimit(maxNrOfRetries: Int, withinTimeRange: FiniteDuration): RestartSupervisorStrategy =
      copy(maxNrOfRetries, withinTimeRange)

    override def withLimit(maxNrOfRetries: Int, withinTimeRange: java.time.Duration): RestartSupervisorStrategy =
      copy(maxNrOfRetries, withinTimeRange.toScala)

    override def withStopChildren(enabled: Boolean): RestartSupervisorStrategy =
      copy(stopChildren = enabled)

    override def withStashCapacity(capacity: Int): RestartSupervisorStrategy =
      copy(stashCapacity = capacity)

    override def withLoggingEnabled(enabled: Boolean): RestartSupervisorStrategy =
      copy(loggingEnabled = enabled)

    override def withLogLevel(level: Level): RestartSupervisorStrategy =
      copy(logLevel = level)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final case class Backoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      resetBackoffAfter: FiniteDuration,
      loggingEnabled: Boolean = true,
      logLevel: Level = Level.ERROR,
      criticalLogLevel: Level = Level.ERROR,
      criticalLogLevelAfter: Int = Int.MaxValue,
      maxRestarts: Int = -1,
      stopChildren: Boolean = true,
      stashCapacity: Int = -1)
      extends BackoffSupervisorStrategy
      with RestartOrBackoff {

    override def withResetBackoffAfter(timeout: FiniteDuration): BackoffSupervisorStrategy =
      copy(resetBackoffAfter = timeout)

    override def withResetBackoffAfter(timeout: java.time.Duration): BackoffSupervisorStrategy =
      withResetBackoffAfter(timeout.toScala)

    override def getResetBackoffAfter: java.time.Duration = resetBackoffAfter.toJava

    override def withMaxRestarts(maxRestarts: Int): BackoffSupervisorStrategy =
      copy(maxRestarts = maxRestarts)

    override def withStopChildren(enabled: Boolean): BackoffSupervisorStrategy =
      copy(stopChildren = enabled)

    override def withStashCapacity(capacity: Int): BackoffSupervisorStrategy =
      copy(stashCapacity = capacity)

    override def withLoggingEnabled(enabled: Boolean): BackoffSupervisorStrategy =
      copy(loggingEnabled = enabled)

    override def withLogLevel(level: Level): BackoffSupervisorStrategy =
      copy(logLevel = level)

    override def withCriticalLogLevel(criticalLevel: Level, afterErrors: Int): BackoffSupervisorStrategy =
      copy(criticalLogLevel = criticalLevel, criticalLogLevelAfter = afterErrors)

  }
}

/**
 * Not for user extension
 */
@DoNotInherit
sealed abstract class SupervisorStrategy {
  def loggingEnabled: Boolean
  def logLevel: Level

  def withLoggingEnabled(enabled: Boolean): SupervisorStrategy

  def withLogLevel(level: Level): SupervisorStrategy

}

/**
 * Not for user extension
 */
@DoNotInherit
sealed abstract class RestartSupervisorStrategy extends SupervisorStrategy {

  /**
   * Scala API: Restart with a limit of number of restart retries.
   * The number of restarts are limited to a number of restart attempts (`maxNrOfRetries`)
   * within a time range (`withinTimeRange`). When the time window has elapsed without reaching
   * `maxNrOfRetries` the restart count is reset.
   *
   * The strategy is applied also if the actor behavior is deferred and throws an exception during
   * startup.
   *
   * @param maxNrOfRetries the number of times a child actor is allowed to be restarted,
   *   if the limit is exceeded the child actor is stopped
   * @param withinTimeRange duration of the time window for maxNrOfRetries
   */
  def withLimit(maxNrOfRetries: Int, withinTimeRange: FiniteDuration): RestartSupervisorStrategy

  /**
   * Java API: Restart with a limit of number of restart retries.
   * The number of restarts are limited to a number of restart attempts (`maxNrOfRetries`)
   * within a time range (`withinTimeRange`). When the time window has elapsed without reaching
   * `maxNrOfRetries` the restart count is reset.
   *
   * The strategy is applied also if the actor behavior is deferred and throws an exception during
   * startup.
   *
   * @param maxNrOfRetries the number of times a child actor is allowed to be restarted,
   *   if the limit is exceeded the child actor is stopped
   * @param withinTimeRange duration of the time window for maxNrOfRetries
   */
  def withLimit(maxNrOfRetries: Int, withinTimeRange: java.time.Duration): RestartSupervisorStrategy

  /**
   * Stop or keep child actors when the parent actor is restarted.
   * By default child actors are stopped when parent is restarted.
   * @param enabled if `true` then child actors are stopped, otherwise they are kept
   */
  def withStopChildren(enabled: Boolean): RestartSupervisorStrategy

  /**
   * While restarting (waiting for children to stop) incoming messages and signals are
   * stashed, and delivered later to the newly restarted behavior. This property defines
   * the capacity in number of messages of the stash buffer. If the capacity is exceed
   * then additional incoming messages are dropped.
   *
   * By default the capacity is defined by config property `akka.actor.typed.restart-stash-capacity`.
   */
  def withStashCapacity(capacity: Int): RestartSupervisorStrategy

  override def withLoggingEnabled(enabled: Boolean): RestartSupervisorStrategy

  override def withLogLevel(level: Level): RestartSupervisorStrategy

}

/**
 * Not for user extension
 */
@DoNotInherit
sealed abstract class BackoffSupervisorStrategy extends SupervisorStrategy {
  def resetBackoffAfter: FiniteDuration

  def getResetBackoffAfter: java.time.Duration

  /**
   * Scala API: The back-off algorithm is reset if the actor does not crash within the
   * specified `resetBackoffAfter`. By default, the `resetBackoffAfter` has
   * the value of `(minBackoff + maxBackoff) / 2`.
   */
  def withResetBackoffAfter(timeout: FiniteDuration): BackoffSupervisorStrategy

  /**
   * Java API: The back-off algorithm is reset if the actor does not crash within the
   * specified `resetBackoffAfter`. By default, the `resetBackoffAfter` has
   * the same value as `minBackoff`.
   */
  def withResetBackoffAfter(timeout: java.time.Duration): BackoffSupervisorStrategy

  /**
   * Allow at most this number of failed restarts in a row. Zero or negative disables
   * the upper limit on restarts (and is the default)
   */
  def withMaxRestarts(maxRestarts: Int): BackoffSupervisorStrategy

  /**
   * Stop or keep child actors when the parent actor is restarted.
   * By default child actors are stopped when parent is restarted.
   * @param enabled if `true` then child actors are stopped, otherwise they are kept
   */
  def withStopChildren(enabled: Boolean): BackoffSupervisorStrategy

  /**
   * While restarting (waiting for backoff to expire and children to stop) incoming
   * messages and signals are stashed, and delivered later to the newly restarted
   * behavior. This property defines the capacity in number of messages of the stash
   * buffer. If the capacity is exceed then additional incoming messages are dropped.
   *
   * By default the capacity is defined by config property `akka.actor.typed.restart-stash-capacity`.
   */
  def withStashCapacity(capacity: Int): BackoffSupervisorStrategy

  override def withLoggingEnabled(enabled: Boolean): BackoffSupervisorStrategy

  override def withLogLevel(level: Level): BackoffSupervisorStrategy

  /**
   * Possibility to use another log level after a given number of errors.
   * The initial errors are logged at the level defined with [[BackoffSupervisorStrategy.withLogLevel]].
   * For example, the first 3 errors can be logged at INFO level and thereafter at ERROR level.
   *
   * The counter (and log level) is reset after the [[BackoffSupervisorStrategy.withResetBackoffAfter]]
   * duration.
   */
  def withCriticalLogLevel(criticalLevel: Level, afterErrors: Int): BackoffSupervisorStrategy

}
