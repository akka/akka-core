/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import akka.annotation.InternalApi
import akka.pattern.RetrySettings.ExponentialBackoffFunction
import akka.pattern.RetrySettings.FixedDelayFunction
import akka.pattern.RetrySupport.calculateExponentialBackoffDelay
import com.typesafe.config.Config

import java.time.Duration
import java.util.Optional
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

/**
 * Settings for retrying operations.
 * @param maxRetries maximum number of retries
 * @param delayFunction function to calculate the delay between retries
 * @param shouldRetry function to determine if a failure should be retried
 */
final class RetrySettings private (
    val maxRetries: Int,
    val delayFunction: Int => Option[FiniteDuration],
    val shouldRetry: Throwable => Boolean = _ => true) {

  override def toString: String = {
    s"RetrySettings(maxRetries=$maxRetries, delayFunction=$delayFunction)"
  }

  /**
   * Scala API: Set exponential backoff delay between retries.
   *
   * @param minBackoff minimum backoff duration
   * @param maxBackoff maximum backoff duration
   * @param randomFactor random factor to add jitter to the backoff
   */
  def withExponentialBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): RetrySettings =
    new RetrySettings(maxRetries, new ExponentialBackoffFunction(minBackoff, maxBackoff, randomFactor))

  /**
   * Java API: Set exponential backoff delay between retries.
   *
   * @param minBackoff minimum backoff duration
   * @param maxBackoff maximum backoff duration
   * @param randomFactor random factor to add jitter to the backoff
   * @return Updated settings
   */
  def withExponentialBackoff(minBackoff: Duration, maxBackoff: Duration, randomFactor: Double): RetrySettings =
    withExponentialBackoff(minBackoff.toScala, maxBackoff.toScala, randomFactor)

  /**
   * Scala API: Set fixed delay between retries.
   * @param fixedDelay fixed delay between retries
   * @return Updated settings
   */
  def withFixedDelay(fixedDelay: FiniteDuration): RetrySettings =
    new RetrySettings(maxRetries, new FixedDelayFunction(fixedDelay))

  /**
   * Java API: Set fixed delay between retries.
   * @param fixedDelay fixed delay between retries
   * @return Updated settings
   */
  def withFixedDelay(fixedDelay: Duration): RetrySettings =
    withFixedDelay(fixedDelay.toScala)

  /**
   * Scala API: Set custom delay function between retries.
   * @param delayFunction function to calculate the delay between retries
   * @return Updated settings
   */
  def withDelayFunction(delayFunction: Int => Option[FiniteDuration]): RetrySettings =
    new RetrySettings(maxRetries, delayFunction)

  /**
   * Java API: Set custom delay function between retries.
   * @param delayFunction function to calculate the delay between retries
   * @return Updated settings
   */
  def withJavaDelayFunction(
      delayFunction: java.util.function.IntFunction[Optional[java.time.Duration]]): RetrySettings = {
    val scalaFunc: Int => Option[FiniteDuration] = attempt => {
      val javaDuration = delayFunction.apply(attempt)
      if (javaDuration.isPresent)
        Some(javaDuration.get.toScala)
      else
        None
    }
    withDelayFunction(scalaFunc)
  }

  /**
   * Scala API: Set the function to determine if a failure should be retried.
   * @param shouldRetry function to determine if a failure should be retried
   * @return Updated settings
   */
  def withDecider(shouldRetry: Throwable => Boolean): RetrySettings =
    new RetrySettings(maxRetries, delayFunction, shouldRetry)

  /**
   * Java API: Set the function to determine if a failure should be retried.
   * @param shouldRetry function to determine if a failure should be retried
   * @return Updated settings
   */
  def withJavaDecider(shouldRetry: java.util.function.Function[Throwable, java.lang.Boolean]): RetrySettings =
    withDecider(shouldRetry.apply)
}

object RetrySettings {

  /**
   * INTERNAL API.
   */
  @InternalApi
  private[akka] final class ExponentialBackoffFunction(
      val minBackoff: FiniteDuration,
      val maxBackoff: FiniteDuration,
      val randomFactor: Double)
      extends Function1[Int, Option[FiniteDuration]] {

    override def apply(attempt: Int): Option[FiniteDuration] = {
      Some(calculateExponentialBackoffDelay(attempt, minBackoff, maxBackoff, randomFactor))
    }

    override def toString: String = {
      s"Exponential(minBackoff=$minBackoff, maxBackoff=$maxBackoff, randomFactor=$randomFactor)"
    }
  }

  /**
   * INTERNAL API.
   */
  @InternalApi
  private[akka] final class FixedDelayFunction(val delay: FiniteDuration)
      extends Function1[Int, Option[FiniteDuration]] {

    override def apply(attempt: Int): Option[FiniteDuration] = {
      Some(delay)
    }

    override def toString: String = {
      s"Fixed($delay)"
    }
  }

  /**
   * Scala API: Create settings with exponential backoff delay between retries.
   * The exponential backoff settings are calculated based on number of retries.
   * @param maxRetries maximum number of retries
   * @return RetrySettings with exponential backoff delay
   */
  def apply(maxRetries: Int): RetrySettings = {
    // Start with a reasonable minimum backoff (e.g., 100 milliseconds)
    val minBackoff: FiniteDuration = 100.millis

    // Max backoff increases with maxRetries, capped to a sensible upper limit (e.g., 1 minute)
    val maxBackoff: FiniteDuration = if (maxRetries > 10) { //to avoid multiplication overflow
      1.minute
    } else {
      val base = minBackoff * math.pow(2, maxRetries).toLong
      base.min(1.minute)
    }

    // Random factor can scale slightly with maxRetries to add more jitter
    val randomFactor: Double = (0.1 + (maxRetries * 0.05)).min(1.0) // cap at 1.0

    new RetrySettings(maxRetries, new ExponentialBackoffFunction(minBackoff, maxBackoff, randomFactor))
  }

  /**
   * Scala API: Create settings with exponential backoff delay between retries.
   * The exponential backoff settings are calculated based on number of retries.
   * @param maxRetries maximum number of retries
   * @return RetrySettings with exponential backoff delay
   */
  def create(maxRetries: Int): RetrySettings = {
    apply(maxRetries)
  }

  /**
   * Scala API: Create settings from configuration.
   */
  def apply(config: Config): RetrySettings = {
    val maxRetries = config.getInt("max-retries")
    if (config.hasPath("fixed-delay")) {
      val fixedDelay = config.getDuration("fixed-delay").toScala
      RetrySettings(maxRetries).withFixedDelay(fixedDelay)
    } else if (config.hasPath("min-backoff")) {
      val minBackoff = config.getDuration("min-backoff").toScala
      val maxBackoff = config.getDuration("max-backoff").toScala
      val randomFactor = config.getDouble("random-factor")
      RetrySettings(maxRetries).withExponentialBackoff(minBackoff, maxBackoff, randomFactor)
    } else
      RetrySettings(maxRetries)
  }

  /**
   * Java API: Create settings from configuration.
   */
  def create(config: Config): RetrySettings = {
    apply(config)
  }

}
