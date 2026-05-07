/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.annotation.InternalApi
import akka.stream.scaladsl

/**
 * Allows to manage delay and can be stateful to compute delay for any sequence of elements,
 * all elements go through nextDelay() updating state and returning delay for each element
 */
trait DelayStrategy[T] {

  /**
   * Returns delay for ongoing element, `Duration.Zero` means passing without delay
   */
  def nextDelay(elem: T): java.time.Duration

}

object DelayStrategy {

  /** INTERNAL API */
  @InternalApi
  private[javadsl] def asScala[T](delayStrategy: DelayStrategy[T]) = new scaladsl.DelayStrategy[T] {
    override def nextDelay(elem: T): FiniteDuration = delayStrategy.nextDelay(elem).toScala
  }

  /**
   * Fixed delay strategy, always returns constant delay for any element.
   * @param delay value of the delay
   */
  def fixedDelay[T](delay: java.time.Duration): DelayStrategy[T] = new DelayStrategy[T] {
    override def nextDelay(elem: T): java.time.Duration = delay
  }

  /**
   * Strategy with linear increasing delay.
   * It starts with zero delay for each element,
   * increases by `increaseStep` every time when `needsIncrease` returns `true`,
   * when `needsIncrease` returns `false` it resets to `initialDelay`.
   * @param increaseStep step by which delay is increased
   * @param needsIncrease if `true` delay increases, if `false` delay resets to `initialDelay`
   */
  def linearIncreasingDelay[T](increaseStep: java.time.Duration, needsIncrease: T => Boolean): DelayStrategy[T] =
    linearIncreasingDelay(increaseStep, needsIncrease, java.time.Duration.ZERO)

  /**
   * Strategy with linear increasing delay.
   * It starts with `initialDelay` for each element,
   * increases by `increaseStep` every time when `needsIncrease` returns `true`.
   * when `needsIncrease` returns `false` it resets to `initialDelay`.
   * @param increaseStep step by which delay is increased
   * @param needsIncrease if `true` delay increases, if `false` delay resets to `initialDelay`
   * @param initialDelay initial delay for each of elements
   */
  def linearIncreasingDelay[T](
      increaseStep: java.time.Duration,
      needsIncrease: T => Boolean,
      initialDelay: java.time.Duration): DelayStrategy[T] =
    linearIncreasingDelay(increaseStep, needsIncrease, initialDelay, java.time.Duration.ofNanos(Long.MaxValue))

  /**
   * Strategy with linear increasing delay.
   * It starts with `initialDelay` for each element,
   * increases by `increaseStep` every time when `needsIncrease` returns `true` up to `maxDelay`,
   * when `needsIncrease` returns `false` it resets to `initialDelay`.
   * @param increaseStep step by which delay is increased
   * @param needsIncrease if `true` delay increases, if `false` delay resets to `initialDelay`
   * @param initialDelay initial delay for each of elements
   * @param maxDelay limits maximum delay
   */
  def linearIncreasingDelay[T](
      increaseStep: java.time.Duration,
      needsIncrease: T => Boolean,
      initialDelay: java.time.Duration,
      maxDelay: java.time.Duration): DelayStrategy[T] = {
    require(increaseStep.compareTo(java.time.Duration.ZERO) > 0, "Increase step must be positive")
    require(maxDelay.compareTo(initialDelay) >= 0, "Initial delay may not exceed max delay")

    new DelayStrategy[T] {

      private[this] var delay = initialDelay

      override def nextDelay(elem: T): java.time.Duration = {
        if (needsIncrease(elem)) {
          val next = delay.plus(increaseStep)
          if (next.compareTo(maxDelay) < 0) {
            delay = next
          } else {
            delay = maxDelay
          }
        } else {
          delay = initialDelay
        }
        delay
      }

    }

  }

}
