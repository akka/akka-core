/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
 * Allows to manage delay. Can be stateful to compute delay for any sequence
 * of elements, as instances are not shared among running streams and all
 * elements go through nextDelay(), updating state and returning delay for that
 * element.
 */
trait DelayStrategy[-T] {

  /**
   * Returns delay for ongoing element, `Duration.Zero` means passing without delay
   */
  def nextDelay(elem: T): FiniteDuration

}

object DelayStrategy {

  /**
   * Fixed delay strategy, always returns constant delay for any element.
   * @param delay value of the delay
   */
  def fixedDelay(delay: FiniteDuration): DelayStrategy[Any] = new DelayStrategy[Any] {
    override def nextDelay(elem: Any): FiniteDuration = delay
  }

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
      increaseStep: FiniteDuration,
      needsIncrease: T => Boolean,
      initialDelay: FiniteDuration = Duration.Zero,
      maxDelay: Duration = Duration.Inf): DelayStrategy[T] = {
    require(increaseStep > Duration.Zero, "Increase step must be positive")
    require(maxDelay > initialDelay, "Max delay must be bigger than initial delay")

    new DelayStrategy[T] {

      private[this] var delay: FiniteDuration = initialDelay

      override def nextDelay(elem: T): FiniteDuration = {
        if (needsIncrease(elem)) {
          // minimum of a finite and an infinite duration is finite
          delay = Seq(delay + increaseStep, maxDelay).min.asInstanceOf[FiniteDuration]
        } else {
          delay = initialDelay
        }
        delay
      }

    }

  }

}
