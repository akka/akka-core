/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

/**
 * INTERNAL API
 */
private[akka] abstract class TokenBucket(capacity: Long, nanosBetweenTokens: Long) {
  require(capacity >= 0, "Capacity must be non-negative.")
  require(
    capacity == Long.MaxValue || nanosBetweenTokens > 0,
    "Time between tokens must be larger than zero nanoseconds.")

  private[this] var availableTokens: Long = _
  private[this] var lastUpdate: Long = _

  /**
   * This method must be called before the token bucket can be used.
   */
  def init(): Unit = {
    availableTokens = capacity
    lastUpdate = currentTime
  }

  /**
   * The current time in nanos. The returned value is monotonic, might wrap over and has no relationship with wall-clock.
   *
   * @return return the current time in nanos as a Long.
   */
  def currentTime: Long

  /**
   * Call this (side-effecting) method whenever an element should be passed through the token-bucket. This method
   * will return the number of nanoseconds the element needs to be delayed to conform with the token bucket parameters.
   * Returns zero if the element can be emitted immediately. The method does not handle overflow, if an element is to
   * be delayed longer in nanoseconds than what can be represented as a positive Long then an undefined value is returned.
   *
   * If a non-zero value is returned, it is the responsibility of the caller to not call this method before the
   * returned delay has been elapsed (but can be called later). This class does not check or protect against early
   * calls.
   *
   * @param cost How many tokens the element costs. Can be larger than the capacity of the bucket.
   * @return
   */
  def offer(cost: Long): Long = {
    if (cost < 0) throw new IllegalArgumentException("Cost must be non-negative")

    if (capacity == Long.MaxValue) {
      0
    } else {
      val now = currentTime
      val timeElapsed = now - lastUpdate

      val tokensArrived =
        // Was there even a tick since last time?
        if (timeElapsed >= nanosBetweenTokens) {
          // only one tick elapsed
          if (timeElapsed < nanosBetweenTokens * 2) {
            lastUpdate += nanosBetweenTokens
            1
          } else {
            // Ok, no choice, do the slow integer division
            val tokensArrived = timeElapsed / nanosBetweenTokens
            lastUpdate += tokensArrived * nanosBetweenTokens
            tokensArrived
          }
        } else 0

      availableTokens = math.min(availableTokens + tokensArrived, capacity)

      if (cost <= availableTokens) {
        availableTokens -= cost
        0
      } else {
        val remainingCost = cost - availableTokens
        // Tokens always arrive at exact multiples of the token generation period, we must account for that
        val timeSinceTokenArrival = now - lastUpdate
        val delay = remainingCost * nanosBetweenTokens - timeSinceTokenArrival
        availableTokens = 0
        lastUpdate = now + delay
        delay
      }
    }
  }

}

/**
 * Default implementation of [[TokenBucket]] that uses `System.nanoTime` as the time source.
 */
final class NanoTimeTokenBucket(_cap: Long, _period: Long) extends TokenBucket(_cap, _period) {
  override def currentTime: Long = System.nanoTime()
}
