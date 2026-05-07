/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.stream.ThrottleMode
import akka.stream.impl.Throttle
import akka.util.NanoTimeTokenBucket
import akka.util.TokenBucket

/**
 * Control the throttle rate from the outside of the stream, or share a common throttle rate
 * across several streams.
 */
final class ThrottleControl private[akka] (
    cost: Int,
    per: FiniteDuration,
    maximumBurst: Int,
    val mode: ThrottleMode,
    shared: Boolean) {
  def this(cost: Int, per: FiniteDuration) =
    this(cost, per, Throttle.AutomaticMaximumBurst, ThrottleMode.Shaping, shared = true)

  def this(cost: Int, per: FiniteDuration, maximumBurst: Int, mode: ThrottleMode) =
    this(cost, per, maximumBurst, mode, shared = true)

  private def createTokenBucket(cost: Int, per: FiniteDuration): TokenBucket = {
    require(cost > 0, "cost must be > 0")
    require(per.toNanos > 0, "per time must be > 0")
    require(cost == Int.MaxValue || per.toNanos >= cost, "Rates larger than 1 unit / nanosecond are not supported")

    // There is some loss of precision here because of rounding, but this only happens if nanosBetweenTokens is very
    // small which is usually at rates where that precision is highly unlikely anyway as the overhead of this stage
    // is likely higher than the required accuracy interval.
    val nanosBetweenTokens = per.toNanos / cost
    // 100 ms is a realistic minimum between tokens, otherwise the maximumBurst is adjusted
    // to be able to support higher rates
    val effectiveMaximumBurst: Long = {
      if (cost == Int.MaxValue) Long.MaxValue
      else if (maximumBurst == Throttle.AutomaticMaximumBurst) math.max(1, ((100 * 1000 * 1000) / nanosBetweenTokens))
      else maximumBurst
    }

    require(
      !(mode == ThrottleMode.Enforcing && effectiveMaximumBurst < 0),
      "maximumBurst must be > 0 in Enforcing mode")

    val newTokenBucket = new NanoTimeTokenBucket(effectiveMaximumBurst, nanosBetweenTokens)
    newTokenBucket.init()
    newTokenBucket
  }

  private var tokenBucket = createTokenBucket(cost, per)
  private var _cost = cost
  private var _per = per

  def update(cost: Int, per: FiniteDuration): Unit = synchronized {
    tokenBucket = createTokenBucket(cost, per)
    _cost = cost
    _per = per
  }

  /**
   * Speed is limited to `cost/per`. This is the current cost.
   */
  def getCost(): Int =
    _cost

  /**
   * Speed is limited to `cost/per`. This is the current per duration.
   */
  def getPer(): FiniteDuration =
    _per

  /**
   * INTERNAL API
   *
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
   */
  @InternalApi private[akka] def offer(cost: Long): Long = {
    if (shared) {
      synchronized {
        tokenBucket.offer(cost)
      }
    } else {
      tokenBucket.offer(cost)
    }
  }

  def initIfNotShared(): Unit = {
    if (!shared)
      tokenBucket.init()
  }

}
