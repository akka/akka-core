/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

/**
 * Represents a mode that decides how to deal exceed rate for Throttle operator
 */
sealed abstract class ThrottleMode

object ThrottleMode {

  /**
   *  Tells throttle to make pauses before emitting messages to meet throttle rate
   */
  case object Shaping extends ThrottleMode

  /**
   * Makes throttle fail with exception when upstream is faster than throttle rate
   */
  case object Enforcing extends ThrottleMode

  /**
   * Java API: Tells throttle to make pauses before emitting messages to meet throttle rate
   */
  def shaping = Shaping

  /**
   * Java API: Makes throttle fail with exception when upstream is faster than throttle rate
   */
  def enforcing = Enforcing
}

/**
 * Exception that is thrown when rated controlled by stream is exceeded
 */
class RateExceededException(msg: String) extends RuntimeException(msg)
