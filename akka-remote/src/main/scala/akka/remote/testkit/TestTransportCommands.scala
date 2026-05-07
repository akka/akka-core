/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testkit

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.math.min

import akka.actor.Address
import akka.actor.NoSerializationVerificationNeeded

sealed trait Direction {
  def includes(other: Direction): Boolean
}

object Direction {

  /**
   * Java API: get the Direction.Send instance
   */
  def sendDirection(): Direction = Direction.Send

  /**
   * Java API: get the Direction.Receive instance
   */
  def receiveDirection(): Direction = Direction.Receive

  /**
   * Java API: get the Direction.Both instance
   */
  def bothDirection(): Direction = Direction.Both

  @SerialVersionUID(1L)
  case object Send extends Direction {
    override def includes(other: Direction): Boolean = other match {
      case Send => true
      case _    => false
    }

    /**
     * Java API: get the singleton instance
     */
    def getInstance: Direction = this
  }

  @SerialVersionUID(1L)
  case object Receive extends Direction {
    override def includes(other: Direction): Boolean = other match {
      case Receive => true
      case _       => false
    }

    /**
     * Java API: get the singleton instance
     */
    def getInstance: Direction = this
  }

  @SerialVersionUID(1L)
  case object Both extends Direction {
    override def includes(other: Direction): Boolean = true

    /**
     * Java API: get the singleton instance
     */
    def getInstance: Direction = this
  }
}

@SerialVersionUID(1L)
final case class SetThrottle(address: Address, direction: Direction, mode: ThrottleMode)

@SerialVersionUID(1L)
case object SetThrottleAck {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

object ThrottleMode {

  /**
   * Java API: get the Blackhole instance
   */
  def blackholeThrottleMode(): ThrottleMode = Blackhole

  /**
   * Java API: get the Unthrottled instance
   */
  def unthrottledThrottleMode(): ThrottleMode = Unthrottled
}

sealed trait ThrottleMode extends NoSerializationVerificationNeeded {
  def tryConsumeTokens(nanoTimeOfSend: Long, tokens: Int): (ThrottleMode, Boolean)
  def timeToAvailable(currentNanoTime: Long, tokens: Int): FiniteDuration
}

@SerialVersionUID(1L)
final case class TokenBucket(capacity: Int, tokensPerSecond: Double, nanoTimeOfLastSend: Long, availableTokens: Int)
    extends ThrottleMode {

  private def isAvailable(nanoTimeOfSend: Long, tokens: Int): Boolean =
    if ((tokens > capacity && availableTokens > 0)) {
      true // Allow messages larger than capacity through, it will be recorded as negative tokens
    } else min((availableTokens + tokensGenerated(nanoTimeOfSend)), capacity) >= tokens

  override def tryConsumeTokens(nanoTimeOfSend: Long, tokens: Int): (ThrottleMode, Boolean) = {
    if (isAvailable(nanoTimeOfSend, tokens))
      (
        this.copy(
          nanoTimeOfLastSend = nanoTimeOfSend,
          availableTokens = min(availableTokens - tokens + tokensGenerated(nanoTimeOfSend), capacity)),
        true)
    else (this, false)
  }

  override def timeToAvailable(currentNanoTime: Long, tokens: Int): FiniteDuration = {
    val needed = (if (tokens > capacity) 1 else tokens) - tokensGenerated(currentNanoTime)
    (needed / tokensPerSecond).seconds
  }

  private def tokensGenerated(nanoTimeOfSend: Long): Int =
    (TimeUnit.NANOSECONDS.toMillis(nanoTimeOfSend - nanoTimeOfLastSend) * tokensPerSecond / 1000.0).toInt
}

@SerialVersionUID(1L)
case object Unthrottled extends ThrottleMode {
  override def tryConsumeTokens(nanoTimeOfSend: Long, tokens: Int): (ThrottleMode, Boolean) = (this, true)
  override def timeToAvailable(currentNanoTime: Long, tokens: Int): FiniteDuration = Duration.Zero

  /**
   * Java API: get the singleton instance
   */
  def getInstance: ThrottleMode = this
}

@SerialVersionUID(1L)
case object Blackhole extends ThrottleMode {
  override def tryConsumeTokens(nanoTimeOfSend: Long, tokens: Int): (ThrottleMode, Boolean) = (this, false)
  override def timeToAvailable(currentNanoTime: Long, tokens: Int): FiniteDuration = Duration.Zero

  /**
   * Java API: get the singleton instance
   */
  def getInstance: ThrottleMode = this
}

/**
 * Management Command to force disassociation of an address.
 */
@SerialVersionUID(1L)
final case class ForceDisassociate(address: Address)

@SerialVersionUID(1L)
case object ForceDisassociateAck {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}
