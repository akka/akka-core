/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package akka.pattern

import java.util.Optional
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import akka.actor.{ ActorRef, DeadLetterSuppression, Props }
import akka.annotation.InternalApi

object BackoffSupervisor {

  /**
   * Props for creating a `BackoffSupervisor` actor from [[BackoffOnStopOptions]].
   *
   * @param options the [[BackoffOnStopOptions]] that specify how to construct a backoff-supervisor.
   */
  def props(options: BackoffOnStopOptions): Props = options.props

  /**
   * Props for creating a `BackoffSupervisor` actor from [[BackoffOnFailureOptions]].
   *
   * @param options the [[BackoffOnFailureOptions]] that specify how to construct a backoff-supervisor.
   */
  def props(options: BackoffOnFailureOptions): Props = options.props

  /**
   * Send this message to the `BackoffSupervisor` and it will reply with
   * [[BackoffSupervisor.CurrentChild]] containing the `ActorRef` of the current child, if any.
   */
  case object GetCurrentChild

  /**
   * Java API: Send this message to the `BackoffSupervisor` and it will reply with
   * [[BackoffSupervisor.CurrentChild]] containing the `ActorRef` of the current child, if any.
   */
  def getCurrentChild = GetCurrentChild

  /**
   * Send this message to the `BackoffSupervisor` and it will reply with
   * [[BackoffSupervisor.CurrentChild]] containing the `ActorRef` of the current child, if any.
   */
  final case class CurrentChild(ref: Option[ActorRef]) {

    /**
     * Java API: The `ActorRef` of the current child, if any
     */
    def getRef: Optional[ActorRef] = Optional.ofNullable(ref.orNull)
  }

  /**
   * Send this message to the `BackoffSupervisor` and it will reset the back-off.
   * This should be used in conjunction with `withManualReset` in [[BackoffOptions]].
   */
  case object Reset

  /**
   * Java API: Send this message to the `BackoffSupervisor` and it will reset the back-off.
   * This should be used in conjunction with `withManualReset` in [[BackoffOptions]].
   */
  def reset = Reset

  /**
   * Send this message to the `BackoffSupervisor` and it will reply with
   * [[BackoffSupervisor.RestartCount]] containing the current restart count.
   */
  case object GetRestartCount

  /**
   * Java API: Send this message to the `BackoffSupervisor` and it will reply with
   * [[BackoffSupervisor.RestartCount]] containing the current restart count.
   */
  def getRestartCount = GetRestartCount

  final case class RestartCount(count: Int)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] case object StartChild extends DeadLetterSuppression

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] case class ResetRestartCount(current: Int) extends DeadLetterSuppression

  /**
   * INTERNAL API
   *
   * Calculates an exponential back off delay.
   */
  private[akka] def calculateDelay(
      restartCount: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): FiniteDuration = {
    val rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
    val calculatedDuration = Try(maxBackoff.min(minBackoff * math.pow(2, restartCount)) * rnd).getOrElse(maxBackoff)
    calculatedDuration match {
      case f: FiniteDuration => f
      case _                 => maxBackoff
    }
  }
}
