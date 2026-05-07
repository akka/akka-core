/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern.internal

import scala.concurrent.duration._

import akka.actor.{ OneForOneStrategy, _ }
import akka.actor.SupervisorStrategy._
import akka.annotation.InternalApi
import akka.pattern.RetrySupport
import akka.pattern.{
  BackoffReset,
  BackoffSupervisor,
  ForwardDeathLetters,
  ForwardTo,
  HandleBackoff,
  HandlingWhileStopped,
  ReplyWith
}

/**
 * INTERNAL API
 *
 * Back-off supervisor that stops and starts a child actor when the child actor restarts.
 * This back-off supervisor is created by using ``akka.pattern.BackoffSupervisor.props``
 * with ``akka.pattern.BackoffOpts.onFailure``.
 */
@InternalApi private[pattern] class BackoffOnRestartSupervisor(
    val childProps: Props,
    val childName: String,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    val reset: BackoffReset,
    randomFactor: Double,
    strategy: OneForOneStrategy,
    handlingWhileStopped: HandlingWhileStopped)
    extends Actor
    with HandleBackoff
    with ActorLogging {

  import BackoffSupervisor._
  import context._

  override val supervisorStrategy: OneForOneStrategy = {
    val decider = super.supervisorStrategy.decider
    OneForOneStrategy(strategy.maxNrOfRetries, strategy.withinTimeRange, strategy.loggingEnabled) {
      case ex =>
        val defaultDirective: Directive =
          decider.applyOrElse(ex, (_: Any) => Escalate)

        strategy.decider.applyOrElse(ex, (_: Any) => defaultDirective) match {
          // Whatever the final Directive is, we will translate all Restarts
          // to our own Restarts, which involves stopping the child.
          case Restart =>
            val nextRestartCount = restartCount + 1

            if (strategy.withinTimeRange.isFinite && restartCount == 0) {
              // If the user has defined a time range for the maxNrOfRetries, we'll schedule a message
              // to ourselves every time that range elapses, to reset the restart counter. We hide it
              // behind this conditional to avoid queuing the message unnecessarily
              val finiteWithinTimeRange = strategy.withinTimeRange.asInstanceOf[FiniteDuration]
              system.scheduler.scheduleOnce(finiteWithinTimeRange, self, ResetRestartCount(nextRestartCount))
            }
            val childRef = sender()
            if (strategy.maxNrOfRetries >= 0 && nextRestartCount > strategy.maxNrOfRetries) {
              // If we've exceeded the maximum # of retries allowed by the Strategy, die.
              log.debug(
                s"Terminating on restart #{} which exceeds max allowed restarts ({})",
                nextRestartCount,
                strategy.maxNrOfRetries)
              become(receive)
              stop(self)
            } else {
              become(waitChildTerminatedBeforeBackoff(childRef).orElse(handleBackoff))
            }
            Stop

          case other => other
        }
    }
  }

  def waitChildTerminatedBeforeBackoff(childRef: ActorRef): Receive = {
    case Terminated(`childRef`) =>
      become(receive)
      child = None
      val restartDelay =
        RetrySupport.calculateExponentialBackoffDelay(restartCount, minBackoff, maxBackoff, randomFactor)
      context.system.scheduler.scheduleOnce(restartDelay, self, BackoffSupervisor.StartChild)
      restartCount += 1

    case StartChild => // Ignore it, we will schedule a new one once current child terminated.
  }

  def onTerminated: Receive = {
    case Terminated(c) =>
      log.debug(s"Terminating, because child [$c] terminated itself")
      stop(self)
  }

  def receive: Receive = onTerminated.orElse(handleBackoff)

  protected def handleMessageToChild(msg: Any): Unit = child match {
    case Some(c) =>
      c.forward(msg)
    case None =>
      handlingWhileStopped match {
        case ForwardDeathLetters => context.system.deadLetters.forward(msg)
        case ForwardTo(h)        => h.forward(msg)
        case ReplyWith(r)        => sender() ! r
      }
  }
}
