/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.concurrent.duration._

import akka.annotation.InternalApi
import akka.pattern.RetrySupport
import akka.stream.{ Attributes, BidiShape, Inlet, Outlet }
import akka.stream.SubscriptionWithCancelException.NonFailureCancellation
import akka.stream.stage._
import akka.util.OptionVal

/**
 * INTERNAL API.
 *
 * ```
 *        externalIn
 *            |
 *            |
 *   +-> internalOut -->+
 *   |                  |
 *   |                 flow
 *   |                  |
 *   |     internalIn --+
 *   +<-yes- retry?
 *            |
 *            no
 *            |
 *       externalOut
 * ```
 */
@InternalApi private[akka] final class RetryFlowCoordinator[In, Out](
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    maxRetries: Int,
    decideRetry: (In, Out) => Option[In])
    extends GraphStage[BidiShape[In, In, Out, Out]] {

  private val externalIn = Inlet[In]("RetryFlow.externalIn")
  private val externalOut = Outlet[Out]("RetryFlow.externalOut")

  private val internalOut = Outlet[In]("RetryFlow.internalOut")
  private val internalIn = Inlet[Out]("RetryFlow.internalIn")

  override val shape: BidiShape[In, In, Out, Out] =
    BidiShape(externalIn, internalOut, internalIn, externalOut)

  override def createLogic(attributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    private var elementInProgress: OptionVal[In] = OptionVal.none
    private var retryNo = 0

    setHandler(
      externalIn,
      new InHandler {
        override def onPush(): Unit = {
          val element = grab(externalIn)
          elementInProgress = OptionVal.Some(element)
          retryNo = 0
          pushInternal(element)
        }

        override def onUpstreamFinish(): Unit =
          if (elementInProgress.isEmpty) {
            completeStage()
          }
      })

    setHandler(
      internalOut,
      new OutHandler {

        override def onPull(): Unit = {
          if (elementInProgress.isEmpty) {
            if (!hasBeenPulled(externalIn) && !isClosed(externalIn)) {
              pull(externalIn)
            }
          }
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          if (elementInProgress.isEmpty || !cause.isInstanceOf[NonFailureCancellation]) {
            super.onDownstreamFinish(cause)
          } else {
            // emit elements before finishing
            setKeepGoing(true)
          }
        }
      })

    setHandler(
      internalIn,
      new InHandler {
        override def onPush(): Unit = {
          val result = grab(internalIn)
          elementInProgress match {
            case OptionVal.Some(_) if retryNo == maxRetries => pushExternal(result)
            case OptionVal.Some(in) =>
              decideRetry(in, result) match {
                case None          => pushExternal(result)
                case Some(element) => planRetry(element)
              }
            case _ =>
              failStage(
                new IllegalStateException(
                  s"inner flow emitted unexpected element $result; the flow must be one-in one-out"))
          }
        }
      })

    setHandler(externalOut, new OutHandler {
      override def onPull(): Unit =
        // external demand
        if (!hasBeenPulled(internalIn)) pull(internalIn)
    })

    private def pushInternal(element: In): Unit = {
      push(internalOut, element)
    }

    private def pushExternal(result: Out): Unit = {
      elementInProgress = OptionVal.none
      push(externalOut, result)
      if (isClosed(externalIn)) {
        completeStage()
      } else if (isAvailable(internalOut)) {
        pull(externalIn)
      }
    }

    private def planRetry(element: In): Unit = {
      val delay = RetrySupport.calculateExponentialBackoffDelay(retryNo, minBackoff, maxBackoff, randomFactor)
      elementInProgress = OptionVal.Some(element)
      retryNo += 1
      pull(internalIn)
      scheduleOnce(RetryFlowCoordinator.RetryCurrentElement, delay)
    }

    override def onTimer(timerKey: Any): Unit = pushInternal(elementInProgress.get)

  }
}

private object RetryFlowCoordinator {
  case object RetryCurrentElement
}
