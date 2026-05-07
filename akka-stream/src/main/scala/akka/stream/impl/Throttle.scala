/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.concurrent.duration._

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.ThrottleMode.Enforcing
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.ThrottleControl
import akka.stream.stage._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Throttle {
  final val AutomaticMaximumBurst = -1
  private case object TimerKey
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class Throttle[T](
    val controlFactory: () => ThrottleControl,
    val costCalculation: (T) => Int,
    val mode: ThrottleMode)
    extends SimpleLinearGraphStage[T] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private val control = controlFactory()
      private var currentElement: T = _

      override def preStart(): Unit = control.initIfNotShared()

      override def onUpstreamFinish(): Unit =
        if (!(isAvailable(out) && isTimerActive(Throttle.TimerKey))) {
          completeStage()
        }

      override def onPush(): Unit = {
        val elem = grab(in)
        val cost = costCalculation(elem)
        val delayNanos = control.offer(cost)

        if (delayNanos == 0L) push(out, elem)
        else {
          if (mode eq Enforcing) failStage(new RateExceededException("Maximum throttle throughput exceeded."))
          else {
            currentElement = elem
            scheduleOnce(Throttle.TimerKey, delayNanos.nanos)
          }
        }
      }

      override def onPull(): Unit = pull(in)

      override protected def onTimer(key: Any): Unit = {
        push(out, currentElement)
        currentElement = null.asInstanceOf[T]
        if (isClosed(in)) completeStage()
      }

      setHandlers(in, out, this)
    }

  override def toString = "Throttle"
}
