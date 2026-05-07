/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._

/**
 * INTERNAL API
 *
 * Various operators for controlling timeouts on IO related streams (although not necessarily).
 *
 * The common theme among the processing operators here that
 *  - they wait for certain event or events to happen
 *  - they have a timer that may fire before these events
 *  - if the timer fires before the event happens, these operators all fail the stream
 *  - otherwise, these streams do not interfere with the element flow, ordinary completion or failure
 */
@InternalApi private[akka] object Timers {

  /**
   * Given a timeout computes how often the check should be run without causing
   * excessive load or losing timeout precision.
   */
  private[akka] def timeoutCheckInterval(timeout: FiniteDuration): FiniteDuration = {
    import scala.concurrent.duration._
    if (timeout > 1.second) 1.second
    else {
      FiniteDuration(
        math.min(math.max(timeout.toNanos / 8, 100.millis.toNanos), timeout.toNanos / 2),
        TimeUnit.NANOSECONDS)
    }
  }

  final class Initial[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def initialAttributes = DefaultAttributes.initial

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private var initialHasPassed = false

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          initialHasPassed = true
          push(out, grab(in))
        }

        override def onPull(): Unit = pull(in)

        final override protected def onTimer(key: Any): Unit =
          if (!initialHasPassed)
            failStage(
              new InitialTimeoutException(s"The first element has not yet passed through in ${timeout.toCoarsest}."))

        override def preStart(): Unit = scheduleOnce(GraphStageLogicTimer, timeout)
      }

    override def toString = "InitialTimeout"

  }

  final class Completion[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def initialAttributes = DefaultAttributes.completion

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        setHandlers(in, out, this)

        override def onPush(): Unit = push(out, grab(in))

        override def onPull(): Unit = pull(in)

        final override protected def onTimer(key: Any): Unit =
          failStage(new CompletionTimeoutException(s"The stream has not been completed in ${timeout.toCoarsest}."))

        override def preStart(): Unit = scheduleOnce(GraphStageLogicTimer, timeout)
      }

    override def toString = "CompletionTimeout"

  }

  final class Idle[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def initialAttributes = DefaultAttributes.idle

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private var nextDeadline: Long = System.nanoTime + timeout.toNanos

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          nextDeadline = System.nanoTime + timeout.toNanos
          push(out, grab(in))
        }

        override def onPull(): Unit = pull(in)

        final override protected def onTimer(key: Any): Unit =
          if (nextDeadline - System.nanoTime < 0)
            failStage(new StreamIdleTimeoutException(s"No elements passed in the last ${timeout.toCoarsest}."))

        override def preStart(): Unit =
          scheduleWithFixedDelay(GraphStageLogicTimer, timeoutCheckInterval(timeout), timeoutCheckInterval(timeout))
      }

    override def toString = "IdleTimeout"

  }

  final class BackpressureTimeout[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def initialAttributes = DefaultAttributes.backpressureTimeout

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private var nextDeadline: Long = System.nanoTime + timeout.toNanos
        private var waitingDemand: Boolean = true

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          push(out, grab(in))
          nextDeadline = System.nanoTime + timeout.toNanos
          waitingDemand = true
        }

        override def onPull(): Unit = {
          waitingDemand = false
          pull(in)
        }

        final override protected def onTimer(key: Any): Unit =
          if (waitingDemand && (nextDeadline - System.nanoTime < 0))
            failStage(new BackpressureTimeoutException(s"No demand signalled in the last ${timeout.toCoarsest}."))

        override def preStart(): Unit =
          scheduleWithFixedDelay(GraphStageLogicTimer, timeoutCheckInterval(timeout), timeoutCheckInterval(timeout))
      }

    override def toString = "BackpressureTimeout"

  }

  final class IdleTimeoutBidi[I, O](val timeout: FiniteDuration) extends GraphStage[BidiShape[I, I, O, O]] {
    val in1 = Inlet[I]("in1")
    val in2 = Inlet[O]("in2")
    val out1 = Outlet[I]("out1")
    val out2 = Outlet[O]("out2")
    val shape = BidiShape(in1, out1, in2, out2)

    override def initialAttributes = DefaultAttributes.idleTimeoutBidi

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
      private var nextDeadline: Long = System.nanoTime + timeout.toNanos

      setHandlers(in1, out1, new IdleBidiHandler(in1, out1))
      setHandlers(in2, out2, new IdleBidiHandler(in2, out2))

      private def onActivity(): Unit = nextDeadline = System.nanoTime + timeout.toNanos

      final override def onTimer(key: Any): Unit =
        if (nextDeadline - System.nanoTime < 0)
          failStage(new StreamIdleTimeoutException(s"No elements passed in the last ${timeout.toCoarsest}."))

      override def preStart(): Unit =
        scheduleWithFixedDelay(GraphStageLogicTimer, timeoutCheckInterval(timeout), timeoutCheckInterval(timeout))

      class IdleBidiHandler[P](in: Inlet[P], out: Outlet[P]) extends InHandler with OutHandler {
        override def onPush(): Unit = {
          onActivity()
          push(out, grab(in))
        }

        override def onPull(): Unit = pull(in)
        override def onUpstreamFinish(): Unit = complete(out)
        override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
      }
    }

    override def toString = "IdleTimeoutBidi"

  }

  final class DelayInitial[T](val delay: FiniteDuration) extends SimpleLinearGraphStage[T] {
    override def initialAttributes = DefaultAttributes.delayInitial

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {
        private var open: Boolean = false

        setHandlers(in, out, this)

        override def preStart(): Unit = {
          if (delay == Duration.Zero) open = true
          else scheduleOnce(GraphStageLogicTimer, delay)
        }

        override def onPush(): Unit = push(out, grab(in))

        override def onPull(): Unit = if (open) pull(in)

        override protected def onTimer(timerKey: Any): Unit = {
          open = true
          if (isAvailable(out)) pull(in)
        }
      }

    override def toString = "DelayTimer"

  }

  final class IdleInject[I, O >: I](val timeout: FiniteDuration, val inject: () => O)
      extends GraphStage[FlowShape[I, O]] {
    val in: Inlet[I] = Inlet("IdleInject.in")
    val out: Outlet[O] = Outlet("IdleInject.out")

    override def initialAttributes = DefaultAttributes.idleInject

    override val shape: FlowShape[I, O] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with StageLogging with InHandler with OutHandler {
        private var nextDeadline: Long = System.nanoTime + timeout.toNanos
        private val contextPropagation = ContextPropagation()

        setHandlers(in, out, this)

        // Prefetching to ensure priority of actual upstream elements
        override def preStart(): Unit = pull(in)

        override def onPush(): Unit = {
          nextDeadline = System.nanoTime + timeout.toNanos
          cancelTimer(GraphStageLogicTimer)
          if (isAvailable(out)) {
            push(out, grab(in))
            pull(in)
          } else {
            contextPropagation.suspendContext()
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (!isAvailable(in)) completeStage()
        }

        override def onPull(): Unit = {
          if (isAvailable(in)) {
            contextPropagation.resumeContext()
            push(out, grab(in))
            if (isClosed(in)) completeStage()
            else pull(in)
          } else emitInjectedElementOrReschedule(onTimer = false)
        }

        private def emitInjectedElementOrReschedule(onTimer: Boolean): Unit = {
          val now = System.nanoTime()
          val diff = now - nextDeadline
          if (diff < 0) {
            if (onTimer) {
              // Clock may be non-monotonic, see https://stackoverflow.com/questions/51344787/in-what-cases-clock-monotonic-might-not-be-available
              log.warning(
                s"Timer should have triggered only after deadline but now is $now and deadline was $nextDeadline diff $diff (JVM System.nanoTime non-monotonic). Rescheduling, timer may not be triggered at the expected time.")
            }
            scheduleOnce(GraphStageLogicTimer, FiniteDuration(-diff, TimeUnit.NANOSECONDS))
          } else {
            push(out, inject())
            nextDeadline = now + timeout.toNanos
          }
        }

        override protected def onTimer(timerKey: Any): Unit = emitInjectedElementOrReschedule(onTimer = true)
      }

    override def toString = "IdleInject"
  }

  case object GraphStageLogicTimer
}
