/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.event.Logging
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimerSchedulerImpl {
  sealed trait TimerMsg {
    def key: Any
    def generation: Int
    def owner: TimerSchedulerImpl
  }

  final case class Timer(key: Any, msg: Any, repeat: Boolean, generation: Int, task: Cancellable)
  final case class InfluenceReceiveTimeoutTimerMsg(key: Any, generation: Int, owner: TimerSchedulerImpl)
      extends TimerMsg
      with NoSerializationVerificationNeeded
  final case class NotInfluenceReceiveTimeoutTimerMsg(key: Any, generation: Int, owner: TimerSchedulerImpl)
      extends TimerMsg
      with NoSerializationVerificationNeeded
      with NotInfluenceReceiveTimeout

  private sealed trait TimerMode {
    def repeat: Boolean
  }
  private case class FixedRateMode(initialDelay: FiniteDuration) extends TimerMode {
    override def repeat: Boolean = true
  }
  private case class FixedDelayMode(initialDelay: FiniteDuration) extends TimerMode {
    override def repeat: Boolean = true
  }
  private case object SingleMode extends TimerMode {
    override def repeat: Boolean = false
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TimerSchedulerImpl(ctx: ActorContext) extends TimerScheduler {
  import TimerSchedulerImpl._

  private val log = Logging(ctx.system, classOf[TimerScheduler])
  private var timers: Map[Any, Timer] = Map.empty
  private var timerGen = 0
  private def nextTimerGen(): Int = {
    timerGen += 1
    timerGen
  }

  override def startTimerAtFixedRate(key: Any, msg: Any, interval: FiniteDuration): Unit =
    startTimer(key, msg, interval, FixedRateMode(interval))

  override def startTimerAtFixedRate(key: Any, msg: Any, initialDelay: FiniteDuration, interval: FiniteDuration): Unit =
    startTimer(key, msg, interval, FixedRateMode(initialDelay))

  override def startTimerWithFixedDelay(key: Any, msg: Any, delay: FiniteDuration): Unit =
    startTimer(key, msg, delay, FixedDelayMode(delay))

  override def startTimerWithFixedDelay(key: Any, msg: Any, initialDelay: FiniteDuration, delay: FiniteDuration): Unit =
    startTimer(key, msg, delay, FixedDelayMode(initialDelay))

  override def startPeriodicTimer(key: Any, msg: Any, interval: FiniteDuration): Unit =
    startTimerAtFixedRate(key, msg, interval)

  override def startSingleTimer(key: Any, msg: Any, timeout: FiniteDuration): Unit =
    startTimer(key, msg, timeout, SingleMode)

  private def startTimer(key: Any, msg: Any, timeout: FiniteDuration, mode: TimerMode): Unit = {
    timers.get(key) match {
      case Some(t) => cancelTimer(t)
      case None    =>
    }
    val nextGen = nextTimerGen()

    val timerMsg =
      if (msg.isInstanceOf[NotInfluenceReceiveTimeout])
        NotInfluenceReceiveTimeoutTimerMsg(key, nextGen, this)
      else
        InfluenceReceiveTimeoutTimerMsg(key, nextGen, this)

    val task = mode match {
      case SingleMode =>
        ctx.system.scheduler.scheduleOnce(timeout, ctx.self, timerMsg)(ctx.dispatcher)
      case m: FixedDelayMode =>
        ctx.system.scheduler.scheduleWithFixedDelay(m.initialDelay, timeout, ctx.self, timerMsg)(ctx.dispatcher)
      case m: FixedRateMode =>
        ctx.system.scheduler.scheduleAtFixedRate(m.initialDelay, timeout, ctx.self, timerMsg)(ctx.dispatcher)
    }

    val nextTimer = Timer(key, msg, mode.repeat, nextGen, task)
    timers = timers.updated(key, nextTimer)
  }

  override def isTimerActive(key: Any): Boolean =
    timers.contains(key)

  override def cancel(key: Any): Unit = {
    timers.get(key) match {
      case None    => // already removed/canceled
      case Some(t) => cancelTimer(t)
    }
  }

  private def cancelTimer(timer: Timer): Unit = {
    timer.task.cancel()
    timers -= timer.key
  }

  override def cancelAll(): Unit = {
    timers.valuesIterator.foreach { timer =>
      timer.task.cancel()
    }
    timers = Map.empty
  }

  def interceptTimerMsg(timerMsg: TimerMsg): OptionVal[AnyRef] = {
    timers.get(timerMsg.key) match {
      case None =>
        // it was from canceled timer that was already enqueued in mailbox
        log.debug("Received timer [{}] that has been removed, discarding", timerMsg.key)
        OptionVal.None // message should be ignored
      case Some(t) =>
        if (timerMsg.owner ne this) {
          // after restart, it was from an old instance that was enqueued in mailbox before canceled
          log.debug("Received timer [{}] from old restarted instance, discarding", timerMsg.key)
          OptionVal.None // message should be ignored
        } else if (timerMsg.generation == t.generation) {
          // valid timer
          if (!t.repeat)
            timers -= t.key
          OptionVal.Some(t.msg.asInstanceOf[AnyRef])
        } else {
          // it was from an old timer that was enqueued in mailbox before canceled
          log.debug(
            "Received timer [{}] from from old generation [{}], expected generation [{}], discarding",
            timerMsg.key,
            timerMsg.generation,
            t.generation)
          OptionVal.None // message should be ignored
        }
    }
  }

}
