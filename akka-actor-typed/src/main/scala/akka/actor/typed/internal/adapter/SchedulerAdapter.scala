/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adapter

import java.time.Duration

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.Cancellable
import akka.actor.typed.Scheduler
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SchedulerAdapter {
  def toClassic(scheduler: Scheduler): akka.actor.Scheduler =
    scheduler match {
      case s: SchedulerAdapter => s.classicScheduler
      case _ =>
        throw new UnsupportedOperationException(
          "unknown Scheduler type " +
          s"($scheduler of class ${scheduler.getClass.getName})")
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class SchedulerAdapter(private[akka] val classicScheduler: akka.actor.Scheduler) extends Scheduler {
  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleOnce(delay, runnable)

  override def scheduleOnce(delay: Duration, runnable: Runnable, executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleOnce(delay, runnable)(executor)

  override def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleWithFixedDelay(initialDelay, delay)(runnable)

  override def scheduleWithFixedDelay(
      initialDelay: Duration,
      delay: Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleWithFixedDelay(initialDelay, delay, runnable, executor)

  override def scheduleAtFixedRate(initialDelay: FiniteDuration, interval: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleAtFixedRate(initialDelay, interval)(runnable)

  override def scheduleAtFixedRate(
      initialDelay: Duration,
      interval: Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable =
    classicScheduler.scheduleAtFixedRate(initialDelay, interval, runnable, executor)

}
