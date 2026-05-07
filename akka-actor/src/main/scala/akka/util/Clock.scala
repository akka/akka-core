/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Scheduler
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] object Clock extends ExtensionId[Clock] with ExtensionIdProvider {
  override def get(system: ActorSystem): Clock = super.get(system)

  override def get(system: ClassicActorSystemProvider): Clock = super.get(system)

  override def lookup = Clock

  override def createExtension(system: ExtendedActorSystem): Clock = {
    import scala.concurrent.duration._
    val interval = system.settings.config.getDuration("akka.scheduled-clock-interval", TimeUnit.MILLISECONDS).millis
    if (interval > Duration.Zero)
      new ScheduledClock(interval, system.scheduler, system.dispatcher)
    else new NanoClock
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] trait Clock extends Extension {
  def currentTime(): Long

  def earlierTime(duration: FiniteDuration): Long
}

/**
 * INTERNAL API: Clock backed by `System.nanoTime`.
 */
@InternalApi private[akka] final class NanoClock extends Clock {
  override def currentTime(): Long = System.nanoTime()

  override def earlierTime(duration: FiniteDuration): Long = currentTime() - duration.toNanos
}

/**
 * INTERNAL API: Clock backed by `System.nanoTime` but only calls `nanoTime` with the given interval using the `scheduler`.
 * This has the benefit of not calling `nanoTime` to often when exact timestamps are not needed.
 * The `currentTime` never moves backwards (but overflows to negative in same way as `nanoTime`).
 * Subsequent calls to `currentTime` will increment the "time" with 1, unless for very high frequency where it may
 * keep the same time value until next background update.
 */
@InternalApi private[akka] final class ScheduledClock(
    updateInterval: FiniteDuration,
    scheduler: Scheduler,
    executionContext: ExecutionContext)
    extends Clock {
  private val time = new AtomicLong(System.nanoTime())
  @volatile private var updatedTime = time.get()

  private val maxIncrement = math.max(updateInterval.toNanos - 100000, 0L)
  @volatile var maxIncrementReached = false

  scheduler.scheduleWithFixedDelay(updateInterval, updateInterval) { () =>
    update()
  }(executionContext)

  @tailrec private def update(): Unit = {
    val current = time.get()
    val now = System.nanoTime()
    val newTime =
      if (now - current >= 0L) now // the diff also handles the case of Long.MaxValue overflow to negative
      else current

    if (time.compareAndSet(current, newTime)) {
      updatedTime = newTime
      maxIncrementReached = false
    } else {
      // concurrent update via currentTime(), try again
      update()
    }
  }

  override def currentTime(): Long = {
    if (maxIncrementReached) {
      time.get()
    } else {
      val now = time.incrementAndGet()
      if (now - updatedTime >= maxIncrement)
        maxIncrementReached = true
      now
    }
  }

  override def earlierTime(duration: FiniteDuration): Long =
    time.get() - duration.toNanos
}
