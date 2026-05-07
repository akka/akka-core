/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.typesafe.config.Config

import akka.actor.Cancellable
import akka.actor.Scheduler
import akka.event.LoggingAdapter

/**
 * For testing: scheduler that does not look at the clock, but must be
 * progressed manually by calling `timePasses`.
 *
 * This allows for faster and less timing-sensitive specs, as jobs will be
 * executed on the test thread instead of using the original
 * {ExecutionContext}. This means recreating specific scenario's becomes
 * easier, but these tests might fail to catch race conditions that only
 * happen when tasks are scheduled in parallel in 'real time'.
 */
class ExplicitlyTriggeredScheduler(
    @nowarn("msg=never used") config: Config,
    log: LoggingAdapter,
    @nowarn("msg=never used") tf: ThreadFactory)
    extends Scheduler {

  private class Item(val interval: Option[FiniteDuration], val runnable: Runnable)

  private val currentTime = new AtomicLong()
  private val scheduled = new ConcurrentHashMap[Item, Long]()

  override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    schedule(initialDelay, Some(interval), runnable)

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    schedule(delay, None, runnable)

  /**
   * Advance the clock by the specified duration, executing all outstanding jobs on the calling thread before returning.
   *
   * We will not add a dilation factor to this amount, since the scheduler API also does not apply dilation.
   * If you want the amount of time passed to be dilated, apply the dilation before passing the delay to
   * this method.
   */
  def timePasses(amount: FiniteDuration) = {
    // Give dispatchers time to clear :(. See
    // https://github.com/akka/akka-core/pull/24243#discussion_r160985493
    // for some discussion on how to deal with this properly.
    Thread.sleep(100)

    val newTime = currentTime.get + amount.toMillis
    if (log.isDebugEnabled)
      log.debug(
        s"Time proceeds from ${currentTime.get} to $newTime, currently scheduled for this period:" + scheduledTasks(
          newTime).map(item => s"\n- $item"))

    executeTasks(newTime)
    currentTime.set(newTime)
  }

  private def scheduledTasks(runTo: Long): Seq[(Item, Long)] =
    scheduled
      .entrySet()
      .asScala
      .map(s => (s.getKey, s.getValue))
      .toSeq
      .filter { case (_, v) => v <= runTo }
      .sortBy(_._2)

  @tailrec
  private[testkit] final def executeTasks(runTo: Long): Unit = {
    scheduledTasks(runTo).headOption match {
      case Some((task, time)) =>
        currentTime.set(time)
        val runResult = Try(task.runnable.run())
        scheduled.remove(task)

        if (runResult.isSuccess)
          task.interval.foreach(i => scheduled.put(task, time + i.toMillis))

        // running the runnable might have scheduled new events
        executeTasks(runTo)
      case _ => // Done
    }
  }

  private def schedule(
      initialDelay: FiniteDuration,
      interval: Option[FiniteDuration],
      runnable: Runnable): Cancellable = {
    val firstTime = currentTime.get + initialDelay.toMillis
    val item = new Item(interval, runnable)
    log.debug("Scheduled item for {}: {}", firstTime, item)
    scheduled.put(item, firstTime)

    if (initialDelay <= Duration.Zero)
      executeTasks(currentTime.get)

    new Cancellable {
      var cancelled = false

      override def cancel(): Boolean = {
        val before = scheduled.size
        scheduled.remove(item)
        cancelled = true
        before > scheduled.size
      }

      override def isCancelled: Boolean = cancelled
    }
  }

  override def maxFrequency: Double = 42

  /**
   * The scheduler need to expose its internal time for testing.
   */
  def currentTimeMs: Long = currentTime.get()
}
