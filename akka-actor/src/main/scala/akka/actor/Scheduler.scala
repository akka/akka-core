/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.control.NoStackTrace

import akka.annotation.InternalApi

/**
 * This exception is thrown by Scheduler.schedule* when scheduling is not
 * possible, e.g. after shutting down the Scheduler.
 */
private final case class SchedulerException(msg: String) extends akka.AkkaException(msg) with NoStackTrace

/**
 * An Akka scheduler service.
 *
 * For scheduling within actors `with Timers` should be preferred.
 *
 * Please note that this scheduler implementation is highly optimised for high-throughput
 * and high-frequency events. It is not to be confused with long-term schedulers such as
 * Quartz. The scheduler will throw an exception if attempts are made to schedule too far
 * into the future (which by default is around 8 months (`Int.MaxValue` seconds).
 *
 * It's possible to implement a custom `Scheduler`, although that should rarely be needed.
 *
 * A `Scheduler` implementation needs one special behavior: if
 * Closeable, it MUST execute all outstanding tasks that implement [[Scheduler.TaskRunOnClose]]
 * upon .close() in order to properly shutdown all dispatchers.
 *
 * Furthermore, this timer service MUST throw IllegalStateException if it
 * cannot schedule a task. Once scheduled, the task MUST be executed. If
 * executed upon close(), the task may execute before its timeout.
 *
 * Scheduler implementation are loaded reflectively at ActorSystem start-up
 * with the following constructor arguments:
 *  1) the system’s com.typesafe.config.Config (from system.settings.config)
 *  2) a akka.event.LoggingAdapter
 *  3) a java.util.concurrent.ThreadFactory
 */
trait Scheduler {

  /**
   * Scala API: Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a fixed `delay` between subsequent executions. E.g. if you would like the function to
   * be run after 2 seconds and thereafter every 100ms you would set `delay=Duration(2, TimeUnit.SECONDS)`
   * and `interval=Duration(100, TimeUnit.MILLISECONDS)`.
   *
   * It will not compensate the delay between tasks if the execution takes a long time or if
   * scheduling is delayed longer than specified for some reason. The delay between subsequent
   * execution will always be (at least) the given `delay`. In the long run, the
   * frequency of execution will generally be slightly lower than the reciprocal of the specified
   * `delay`.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable = {
    if (delay.length <= 0L) throw new IllegalArgumentException(s"Scheduling must use a positive delay (was $delay)")
    try new AtomicReference[Cancellable](Cancellable.initialNotCancelled) with Cancellable { self =>
      compareAndSet(
        Cancellable.initialNotCancelled,
        scheduleOnce(
          initialDelay,
          new Runnable {
            override def run(): Unit = {
              try {
                runnable.run()
                if (self.get != null)
                  swap(scheduleOnce(delay, this))
              } catch {
                // ignore failure to enqueue or terminated target actor
                case _: SchedulerException                                                                         =>
                case e: IllegalStateException if e.getCause != null && e.getCause.isInstanceOf[SchedulerException] =>
              }
            }
          }))

      @tailrec private def swap(c: Cancellable): Unit = {
        get match {
          case null => if (c != null) c.cancel()
          case old  => if (!compareAndSet(old, c)) swap(c)
        }
      }

      final def cancel(): Boolean = {
        @tailrec def tailrecCancel(): Boolean = {
          get match {
            case null => false
            case c =>
              if (c.cancel()) compareAndSet(c, null)
              else compareAndSet(c, null) || tailrecCancel()
          }
        }

        tailrecCancel()
      }

      override def isCancelled: Boolean = get == null
    } catch {
      case SchedulerException(msg) => throw new IllegalStateException(msg)
    }
  }

  /**
   * Java API: Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a fixed `delay` between subsequent executions. E.g. if you would like the function to
   * be run after 2 seconds and thereafter every 100ms you would set delay to `Duration.ofSeconds(2)`,
   * and interval to `Duration.ofMillis(100)`.
   *
   * It will not compensate the delay between tasks if the execution takes a long time or if
   * scheduling is delayed longer than specified for some reason. The delay between subsequent
   * execution will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of tasks will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  final def scheduleWithFixedDelay(
      initialDelay: java.time.Duration,
      delay: java.time.Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable = {
    scheduleWithFixedDelay(initialDelay.toScala, delay.toScala)(runnable)(executor)
  }

  /**
   * Scala API: Schedules a message to be sent repeatedly with an initial delay and
   * a fixed `delay` between messages. E.g. if you would like a message to be sent
   * immediately and thereafter every 500ms you would set `delay=Duration.Zero` and
   * `interval=Duration(500, TimeUnit.MILLISECONDS)`.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  final def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      receiver: ActorRef,
      message: Any)(
      implicit
      executor: ExecutionContext,
      sender: ActorRef = Actor.noSender): Cancellable = {
    scheduleWithFixedDelay(initialDelay, delay)(new Runnable {
      def run(): Unit = {
        receiver ! message
        if (receiver.isTerminated)
          throw SchedulerException("timer active for terminated actor")
      }
    })
  }

  /**
   * Java API: Schedules a message to be sent repeatedly with an initial delay and
   * a fixed `delay` between messages. E.g. if you would like a message to be sent
   * immediately and thereafter every 500ms you would set `delay=Duration.ZERO` and
   * `interval=Duration.ofMillis(500)`.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  final def scheduleWithFixedDelay(
      initialDelay: java.time.Duration,
      delay: java.time.Duration,
      receiver: ActorRef,
      message: Any,
      executor: ExecutionContext,
      sender: ActorRef): Cancellable = {
    scheduleWithFixedDelay(initialDelay.toScala, delay.toScala, receiver, message)(executor, sender)
  }

  /**
   * Scala API: Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a frequency. E.g. if you would like the function to be run after 2
   * seconds and thereafter every 100ms you would set `delay=Duration(2, TimeUnit.SECONDS)`
   * and `interval=Duration(100, TimeUnit.MILLISECONDS)`.
   *
   * It will compensate the delay for a subsequent task if the previous tasks took
   * too long to execute. In such cases, the actual execution interval will differ from
   * the interval passed to the method.
   *
   * If the execution of the tasks takes longer than the `interval`, the subsequent
   * execution will start immediately after the prior one completes (there will be
   * no overlap of executions). This also has the consequence that after long garbage
   * collection pauses or other reasons when the JVM was suspended all "missed" tasks
   * will execute when the process wakes up again.
   *
   * In the long run, the frequency of execution will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `scheduleAtFixedRate` can result in bursts of scheduled tasks after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `scheduleWithFixedDelay` is often preferred.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  @nowarn("msg=deprecated")
  final def scheduleAtFixedRate(initialDelay: FiniteDuration, interval: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable = {
    if (interval.length <= 0L)
      throw new IllegalArgumentException(s"Scheduling must use a positive interval (was $interval)")
    schedule(initialDelay, interval, runnable)(executor)
  }

  /**
   * Java API: Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a frequency. E.g. if you would like the function to be run after 2
   * seconds and thereafter every 100ms you would set delay to `Duration.ofSeconds(2)`,
   * and interval to `Duration.ofMillis(100)`.
   *
   * It will compensate the delay for a subsequent task if the previous tasks took
   * too long to execute. In such cases, the actual execution interval will differ from
   * the interval passed to the method.
   *
   * If the execution of the tasks takes longer than the `interval`, the subsequent
   * execution will start immediately after the prior one completes (there will be
   * no overlap of executions). This also has the consequence that after long garbage
   * collection pauses or other reasons when the JVM was suspended all "missed" tasks
   * will execute when the process wakes up again.
   *
   * In the long run, the frequency of execution will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `scheduleAtFixedRate` can result in bursts of scheduled tasks after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `scheduleWithFixedDelay` is often preferred.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  final def scheduleAtFixedRate(
      initialDelay: java.time.Duration,
      interval: java.time.Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable = {
    scheduleAtFixedRate(initialDelay.toScala, interval.toScala)(runnable)(executor)
  }

  /**
   * Scala API: Schedules a message to be sent repeatedly with an initial delay and
   * frequency. E.g. if you would like a message to be sent immediately and
   * thereafter every 500ms you would set `delay=Duration.Zero` and
   * `interval=Duration(500, TimeUnit.MILLISECONDS)`
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `scheduleAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `scheduleWithFixedDelay` is often preferred.
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  @nowarn("msg=deprecated")
  final def scheduleAtFixedRate(
      initialDelay: FiniteDuration,
      interval: FiniteDuration,
      receiver: ActorRef,
      message: Any)(
      implicit
      executor: ExecutionContext,
      sender: ActorRef = Actor.noSender): Cancellable =
    schedule(initialDelay, interval, receiver, message)

  /**
   * Java API: Schedules a message to be sent repeatedly with an initial delay and
   * frequency. E.g. if you would like a message to be sent immediately and
   * thereafter every 500ms you would set `delay=Duration.ZERO` and
   * `interval=Duration.ofMillis(500)`
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `scheduleAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `scheduleWithFixedDelay` is often preferred.
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  final def scheduleAtFixedRate(
      initialDelay: java.time.Duration,
      interval: java.time.Duration,
      receiver: ActorRef,
      message: Any,
      executor: ExecutionContext,
      sender: ActorRef): Cancellable = {
    scheduleAtFixedRate(initialDelay.toScala, interval.toScala, receiver, message)(executor, sender)
  }

  /**
   * Deprecated API: See [[Scheduler#scheduleWithFixedDelay]] or [[Scheduler#scheduleAtFixedRate]].
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, receiver: ActorRef, message: Any)(
      implicit
      executor: ExecutionContext,
      sender: ActorRef = Actor.noSender): Cancellable =
    schedule(
      initialDelay,
      interval,
      new Runnable {
        def run(): Unit = {
          receiver ! message
          if (receiver.isTerminated)
            throw SchedulerException("timer active for terminated actor")
        }
      })

  /**
   * Deprecated API: See [[Scheduler#scheduleWithFixedDelay]] or [[Scheduler#scheduleAtFixedRate]].
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final def schedule(
      initialDelay: java.time.Duration,
      interval: java.time.Duration,
      receiver: ActorRef,
      message: Any,
      executor: ExecutionContext,
      sender: ActorRef): Cancellable = {
    schedule(initialDelay.toScala, interval.toScala, receiver, message)(executor, sender)
  }

  /**
   * Deprecated API: See [[Scheduler#scheduleWithFixedDelay]] or [[Scheduler#scheduleAtFixedRate]].
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(f: => Unit)(
      implicit
      executor: ExecutionContext): Cancellable =
    schedule(initialDelay, interval, new Runnable { override def run(): Unit = f })

  /**
   * Deprecated API: See [[Scheduler#scheduleWithFixedDelay]] or [[Scheduler#scheduleAtFixedRate]].
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable

  /**
   * Deprecated API: See [[Scheduler#scheduleWithFixedDelay]] or [[Scheduler#scheduleAtFixedRate]].
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  def schedule(initialDelay: java.time.Duration, interval: java.time.Duration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable = {
    schedule(initialDelay.toScala, interval.toScala, runnable)
  }

  /**
   * Scala API: Schedules a message to be sent once with a delay, i.e. a time period that has
   * to pass before the message is sent.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  final def scheduleOnce(delay: FiniteDuration, receiver: ActorRef, message: Any)(
      implicit
      executor: ExecutionContext,
      sender: ActorRef = Actor.noSender): Cancellable =
    scheduleOnce(delay, new Runnable {
      override def run(): Unit = receiver ! message
    })

  /**
   * Java API: Schedules a message to be sent once with a delay, i.e. a time period that has
   * to pass before the message is sent.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  final def scheduleOnce(
      delay: java.time.Duration,
      receiver: ActorRef,
      message: Any,
      executor: ExecutionContext,
      sender: ActorRef): Cancellable = {
    scheduleOnce(delay.toScala, receiver, message)(executor, sender)
  }

  /**
   * Scala API: Schedules a function to be run once with a delay, i.e. a time period that has
   * to pass before the function is run.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  final def scheduleOnce(delay: FiniteDuration)(f: => Unit)(
      implicit
      executor: ExecutionContext): Cancellable =
    scheduleOnce(delay, new Runnable { override def run(): Unit = f })

  /**
   * Scala API: Schedules a Runnable to be run once with a delay, i.e. a time period that
   * has to pass before the runnable is executed.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `with Timers` should be preferred.
   */
  def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable

  /**
   * Java API: Schedules a Runnable to be run once with a delay, i.e. a time period that
   * has to pass before the runnable is executed.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `AbstractActorWithTimers` should be preferred.
   */
  def scheduleOnce(delay: java.time.Duration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable = {
    scheduleOnce(delay.toScala, runnable)(executor)
  }

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency: Double

}

// this one is just here so we can present a nice AbstractScheduler for Java
abstract class AbstractSchedulerBase extends Scheduler

/**
 * Signifies something that can be cancelled
 * There is no strict guarantee that the implementation is thread-safe,
 * but it should be good practice to make it so.
 */
trait Cancellable {

  /**
   * Cancels this Cancellable and returns true if that was successful.
   * If this cancellable was (concurrently) cancelled already, then this method
   * will return false although isCancelled will return true.
   *
   * Java & Scala API
   */
  def cancel(): Boolean

  /**
   * Returns true if and only if this Cancellable has been successfully cancelled
   *
   * Java & Scala API
   */
  def isCancelled: Boolean
}

object Cancellable {
  val alreadyCancelled: Cancellable = new Cancellable {
    def cancel(): Boolean = false
    def isCancelled: Boolean = true
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val initialNotCancelled: Cancellable = new Cancellable {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
  }
}

object Scheduler {

  /**
   * If a `TaskRunOnClose` is used in `scheduleOnce` it will be run when the `Scheduler` is
   * closed (`ActorSystem` shutdown). This is needed for the internal shutdown of dispatchers
   * in Akka and is not intended to be used by end user applications, but it's public because
   * a custom implementation of `Scheduler` must also implement this.
   */
  trait TaskRunOnClose extends Runnable
}
