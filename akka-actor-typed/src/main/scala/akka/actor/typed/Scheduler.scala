/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.Cancellable
import akka.annotation.DoNotInherit

/**
 * The ActorSystem facility for scheduling tasks.
 *
 * For scheduling within actors `Behaviors.withTimers` should be preferred.
 *
 * Not for user extension
 */
@DoNotInherit
trait Scheduler {

  /**
   * Scala API: Schedules a Runnable to be run once with a delay, i.e. a time period that
   * has to pass before the runnable is executed.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `Behaviors.withTimers` or `ActorContext.scheduleOnce` should be preferred.
   */
  def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable

  /**
   * Java API: Schedules a Runnable to be run once with a delay, i.e. a time period that
   * has to pass before the runnable is executed.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   * reach (calculated as: `delay / tickNanos > Int.MaxValue`).
   *
   * Note: For scheduling within actors `Behaviors.withTimers` or `ActorContext.scheduleOnce` should be preferred.
   *
   */
  def scheduleOnce(delay: java.time.Duration, runnable: Runnable, executor: ExecutionContext): Cancellable

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
   * Note: For scheduling within actors `Behaviors.withTimers` should be preferred.
   *
   */
  def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable

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
   * Note: For scheduling in actors `Behaviors.withTimers` should be preferred.
   */
  def scheduleWithFixedDelay(
      initialDelay: java.time.Duration,
      delay: java.time.Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable

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
   * Note: For scheduling within actors `Behaviors.withTimers` should be preferred.
   *
   */
  def scheduleAtFixedRate(initialDelay: FiniteDuration, interval: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable

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
   * Note: For scheduling in actors `Behaviors.withTimers` should be preferred.
   */
  def scheduleAtFixedRate(
      initialDelay: java.time.Duration,
      interval: java.time.Duration,
      runnable: Runnable,
      executor: ExecutionContext): Cancellable
}
