/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.{ switch, tailrec }
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import akka.dispatch.AbstractNodeQueue

private[akka] object SerializedSuspendableExecutionContext {
  final val Off = 0
  final val On = 1
  final val Suspended = 2

  def apply(throughput: Int)(implicit context: ExecutionContext): SerializedSuspendableExecutionContext =
    new SerializedSuspendableExecutionContext(throughput)(context match {
      case s: SerializedSuspendableExecutionContext => s.context
      case other                                    => other
    })
}

/**
 * This `ExecutionContext` allows to wrap an underlying `ExecutionContext` and provide guaranteed serial execution
 * of tasks submitted to it. On top of that it also allows for *suspending* and *resuming* processing of tasks.
 *
 * WARNING: This type must never leak into User code as anything but `ExecutionContext`
 *
 * @param throughput maximum number of tasks to be executed in serial before relinquishing the executing thread.
 * @param context the underlying context which will be used to actually execute the submitted tasks
 */
private[akka] final class SerializedSuspendableExecutionContext(throughput: Int)(val context: ExecutionContext)
    extends AbstractNodeQueue[Runnable]
    with Runnable
    with ExecutionContext {
  import SerializedSuspendableExecutionContext._
  require(
    throughput > 0,
    s"SerializedSuspendableExecutionContext.throughput must be greater than 0 but was $throughput")

  private final val state = new AtomicInteger(Off)
  @tailrec private final def addState(newState: Int): Boolean = {
    val c = state.get
    state.compareAndSet(c, c | newState) || addState(newState)
  }
  @tailrec private final def remState(oldState: Int): Unit = {
    val c = state.get
    if (state.compareAndSet(c, c & ~oldState)) attach() else remState(oldState)
  }

  /**
   * Resumes execution of tasks until `suspend` is called,
   * if it isn't currently suspended, it is a no-op.
   * This operation is idempotent.
   */
  final def resume(): Unit = remState(Suspended)

  /**
   * Suspends execution of tasks until `resume` is called,
   * this operation is idempotent.
   */
  final def suspend(): Unit = addState(Suspended)

  final def run(): Unit = {
    @tailrec def run(done: Int): Unit =
      if (done < throughput && state.get == On) {
        poll() match {
          case null => ()
          case some =>
            try some.run()
            catch { case NonFatal(t) => context.reportFailure(t) }
            run(done + 1)
        }
      }
    try run(0)
    finally remState(On)
  }

  final def attach(): Unit = if (!isEmpty() && state.compareAndSet(Off, On)) context.execute(this)
  override final def execute(task: Runnable): Unit =
    try add(task)
    finally attach()
  override final def reportFailure(t: Throwable): Unit = context.reportFailure(t)

  /**
   * O(N)
   * @return the number of Runnable's currently enqueued
   */
  final def size(): Int = count()

  override final def toString: String = (state.get: @switch) match {
    case 0 => "Off"
    case 1 => "On"
    case 2 => "Off & Suspended"
    case 3 => "On & Suspended"
  }
}
