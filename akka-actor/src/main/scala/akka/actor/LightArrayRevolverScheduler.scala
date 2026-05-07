/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.io.Closeable
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.control.NonFatal
import com.typesafe.config.Config
import akka.dispatch.AbstractNodeQueue
import akka.event.LoggingAdapter
import akka.util.Helpers

import java.lang.invoke.MethodHandles

/**
 * This scheduler implementation is based on a revolving wheel of buckets,
 * like Netty’s HashedWheelTimer, which it advances at a fixed tick rate and
 * dispatches tasks it finds in the current bucket to their respective
 * ExecutionContexts. The tasks are held in TaskHolders, which upon
 * cancellation null out their reference to the actual task, leaving only this
 * shell to be cleaned up when the wheel reaches that bucket next time. This
 * enables the use of a simple linked list to chain the TaskHolders off the
 * wheel.
 *
 * Also noteworthy is that this scheduler does not obtain a current time stamp
 * when scheduling single-shot tasks, instead it always rounds up the task
 * delay to a full multiple of the TickDuration. This means that tasks are
 * scheduled possibly one tick later than they could be (if checking that
 * “now() + delay &lt;= nextTick” were done).
 */
class LightArrayRevolverScheduler(config: Config, log: LoggingAdapter, threadFactory: ThreadFactory)
    extends Scheduler
    with Closeable {

  import Helpers.ConfigOps
  import Helpers.Requiring

  val WheelSize =
    config
      .getInt("akka.scheduler.ticks-per-wheel")
      .requiring(ticks => (ticks & (ticks - 1)) == 0, "ticks-per-wheel must be a power of 2")
  val TickDuration =
    config
      .getMillisDuration("akka.scheduler.tick-duration")
      .requiring(
        _ >= 10.millis || !Helpers.isWindows,
        "minimum supported akka.scheduler.tick-duration on Windows is 10ms")
      .requiring(_ >= 1.millis, "minimum supported akka.scheduler.tick-duration is 1ms")
  val ShutdownTimeout = config.getMillisDuration("akka.scheduler.shutdown-timeout")

  import LightArrayRevolverScheduler._

  private def roundUp(d: FiniteDuration): FiniteDuration = {
    val dn = d.toNanos
    val r = ((dn - 1) / tickNanos + 1) * tickNanos
    if (r != dn && r > 0 && dn > 0) r.nanos else d
  }

  /**
   * Clock implementation is replaceable (for testing); the implementation must
   * return a monotonically increasing series of Long nanoseconds.
   */
  protected def clock(): Long = System.nanoTime

  /**
   * Replaceable for testing.
   */
  protected def startTick: Int = 0

  /**
   * Overridable for tests
   */
  protected def getShutdownTimeout: FiniteDuration = ShutdownTimeout

  /**
   * Overridable for tests
   */
  protected def waitNanos(nanos: Long): Unit = {
    // see https://www.javamex.com/tutorials/threads/sleep_issues.shtml
    val sleepMs = if (Helpers.isWindows) (nanos + 4999999) / 10000000 * 10 else (nanos + 999999) / 1000000
    try Thread.sleep(sleepMs)
    catch {
      case _: InterruptedException => Thread.currentThread().interrupt() // we got woken up
    }
  }

  override def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable = {
    checkMaxDelay(roundUp(delay).toNanos)
    super.scheduleWithFixedDelay(initialDelay, delay)(runnable)
  }

  override def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable = {
    if (delay.length <= 0L) throw new IllegalArgumentException(s"Scheduling must use a positive delay (was $delay)")
    checkMaxDelay(roundUp(delay).toNanos)
    try new AtomicReference[Cancellable](InitialRepeatMarker) with Cancellable { self =>
      compareAndSet(
        InitialRepeatMarker,
        schedule(
          executor,
          new AtomicLong(clock() + initialDelay.toNanos) with Runnable {
            override def run(): Unit = {
              try {
                runnable.run()
                val driftNanos = clock() - getAndAdd(delay.toNanos)
                if (self.get != null)
                  swap(schedule(executor, this, Duration.fromNanos(Math.max(delay.toNanos - driftNanos, 1))))
              } catch {
                case _: SchedulerException => // ignore failure to enqueue or terminated target actor
              }
            }
          },
          roundUp(initialDelay)))

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
      case cause @ SchedulerException(msg) => throw new IllegalStateException(msg, cause)
    }
  }

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(
      implicit executor: ExecutionContext): Cancellable =
    try schedule(executor, runnable, roundUp(delay))
    catch {
      case cause @ SchedulerException(msg) => throw new IllegalStateException(msg, cause)
    }

  override def close(): Unit = {

    def runTask(task: Runnable): Unit = {
      try task.run()
      catch {
        case e: InterruptedException => throw e
        case _: SchedulerException   => // ignore terminated actors
        case NonFatal(e)             => log.error(e, "exception while executing timer task")
      }
    }

    Await.result(stop(), getShutdownTimeout).foreach {
      case task: Scheduler.TaskRunOnClose =>
        runTask(task)
      case holder: TaskHolder => // don't run
        holder.task match {
          case task: Scheduler.TaskRunOnClose =>
            runTask(task)
          case _ => // don't run
        }
      case _ => // don't run
    }
  }

  override val maxFrequency: Double = 1.second / TickDuration

  /*
   * BELOW IS THE ACTUAL TIMER IMPLEMENTATION
   */

  private val start = clock()
  private val tickNanos = TickDuration.toNanos
  private val wheelMask = WheelSize - 1
  private val queue = new TaskQueue

  private def schedule(ec: ExecutionContext, r: Runnable, delay: FiniteDuration): TimerTask =
    if (delay.length <= 0L) { // use simple comparision instead of Ordering for performance
      if (stopped.get != null) throw SchedulerException("cannot enqueue after timer shutdown")
      ec.execute(r)
      NotCancellable
    } else if (stopped.get != null) {
      throw SchedulerException("cannot enqueue after timer shutdown")
    } else {
      val delayNanos = delay.toNanos
      checkMaxDelay(delayNanos)

      val ticks = (delayNanos / tickNanos).toInt
      val task = new TaskHolder(r, ticks, ec)
      queue.add(task)
      if (stopped.get != null && task.cancel())
        throw SchedulerException("cannot enqueue after timer shutdown")
      task
    }

  private def checkMaxDelay(delayNanos: Long): Unit =
    if (delayNanos / tickNanos > Int.MaxValue)
      // 1 second margin in the error message due to rounding
      throw new IllegalArgumentException(
        s"Task scheduled with [${delayNanos.nanos.toSeconds}] seconds delay, " +
        s"which is too far in future, maximum delay is [${(tickNanos * Int.MaxValue).nanos.toSeconds - 1}] seconds")

  private val stopped = new AtomicReference[Promise[immutable.Seq[TimerTask]]]
  private def stop(): Future[immutable.Seq[TimerTask]] = {
    val p = Promise[immutable.Seq[TimerTask]]()
    if (stopped.compareAndSet(null, p)) {
      // Interrupting the timer thread to make it shut down faster is not good since
      // it could be in the middle of executing the scheduled tasks, which might not
      // respond well to being interrupted.
      // Instead we just wait one more tick for it to finish.
      p.future
    } else Future.successful(Nil)
  }

  @volatile private var timerThread: Thread = threadFactory.newThread(new Runnable {

    var tick = startTick
    var totalTick: Long = tick // tick count that doesn't wrap around, used for calculating sleep time
    val wheel = Array.fill(WheelSize)(new TaskQueue)
    var spareTaskQueue = new TaskQueue

    private def clearAll(): immutable.Seq[TimerTask] = {
      @tailrec def collect(q: TaskQueue, acc: Vector[TimerTask]): Vector[TimerTask] = {
        q.poll() match {
          case null => acc
          case x    => collect(q, acc :+ x)
        }
      }
      (0 until WheelSize).flatMap(i => collect(wheel(i), Vector.empty)) ++ collect(queue, Vector.empty)
    }

    @tailrec
    private def checkQueue(time: Long): Unit = queue.pollNode() match {
      case null => ()
      case node =>
        node.value.ticks match {
          case 0 => node.value.executeTask()
          case ticks =>
            val futureTick = ((
              time - start + // calculate the nanos since timer start
              (ticks * tickNanos) + // adding the desired delay
              tickNanos - 1 // rounding up
            ) / tickNanos).toInt // and converting to slot number
            // tick is an Int that will wrap around, but toInt of futureTick gives us modulo operations
            // and the difference (offset) will be correct in any case
            val offset = futureTick - tick
            val bucket = futureTick & wheelMask
            node.value.ticks = offset
            wheel(bucket).addNode(node)
        }
        checkQueue(time)
    }

    override final def run(): Unit =
      try nextTick()
      catch {
        case t: Throwable =>
          log.error(t, "exception on LARS’ timer thread")
          stopped.get match {
            case null =>
              val thread = threadFactory.newThread(this)
              log.info("starting new LARS thread")
              try thread.start()
              catch {
                case e: Throwable =>
                  log.error(e, "LARS cannot start new thread, ship’s going down!")
                  stopped.set(Promise.successful(Nil))
                  clearAll()
              }
              timerThread = thread
            case p =>
              assert(stopped.compareAndSet(p, Promise.successful(Nil)), "Stop signal violated in LARS")
              p.success(clearAll())
          }
          throw t
      }

    @tailrec final def nextTick(): Unit = {
      val time = clock()
      val sleepTime = start + (totalTick * tickNanos) - time

      if (sleepTime > 0) {
        // check the queue before taking a nap
        checkQueue(time)
        waitNanos(sleepTime)
      } else {
        val bucket = tick & wheelMask
        val tasks = wheel(bucket)
        val putBack = spareTaskQueue

        @tailrec def executeBucket(): Unit = tasks.pollNode() match {
          case null => ()
          case node =>
            val task = node.value
            if (!task.isCancelled) {
              if (task.ticks >= WheelSize) {
                task.ticks -= WheelSize
                putBack.addNode(node)
              } else task.executeTask()
            }
            executeBucket()
        }
        executeBucket()
        wheel(bucket) = putBack
        // we know tasks is empty now, so we can re-use it next tick
        spareTaskQueue = tasks

        tick += 1
        totalTick += 1
      }
      stopped.get match {
        case null => nextTick()
        case p =>
          assert(stopped.compareAndSet(p, Promise.successful(Nil)), "Stop signal violated in LARS")
          p.success(clearAll())
      }
    }
  })

  timerThread.start()
}

object LightArrayRevolverScheduler {
  private[this] val taskHandle =
    MethodHandles
      .privateLookupIn(classOf[TaskHolder], MethodHandles.lookup())
      .unreflectVarHandle(classOf[TaskHolder].getDeclaredField("task"))

  private class TaskQueue extends AbstractNodeQueue[TaskHolder]

  /**
   * INTERNAL API
   */
  protected[actor] trait TimerTask extends Runnable with Cancellable

  /**
   * INTERNAL API
   */
  protected[actor] class TaskHolder(@volatile var task: Runnable, var ticks: Int, executionContext: ExecutionContext)
      extends TimerTask {

    @tailrec
    private final def extractTask(replaceWith: Runnable): Runnable =
      task match {
        case t @ (ExecutedTask | CancelledTask) => t
        case x                                  => if (taskHandle.compareAndSet(this, x, replaceWith)) x else extractTask(replaceWith)
      }

    private[akka] final def executeTask(): Boolean = extractTask(ExecutedTask) match {
      case ExecutedTask | CancelledTask => false
      case other =>
        try {
          executionContext.execute(other)
          true
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt(); false
          case NonFatal(e)             => executionContext.reportFailure(e); false
        }
    }

    // This should only be called in execDirectly
    override def run(): Unit = extractTask(ExecutedTask).run()

    override def cancel(): Boolean = extractTask(CancelledTask) match {
      case ExecutedTask | CancelledTask => false
      case _                            => true
    }

    override def isCancelled: Boolean = task eq CancelledTask
  }

  private[this] val CancelledTask = new Runnable { def run = () }
  private[this] val ExecutedTask = new Runnable { def run = () }

  private val NotCancellable: TimerTask = new TimerTask {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
    def run(): Unit = ()
  }

  private val InitialRepeatMarker: Cancellable = new Cancellable {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
  }
}
