/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.{ ExecutorService, ForkJoinPool, ForkJoinTask, ThreadFactory }

import com.typesafe.config.Config
import java.util.concurrent.TimeUnit

object ForkJoinExecutorConfigurator {

  /**
   * INTERNAL AKKA USAGE ONLY
   */
  final class AkkaForkJoinPool(
      parallelism: Int,
      threadFactory: ForkJoinPool.ForkJoinWorkerThreadFactory,
      unhandledExceptionHandler: Thread.UncaughtExceptionHandler,
      asyncMode: Boolean,
      maxSpareThreads: Int)
      extends ForkJoinPool(
        parallelism,
        threadFactory,
        unhandledExceptionHandler,
        asyncMode,
        0, // corePoolSize effectively equals Math.max(0, parallelism)
        parallelism + maxSpareThreads, // maximumPoolSize
        1, // minimumRunnable
        _ => true, // saturate (don't reject execution)
        60, // how long to allow a thread to be idle (matches default)
        TimeUnit.SECONDS) // unit for previous
      with LoadMetrics {
    def this(
        parallelism: Int,
        threadFactory: ForkJoinPool.ForkJoinWorkerThreadFactory,
        unhandledExceptionHandler: Thread.UncaughtExceptionHandler) =
      this(parallelism, threadFactory, unhandledExceptionHandler, asyncMode = true, 256)

    override def execute(r: Runnable): Unit =
      if (r ne null)
        super.execute(
          (if (r.isInstanceOf[ForkJoinTask[_]]) r else new AkkaForkJoinTask(r)).asInstanceOf[ForkJoinTask[Any]])
      else
        throw new NullPointerException("Runnable was null")

    def atFullThrottle(): Boolean = this.getActiveThreadCount() >= this.getParallelism()
  }

  /**
   * INTERNAL AKKA USAGE ONLY
   */
  @SerialVersionUID(1L)
  final class AkkaForkJoinTask(runnable: Runnable) extends ForkJoinTask[Unit] {
    override def getRawResult(): Unit = ()
    override def setRawResult(unit: Unit): Unit = ()
    override def exec(): Boolean =
      try {
        runnable.run(); true
      } catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
          false
        case anything: Throwable =>
          val t = Thread.currentThread()
          t.getUncaughtExceptionHandler match {
            case null =>
            case some => some.uncaughtException(t, anything)
          }
          throw anything
      }
  }
}

class ForkJoinExecutorConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
    extends ExecutorServiceConfigurator(config, prerequisites) {
  import ForkJoinExecutorConfigurator._

  def validate(t: ThreadFactory): ForkJoinPool.ForkJoinWorkerThreadFactory = t match {
    case correct: ForkJoinPool.ForkJoinWorkerThreadFactory => correct
    case _ =>
      throw new IllegalStateException(
        "The prerequisites for the ForkJoinExecutorConfigurator is a ForkJoinPool.ForkJoinWorkerThreadFactory!")
  }

  class ForkJoinExecutorServiceFactory(
      val threadFactory: ForkJoinPool.ForkJoinWorkerThreadFactory,
      val parallelism: Int,
      val asyncMode: Boolean,
      val maxSpareThreads: Int)
      extends ExecutorServiceFactory {
    def this(threadFactory: ForkJoinPool.ForkJoinWorkerThreadFactory, parallelism: Int) =
      this(threadFactory, parallelism, asyncMode = true, 256)

    def this(threadFactory: ForkJoinPool.ForkJoinWorkerThreadFactory, parallelism: Int, asyncMode: Boolean) =
      this(threadFactory, parallelism, asyncMode, 256)

    def createExecutorService: ExecutorService =
      new AkkaForkJoinPool(parallelism, threadFactory, MonitorableThreadFactory.doNothing, asyncMode, maxSpareThreads)
  }

  final def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    val tf = threadFactory match {
      case m: MonitorableThreadFactory =>
        // add the dispatcher id to the thread names
        m.withName(m.name + "-" + id)
      case other => other
    }

    val asyncMode = config.getString("task-peeking-mode") match {
      case "FIFO" => true
      case "LIFO" => false
      case _ =>
        throw new IllegalArgumentException(
          "Cannot instantiate ForkJoinExecutorServiceFactory. " +
          """"task-peeking-mode" in "fork-join-executor" section could only set to "FIFO" or "LIFO".""")
    }

    val maxSpareThreads = config.getInt("maximum-spare-threads")

    new ForkJoinExecutorServiceFactory(
      validate(tf),
      ThreadPoolConfig.scaledPoolSize(
        config.getInt("parallelism-min"),
        config.getDouble("parallelism-factor"),
        config.getInt("parallelism-max")),
      asyncMode,
      maxSpareThreads)
  }
}
