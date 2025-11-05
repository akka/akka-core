/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.io.PrintStream
import java.lang.management.{ ManagementFactory, ThreadInfo }
import java.util.Date
import java.util.concurrent.{ CountDownLatch, TimeoutException }

import scala.concurrent.{ Await, Awaitable, CanAwait, Promise }
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * The Coroner can be used to print a diagnostic report of the JVM state,
 * including stack traces and deadlocks. A report can be printed directly, by
 * calling `printReport`. Alternatively, the Coroner can be asked to `watch`
 * the JVM and generate a report at a later time - unless the Coroner is cancelled
 * by that time.
 *
 * The latter method is useful for printing diagnostics in the event that, for
 * example, a unit test stalls and fails to cancel the Coroner in time. The
 * Coroner will assume that the test has "died" and print a report to aid in
 * debugging.
 */
object Coroner {

  /**
   * Used to cancel the Coroner after calling `watch`.
   * The result of this Awaitable will be `true` if it has been cancelled.
   */
  trait WatchHandle extends Awaitable[Boolean] {

    /**
     * Will try to ensure that the Coroner has finished reporting.
     */
    def cancel(): Unit
  }

  private class WatchHandleImpl(startAndStopDuration: FiniteDuration) extends WatchHandle {
    val cancelPromise = Promise[Boolean]()
    val startedLatch = new CountDownLatch(1)
    val finishedLatch = new CountDownLatch(1)

    def waitForStart(): Unit = {
      startedLatch.await(startAndStopDuration.length, startAndStopDuration.unit)
    }

    def started(): Unit = startedLatch.countDown()

    def finished(): Unit = finishedLatch.countDown()

    def expired(): Unit = cancelPromise.trySuccess(false)

    override def cancel(): Unit = {
      cancelPromise.trySuccess(true)
      finishedLatch.await(startAndStopDuration.length, startAndStopDuration.unit)
    }

    override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
      result(atMost)
      this
    }

    override def result(atMost: Duration)(implicit permit: CanAwait): Boolean =
      try {
        Await.result(cancelPromise.future, atMost)
      } catch { case _: TimeoutException => false }

  }

  val defaultStartAndStopDuration = 1.second

  /**
   * Ask the Coroner to print a report if it is not cancelled by the given deadline.
   * The returned handle can be used to perform the cancellation.
   *
   * If displayThreadCounts is set to true, then the Coroner will print thread counts during start
   * and stop.
   */
  def watch(
      duration: FiniteDuration,
      reportTitle: String,
      out: PrintStream,
      startAndStopDuration: FiniteDuration = defaultStartAndStopDuration,
      displayThreadCounts: Boolean = false): WatchHandle = {

    val watchedHandle = new WatchHandleImpl(startAndStopDuration)

    def triggerReportIfOverdue(duration: Duration): Unit = {
      val threadMx = ManagementFactory.getThreadMXBean()
      val startThreads = threadMx.getThreadCount
      if (displayThreadCounts) {
        threadMx.resetPeakThreadCount()
        out.println(s"Coroner Thread Count starts at $startThreads in $reportTitle")
      }
      watchedHandle.started()
      try {
        if (!Await.result(watchedHandle, duration)) {
          watchedHandle.expired()
          out.println(s"Coroner not cancelled after ${duration.toMillis}ms. Looking for signs of foul play...")
          try printReport(reportTitle, out)
          catch {
            case NonFatal(ex) => {
              out.println("Error displaying Coroner's Report")
              ex.printStackTrace(out)
            }
          }
        }
      } finally {
        if (displayThreadCounts) {
          val endThreads = threadMx.getThreadCount
          out.println(
            s"Coroner Thread Count started at $startThreads, ended at $endThreads, peaked at ${threadMx.getPeakThreadCount} in $reportTitle")
        }
        out.flush()
        watchedHandle.finished()
      }
    }
    new Thread(new Runnable { def run = triggerReportIfOverdue(duration) }, "Coroner").start()
    watchedHandle.waitForStart()
    watchedHandle
  }

  /**
   * Print a report containing diagnostic information.
   */
  def printReport(reportTitle: String, out: PrintStream): Unit = {
    import out.println

    val osMx = ManagementFactory.getOperatingSystemMXBean()
    val rtMx = ManagementFactory.getRuntimeMXBean()
    val memMx = ManagementFactory.getMemoryMXBean()
    val threadMx = ManagementFactory.getThreadMXBean()

    println(s"""#Coroner's Report: $reportTitle
                #OS Architecture: ${osMx.getArch()}
                #Available processors: ${osMx.getAvailableProcessors()}
                #System load (last minute): ${osMx.getSystemLoadAverage()}
                #VM start time: ${new Date(rtMx.getStartTime())}
                #VM uptime: ${rtMx.getUptime()}ms
                #Heap usage: ${memMx.getHeapMemoryUsage()}
                #Non-heap usage: ${memMx.getNonHeapMemoryUsage()}""".stripMargin('#'))

    // If we look too soon, we've seen the JVM report a thread
    // (in that case deadlock-thread-a from CoronerSpec)
    // waiting on the lock while no thread seemed to be holding it
    Thread.sleep(300)

    def dumpAllThreads: Seq[ThreadInfo] =
      threadMx.dumpAllThreads(threadMx.isObjectMonitorUsageSupported, threadMx.isSynchronizerUsageSupported).toSeq

    def findDeadlockedThreads: (Seq[ThreadInfo], String) = {
      val (ids, desc) = if (threadMx.isSynchronizerUsageSupported()) {
        (threadMx.findDeadlockedThreads(), "monitors and ownable synchronizers")
      } else {
        (threadMx.findMonitorDeadlockedThreads(), "monitors, but NOT ownable synchronizers")
      }
      if (ids == null) {
        (Seq.empty, desc)
      } else {
        val maxTraceDepth = 1000 // Seems deep enough
        (threadMx.getThreadInfo(ids, maxTraceDepth).toSeq, desc)
      }
    }

    def printThreadInfos(threadInfos: Seq[ThreadInfo]) = {
      if (threadInfos.isEmpty) {
        println("None")
      } else {
        for (ti <- threadInfos.sortBy(_.getThreadName)) { println(threadInfoToString(ti)) }
      }
    }

    def threadInfoToString(ti: ThreadInfo): String = {
      val sb = new java.lang.StringBuilder
      sb.append("\"")
      sb.append(ti.getThreadName)
      sb.append("\" Id=")
      sb.append(ti.getThreadId)
      sb.append(" ")
      sb.append(ti.getThreadState)

      if (ti.getLockName != null) {
        sb.append(" on " + ti.getLockName)
      }

      if (ti.getLockOwnerName != null) {
        sb.append(" owned by \"")
        sb.append(ti.getLockOwnerName)
        sb.append("\" Id=")
        sb.append(ti.getLockOwnerId)
      }

      if (ti.isSuspended) {
        sb.append(" (suspended)")
      }

      if (ti.isInNative) {
        sb.append(" (in native)")
      }

      sb.append('\n')

      def appendMsg(msg: String, o: Any) = {
        sb.append(msg)
        sb.append(o)
        sb.append('\n')
      }

      val stackTrace = ti.getStackTrace
      for (i <- 0 until stackTrace.length) {
        val ste = stackTrace(i)
        appendMsg("\tat ", ste)
        if (i == 0 && ti.getLockInfo != null) {
          import java.lang.Thread.State._
          ti.getThreadState match {
            case BLOCKED       => appendMsg("\t-  blocked on ", ti.getLockInfo)
            case WAITING       => appendMsg("\t-  waiting on ", ti.getLockInfo)
            case TIMED_WAITING => appendMsg("\t-  waiting on ", ti.getLockInfo)
            case _             =>
          }
        }

        for (mi <- ti.getLockedMonitors if mi.getLockedStackDepth == i)
          appendMsg("\t-  locked ", mi)
      }

      val locks = ti.getLockedSynchronizers
      if (locks.length > 0) {
        appendMsg("\n\tNumber of locked synchronizers = ", locks.length)
        for (li <- locks) appendMsg("\t- ", li)
      }
      sb.append('\n')
      sb.toString
    }

    println("All threads:")
    printThreadInfos(dumpAllThreads)

    val (deadlockedThreads, deadlockDesc) = findDeadlockedThreads
    println(s"Deadlocks found for $deadlockDesc:")
    printThreadInfos(deadlockedThreads)
  }

}

/**
 * Mixin for tests that should be watched by the Coroner. The `startCoroner`
 * and `stopCoroner` methods should be called before and after the test runs.
 * The Coroner will display its report if the test takes longer than the
 * (dilated) `expectedTestDuration` to run.
 *
 * If displayThreadCounts is set to true, then the Coroner will print thread
 * counts during start and stop.
 */
trait WatchedByCoroner {
  self: TestKit =>

  @volatile private var coronerWatch: Coroner.WatchHandle = _

  final def startCoroner(): Unit = {
    coronerWatch = Coroner.watch(
      expectedTestDuration.dilated,
      getClass.getName,
      System.err,
      startAndStopDuration.dilated,
      displayThreadCounts)
  }

  final def stopCoroner(): Unit = {
    coronerWatch.cancel()
    coronerWatch = null
  }

  def expectedTestDuration: FiniteDuration

  def displayThreadCounts: Boolean = true

  def startAndStopDuration: FiniteDuration = Coroner.defaultStartAndStopDuration
}
