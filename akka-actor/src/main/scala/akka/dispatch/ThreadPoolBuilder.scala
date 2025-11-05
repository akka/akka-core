/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.Collection
import java.util.concurrent.{
  ArrayBlockingQueue,
  BlockingQueue,
  Callable,
  ExecutorService,
  ForkJoinPool,
  ForkJoinWorkerThread,
  LinkedBlockingQueue,
  RejectedExecutionException,
  RejectedExecutionHandler,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

import scala.concurrent.{ BlockContext, CanAwait }
import scala.concurrent.duration.Duration

object ThreadPoolConfig {
  type QueueFactory = () => BlockingQueue[Runnable]

  val defaultAllowCoreThreadTimeout: Boolean = false
  val defaultCorePoolSize: Int = 16
  val defaultMaxPoolSize: Int = 128
  val defaultTimeout: Duration = Duration(60000L, TimeUnit.MILLISECONDS)
  val defaultRejectionPolicy: RejectedExecutionHandler = new SaneRejectedExecutionHandler()

  def scaledPoolSize(floor: Int, multiplier: Double, ceiling: Int): Int =
    math.min(math.max((Runtime.getRuntime.availableProcessors * multiplier).ceil.toInt, floor), ceiling)

  def arrayBlockingQueue(capacity: Int, fair: Boolean): QueueFactory =
    () => new ArrayBlockingQueue[Runnable](capacity, fair)

  def synchronousQueue(fair: Boolean): QueueFactory = () => new SynchronousQueue[Runnable](fair)

  def linkedBlockingQueue(): QueueFactory = () => new LinkedBlockingQueue[Runnable]()

  def linkedBlockingQueue(capacity: Int): QueueFactory = () => new LinkedBlockingQueue[Runnable](capacity)

  def reusableQueue(queue: BlockingQueue[Runnable]): QueueFactory = () => queue

  def reusableQueue(queueFactory: QueueFactory): QueueFactory = reusableQueue(queueFactory())
}

/**
 * Function0 without the fun stuff (mostly for the sake of the Java API side of things)
 */
trait ExecutorServiceFactory {
  def createExecutorService: ExecutorService
}

/**
 * Generic way to specify an ExecutorService to a Dispatcher, create it with the given name if desired
 */
trait ExecutorServiceFactoryProvider {
  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory
}

/**
 * A small configuration DSL to create ThreadPoolExecutors that can be provided as an ExecutorServiceFactoryProvider to Dispatcher
 */
final case class ThreadPoolConfig(
    allowCorePoolTimeout: Boolean = ThreadPoolConfig.defaultAllowCoreThreadTimeout,
    corePoolSize: Int = ThreadPoolConfig.defaultCorePoolSize,
    maxPoolSize: Int = ThreadPoolConfig.defaultMaxPoolSize,
    threadTimeout: Duration = ThreadPoolConfig.defaultTimeout,
    queueFactory: ThreadPoolConfig.QueueFactory = ThreadPoolConfig.linkedBlockingQueue(),
    rejectionPolicy: RejectedExecutionHandler = ThreadPoolConfig.defaultRejectionPolicy)
    extends ExecutorServiceFactoryProvider {
  class ThreadPoolExecutorServiceFactory(val threadFactory: ThreadFactory) extends ExecutorServiceFactory {
    def createExecutorService: ExecutorService = {
      val service: ThreadPoolExecutor = new ThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        threadTimeout.length,
        threadTimeout.unit,
        queueFactory(),
        threadFactory,
        rejectionPolicy) with LoadMetrics {
        def atFullThrottle(): Boolean = this.getActiveCount >= this.getPoolSize
      }
      service.allowCoreThreadTimeOut(allowCorePoolTimeout)
      service
    }
  }
  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    val tf = threadFactory match {
      case m: MonitorableThreadFactory =>
        // add the dispatcher id to the thread names
        m.withName(m.name + "-" + id)
      case other => other
    }
    new ThreadPoolExecutorServiceFactory(tf)
  }
}

/**
 * A DSL to configure and create a MessageDispatcher with a ThreadPoolExecutor
 */
final case class ThreadPoolConfigBuilder(config: ThreadPoolConfig) {
  import ThreadPoolConfig._

  def withNewThreadPoolWithCustomBlockingQueue(newQueueFactory: QueueFactory): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = newQueueFactory))

  def withNewThreadPoolWithCustomBlockingQueue(queue: BlockingQueue[Runnable]): ThreadPoolConfigBuilder =
    withNewThreadPoolWithCustomBlockingQueue(reusableQueue(queue))

  def withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity: ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = linkedBlockingQueue()))

  def withNewThreadPoolWithLinkedBlockingQueueWithCapacity(capacity: Int): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = linkedBlockingQueue(capacity)))

  def withNewThreadPoolWithSynchronousQueueWithFairness(fair: Boolean): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = synchronousQueue(fair)))

  def withNewThreadPoolWithArrayBlockingQueueWithCapacityAndFairness(
      capacity: Int,
      fair: Boolean): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = arrayBlockingQueue(capacity, fair)))

  def setFixedPoolSize(size: Int): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(corePoolSize = size, maxPoolSize = size))

  def setCorePoolSize(size: Int): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(corePoolSize = size, maxPoolSize = math.max(size, config.maxPoolSize)))

  def setMaxPoolSize(size: Int): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(maxPoolSize = math.max(size, config.corePoolSize)))

  def setCorePoolSizeFromFactor(min: Int, multiplier: Double, max: Int): ThreadPoolConfigBuilder =
    setCorePoolSize(scaledPoolSize(min, multiplier, max))

  def setMaxPoolSizeFromFactor(min: Int, multiplier: Double, max: Int): ThreadPoolConfigBuilder =
    setMaxPoolSize(scaledPoolSize(min, multiplier, max))

  def setKeepAliveTimeInMillis(time: Long): ThreadPoolConfigBuilder =
    setKeepAliveTime(Duration(time, TimeUnit.MILLISECONDS))

  def setKeepAliveTime(time: Duration): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(threadTimeout = time))

  def setAllowCoreThreadTimeout(allow: Boolean): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(allowCorePoolTimeout = allow))

  def setQueueFactory(newQueueFactory: QueueFactory): ThreadPoolConfigBuilder =
    this.copy(config = config.copy(queueFactory = newQueueFactory))

  def configure(fs: Option[Function[ThreadPoolConfigBuilder, ThreadPoolConfigBuilder]]*): ThreadPoolConfigBuilder =
    fs.foldLeft(this)((c, f) => f.map(_(c)).getOrElse(c))
}

object MonitorableThreadFactory {
  val doNothing: Thread.UncaughtExceptionHandler =
    new Thread.UncaughtExceptionHandler() { def uncaughtException(thread: Thread, cause: Throwable) = () }

  private[akka] class AkkaForkJoinWorkerThread(_pool: ForkJoinPool)
      extends ForkJoinWorkerThread(_pool)
      with BlockContext {
    override def blockOn[T](thunk: => T)(implicit permission: CanAwait): T = {
      val result = new AtomicReference[Option[T]](None)
      ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker {
        def block(): Boolean = {
          result.set(Some(thunk))
          true
        }
        def isReleasable = result.get.isDefined
      })
      result.get.get // Exception intended if None
    }
  }
}

final case class MonitorableThreadFactory(
    name: String,
    daemonic: Boolean,
    contextClassLoader: Option[ClassLoader],
    exceptionHandler: Thread.UncaughtExceptionHandler = MonitorableThreadFactory.doNothing,
    protected val counter: AtomicLong = new AtomicLong)
    extends ThreadFactory
    with ForkJoinPool.ForkJoinWorkerThreadFactory {

  def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
    val t = wire(new MonitorableThreadFactory.AkkaForkJoinWorkerThread(pool))
    // Name of the threads for the ForkJoinPool are not customizable. Change it here.
    t.setName(name + "-" + counter.incrementAndGet())
    t
  }

  def newThread(runnable: Runnable): Thread = wire(new Thread(runnable, name + "-" + counter.incrementAndGet()))

  def withName(newName: String): MonitorableThreadFactory = copy(newName)

  protected def wire[T <: Thread](t: T): T = {
    t.setUncaughtExceptionHandler(exceptionHandler)
    t.setDaemon(daemonic)
    contextClassLoader.foreach(t.setContextClassLoader)
    t
  }
}

/**
 * As the name says
 */
trait ExecutorServiceDelegate extends ExecutorService {

  def executor: ExecutorService

  def execute(command: Runnable) = executor.execute(command)

  def shutdown(): Unit = { executor.shutdown() }

  def shutdownNow() = executor.shutdownNow()

  def isShutdown = executor.isShutdown

  def isTerminated = executor.isTerminated

  def awaitTermination(l: Long, timeUnit: TimeUnit) = executor.awaitTermination(l, timeUnit)

  def submit[T](callable: Callable[T]) = executor.submit(callable)

  def submit[T](runnable: Runnable, t: T) = executor.submit(runnable, t)

  def submit(runnable: Runnable) = executor.submit(runnable)

  def invokeAll[T](callables: Collection[_ <: Callable[T]]) = executor.invokeAll(callables)

  def invokeAll[T](callables: Collection[_ <: Callable[T]], l: Long, timeUnit: TimeUnit) =
    executor.invokeAll(callables, l, timeUnit)

  def invokeAny[T](callables: Collection[_ <: Callable[T]]) = executor.invokeAny(callables)

  def invokeAny[T](callables: Collection[_ <: Callable[T]], l: Long, timeUnit: TimeUnit) =
    executor.invokeAny(callables, l, timeUnit)
}

/**
 * The RejectedExecutionHandler used by Akka, it improves on CallerRunsPolicy
 * by throwing a RejectedExecutionException if the executor isShutdown.
 * (CallerRunsPolicy silently discards the runnable in this case, which is arguably broken)
 */
class SaneRejectedExecutionHandler extends RejectedExecutionHandler {
  def rejectedExecution(runnable: Runnable, threadPoolExecutor: ThreadPoolExecutor): Unit = {
    if (threadPoolExecutor.isShutdown) throw new RejectedExecutionException("Shutdown")
    else runnable.run()
  }
}
