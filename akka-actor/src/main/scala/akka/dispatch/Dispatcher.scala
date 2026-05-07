/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.{ ExecutorService, RejectedExecutionException }
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater

import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorCell
import akka.dispatch.sysmsg.SystemMessage
import akka.event.Logging
import akka.event.Logging.Error

/**
 * The event-based ``Dispatcher`` binds a set of Actors to a thread pool backed up by a
 * `BlockingQueue`.
 *
 * The preferred way of creating dispatchers is to define configuration of it and use the
 * the `lookup` method in [[akka.dispatch.Dispatchers]].
 *
 * @param throughput positive integer indicates the dispatcher will only process so much messages at a time from the
 *                   mailbox, without checking the mailboxes of other actors. Zero or negative means the dispatcher
 *                   always continues until the mailbox is empty.
 *                   Larger values (or zero or negative) increase throughput, smaller values increase fairness
 */
class Dispatcher(
    _configurator: MessageDispatcherConfigurator,
    val id: String,
    val throughput: Int,
    val throughputDeadlineTime: Duration,
    executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
    val shutdownTimeout: FiniteDuration)
    extends MessageDispatcher(_configurator) {

  import configurator.prerequisites._

  private val batchingEnabled = !executorServiceFactoryProvider.isInstanceOf[NoBatchingExecutorFactoryProvider]

  private class LazyExecutorServiceDelegate(factory: ExecutorServiceFactory) extends ExecutorServiceDelegate {
    lazy val executor: ExecutorService = factory.createExecutorService
    def copy(): LazyExecutorServiceDelegate = new LazyExecutorServiceDelegate(factory)
  }

  /**
   * At first glance this var does not seem to be updated anywhere, but in
   * fact it is, via the esUpdater [[AtomicReferenceFieldUpdater]] below.
   */
  @nowarn("msg=never updated")
  @volatile private var executorServiceDelegate: LazyExecutorServiceDelegate =
    new LazyExecutorServiceDelegate(executorServiceFactoryProvider.createExecutorServiceFactory(id, threadFactory))

  protected final def executorService: ExecutorServiceDelegate = executorServiceDelegate

  /**
   * INTERNAL API
   */
  protected[akka] def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
    val mbox = receiver.mailbox
    mbox.enqueue(receiver.self, invocation)
    registerForExecution(mbox, hasMessageHint = true, hasSystemMessageHint = false)
  }

  /**
   * INTERNAL API
   */
  protected[akka] def systemDispatch(receiver: ActorCell, invocation: SystemMessage): Unit = {
    val mbox = receiver.mailbox
    mbox.systemEnqueue(receiver.self, invocation)
    registerForExecution(mbox, hasMessageHint = false, hasSystemMessageHint = true)
  }

  /**
   * INTERNAL API
   */
  protected[akka] def executeTask(invocation: TaskInvocation): Unit = {
    try {
      executorService.execute(invocation)
    } catch {
      case e: RejectedExecutionException =>
        try {
          executorService.execute(invocation)
        } catch {
          case e2: RejectedExecutionException =>
            eventStream.publish(Error(e, getClass.getName, getClass, "executeTask was rejected twice!"))
            throw e2
        }
    }
  }

  /**
   * INTERNAL API
   */
  protected[akka] def createMailbox(actor: akka.actor.Cell, mailboxType: MailboxType): Mailbox = {
    new Mailbox(mailboxType.create(Some(actor.self), Some(actor.system))) with DefaultSystemMessageQueue
  }

  private val esUpdater = AtomicReferenceFieldUpdater.newUpdater(
    classOf[Dispatcher],
    classOf[LazyExecutorServiceDelegate],
    "executorServiceDelegate")

  /**
   * INTERNAL API
   */
  protected[akka] def shutdown(): Unit = {
    val newDelegate = executorServiceDelegate.copy() // Doesn't matter which one we copy
    val es = esUpdater.getAndSet(this, newDelegate)
    es.shutdown()
  }

  /**
   * Returns if it was registered
   *
   * INTERNAL API
   */
  protected[akka] override def registerForExecution(
      mbox: Mailbox,
      hasMessageHint: Boolean,
      hasSystemMessageHint: Boolean): Boolean = {
    if (mbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) { //This needs to be here to ensure thread safety and no races
      if (mbox.setAsScheduled()) {
        try {
          executorService.execute(mbox)
          true
        } catch {
          case _: RejectedExecutionException =>
            try {
              executorService.execute(mbox)
              true
            } catch { //Retry once
              case e: RejectedExecutionException =>
                mbox.setAsIdle()
                eventStream.publish(Error(e, getClass.getName, getClass, "registerForExecution was rejected twice!"))
                throw e
            }
        }
      } else false
    } else false
  }

  override def batchable(runnable: Runnable): Boolean =
    if (batchingEnabled) super.batchable(runnable)
    else false

  override val toString: String = Logging.simpleName(this) + "[" + id + "]"
}

object PriorityGenerator {

  /**
   * Creates a PriorityGenerator that uses the supplied function as priority generator
   */
  def apply(priorityFunction: Any => Int): PriorityGenerator = new PriorityGenerator {
    def gen(message: Any): Int = priorityFunction(message)
  }
}

/**
 * A PriorityGenerator is a convenience API to create a Comparator that orders the messages of a
 * PriorityDispatcher
 */
abstract class PriorityGenerator extends java.util.Comparator[Envelope] {
  def gen(message: Any): Int

  final def compare(thisMessage: Envelope, thatMessage: Envelope): Int =
    gen(thisMessage.message) - gen(thatMessage.message)
}
