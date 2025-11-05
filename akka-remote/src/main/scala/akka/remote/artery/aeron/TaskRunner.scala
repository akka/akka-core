/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import java.util.concurrent.TimeUnit.{ MICROSECONDS, MILLISECONDS }

import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.agrona.concurrent.{ BackoffIdleStrategy, BusySpinIdleStrategy, IdleStrategy, SleepingIdleStrategy }

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.dispatch.{ AbstractNodeQueue, MonitorableThreadFactory }
import akka.event.Logging

/**
 * INTERNAL API
 */
private[akka] object TaskRunner {

  type Task = () => Boolean
  sealed trait Command
  case object Shutdown extends Command
  final case class Add(task: Task) extends Command
  final case class Remove(task: Task) extends Command

  final class CommandQueue extends AbstractNodeQueue[Command]

  /**
   * A specialized collection with allocation free add, remove and iterate of
   * elements. The order of the elements is not important.
   */
  private final class ArrayBag[T <: AnyRef: ClassTag] {
    private var elements = Array.ofDim[T](16)

    def add(e: T): Unit = {
      val size = elements.length
      @tailrec def tryAdd(i: Int): Unit = {
        if (i == size) {
          doubleCapacity()
          elements(i) = e
        } else if (elements(i) eq null)
          elements(i) = e
        else
          tryAdd(i + 1) //recursive
      }
      tryAdd(0)
    }

    def remove(e: T): Unit = {
      val size = elements.length
      @tailrec def tryRemove(i: Int): Unit = {
        if (i == size)
          () // not found
        else if (elements(i) == e)
          elements(i) = null.asInstanceOf[T]
        else
          tryRemove(i + 1) //recursive
      }
      tryRemove(0)
    }

    def removeAll(): Unit = {
      var i = 0
      while (i < elements.length) {
        elements(i) = null.asInstanceOf[T]
        i += 1
      }
    }

    /**
     * All elements as an array for efficient iteration.
     * The elements can be `null`.
     */
    def all: Array[T] = elements

    private def doubleCapacity(): Unit = {
      val newCapacity = elements.length << 1
      if (newCapacity < 0)
        throw new IllegalStateException("Sorry, too big")
      val a = Array.ofDim[T](newCapacity)
      System.arraycopy(elements, 0, a, 0, elements.length)
      elements = a
    }

    override def toString(): String =
      elements.filterNot(_ eq null).mkString("[", ",", "]")
  }

  def createIdleStrategy(idleCpuLevel: Int): IdleStrategy = {
    if (idleCpuLevel == 1)
      new SleepingIdleStrategy(MILLISECONDS.toNanos(1))
    else if (idleCpuLevel == 10)
      new BusySpinIdleStrategy
    else {
      // spin between 1200 to 8900 depending on idleCpuLevel
      val spinning = 1100 * idleCpuLevel - 1000
      val yielding = 5 * idleCpuLevel
      val minParkNanos = 1
      // park between 220 and 10 micros depending on idleCpuLevel
      val maxParkNanos = MICROSECONDS.toNanos(280 - 30 * idleCpuLevel)
      new BackoffIdleStrategy(spinning, yielding, minParkNanos, maxParkNanos)
    }
  }
}

/**
 * INTERNAL API
 */
private[akka] class TaskRunner(system: ExtendedActorSystem, val idleCpuLevel: Int) extends Runnable {
  import TaskRunner._

  private val log = Logging(system, classOf[TaskRunner])
  private[this] var running = false
  private[this] val cmdQueue = new CommandQueue
  private[this] val tasks = new ArrayBag[Task]
  private[this] val shutdown = Promise[Done]()

  private val idleStrategy = createIdleStrategy(idleCpuLevel)
  private var reset = false

  def start(): Unit = {
    val tf = system.threadFactory match {
      case m: MonitorableThreadFactory =>
        m.withName(m.name + "-taskrunner")
      case other => other
    }
    val thread = tf.newThread(this)
    thread.start()
  }

  def stop(): Future[Done] = {
    command(Shutdown)
    shutdown.future
  }

  def command(cmd: Command): Unit = {
    cmdQueue.add(cmd)
  }

  override def run(): Unit = {
    try {
      running = true
      while (running) {
        processCommand(cmdQueue.poll())
        if (running) {
          executeTasks()
          if (reset) {
            reset = false
            idleStrategy.reset()
          }
          idleStrategy.idle()
        }
      }
    } catch {
      case NonFatal(e) =>
        log.error(e, e.getMessage)
    }
  }

  private def executeTasks(): Unit = {
    val elements = tasks.all
    var i = 0
    val size = elements.length
    while (i < size) {
      val task = elements(i)
      if (task ne null) try {
        if (task()) {
          tasks.remove(task)
          reset = true
        }
      } catch {
        case NonFatal(e) =>
          log.error(e, "Task failed")
          tasks.remove(task)
      }
      i += 1
    }
  }

  private def processCommand(cmd: Command): Unit = {
    cmd match {
      case null         => // no command
      case Add(task)    => tasks.add(task)
      case Remove(task) => tasks.remove(task)
      case Shutdown =>
        running = false
        tasks.removeAll() // gc friendly
        while (cmdQueue.poll() != null) () // gc friendly
        shutdown.trySuccess(Done)
    }
  }

}
