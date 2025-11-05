/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.Queue

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler

/**
 * INTERNAL API
 */
private[remote] object SendQueue {
  trait ProducerApi[T] {
    def offer(message: T): Boolean

    def isEnabled: Boolean
  }

  trait QueueValue[T] extends ProducerApi[T] {
    def inject(queue: Queue[T]): Unit
  }

  private trait WakeupSignal {
    def wakeup(): Unit
  }
}

/**
 * INTERNAL API
 */
private[remote] final class SendQueue[T](postStopAction: Vector[T] => Unit)
    extends GraphStageWithMaterializedValue[SourceShape[T], SendQueue.QueueValue[T]] {
  import SendQueue._

  val out: Outlet[T] = Outlet("SendQueue.out")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, QueueValue[T]) = {
    @volatile var needWakeup = false
    val queuePromise = Promise[Queue[T]]()

    val logic = new GraphStageLogic(shape) with OutHandler with WakeupSignal {

      // using a local field for the consumer side of queue to avoid volatile access
      private var consumerQueue: Queue[T] = null

      private val wakeupCallback = getAsyncCallback[Unit] { _ =>
        if (isAvailable(out))
          tryPush()
      }

      override def preStart(): Unit = {
        implicit val ec = materializer.executionContext
        queuePromise.future.onComplete(getAsyncCallback[Try[Queue[T]]] {
          case Success(q) =>
            consumerQueue = q
            needWakeup = true
            if (isAvailable(out))
              tryPush()
          case Failure(e) =>
            failStage(e)
        }.invoke)
      }

      override def onPull(): Unit = {
        if (consumerQueue ne null)
          tryPush()
      }

      @tailrec private def tryPush(firstAttempt: Boolean = true): Unit = {
        consumerQueue.poll() match {
          case null =>
            needWakeup = true
            // additional poll() to grab any elements that might missed the needWakeup
            // and have been enqueued just after it
            if (firstAttempt)
              tryPush(firstAttempt = false)
          case elem =>
            needWakeup = false // there will be another onPull
            push(out, elem)
        }
      }

      // external call
      override def wakeup(): Unit = {
        wakeupCallback.invoke(())
      }

      override def postStop(): Unit = {
        val pending = Vector.newBuilder[T]
        if (consumerQueue ne null) {
          var msg = consumerQueue.poll()
          while (msg != null) {
            pending += msg
            msg = consumerQueue.poll()
          }
          consumerQueue.clear()
        }
        postStopAction(pending.result())

        super.postStop()
      }

      setHandler(out, this)
    }

    val queueValue = new QueueValue[T] {
      @volatile private var producerQueue: Queue[T] = null

      override def inject(q: Queue[T]): Unit = {
        producerQueue = q
        queuePromise.success(q)
      }

      override def offer(message: T): Boolean = {
        val q = producerQueue
        if (q eq null) throw new IllegalStateException("offer not allowed before injecting the queue")
        val result = q.offer(message)
        if (result && needWakeup) {
          needWakeup = false
          logic.wakeup()
        }
        result
      }

      override def isEnabled: Boolean = true
    }

    (logic, queueValue)

  }
}
