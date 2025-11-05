/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

import akka.annotation.InternalApi
import akka.dispatch.AbstractBoundedNodeQueue
import akka.stream._
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, StageLogging }

/**
 * INTERNAL API
 */
@InternalApi private[akka] object BoundedSourceQueueStage {
  sealed trait State
  case object NeedsActivation extends State
  case object Running extends State
  case class Done(result: QueueCompletionResult) extends State
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class BoundedSourceQueueStage[T](bufferSize: Int)
    extends GraphStageWithMaterializedValue[SourceShape[T], BoundedSourceQueue[T]] {
  import BoundedSourceQueueStage._

  require(bufferSize > 0, "BoundedSourceQueueStage.bufferSize must be > 0")

  val out = Outlet[T]("BoundedSourceQueueStage.out")
  val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, BoundedSourceQueue[T]) = {

    val state = new AtomicReference[State](Running)
    val queue = new AbstractBoundedNodeQueue[T](bufferSize) {}

    object Logic extends GraphStageLogic(shape) with OutHandler with StageLogging {

      setHandler(out, this)
      val callback = getAsyncCallback[Unit] { _ =>
        clearNeedsActivation()
        run()
      }

      override def onPull(): Unit = run()

      override def onDownstreamFinish(cause: Throwable): Unit = {
        setDone(Done(QueueOfferResult.Failure(cause)))
        super.onDownstreamFinish(cause)
      }

      override def postStop(): Unit = {
        // drain queue
        while (!queue.isEmpty) queue.poll()
        val exception = new StreamDetachedException()
        setDone(Done(QueueOfferResult.Failure(exception)))
      }

      /**
       * Main loop of the queue. We do two volatile reads for the fast path of pushing elements
       * from the queue to the stream: one for the state and one to poll the queue. This leads to a somewhat simple design
       * that will quickly pick up failures from the queue interface.
       *
       * An even more optimized version could use a fast path in onPull to avoid reading the state for every element.
       */
      @tailrec
      def run(): Unit =
        state.get() match {
          case Running =>
            if (isAvailable(out)) {
              val next = queue.poll()
              if (next == null) { // queue empty
                if (!setNeedsActivation())
                  run() // didn't manage to set because stream has been completed in the meantime
                else if (!queue.isEmpty) /* && setNeedsActivation was true */ {
                  // tricky case: new element might have been added in the meantime without callback being sent because
                  // NeedsActivation had not yet been set

                  clearNeedsActivation()
                  run()
                } // else Queue.isEmpty && setNeedsActivation was true: waiting for next offer
              } else
                push(out, next) // and then: wait for pull
            } // else: wait for pull

          case Done(QueueOfferResult.QueueClosed) =>
            if (queue.isEmpty)
              completeStage()
            else if (isAvailable(out)) {
              push(out, queue.poll())
              run() // another round, might be empty now
            }
          // else !Queue.isEmpty: wait for pull to drain remaining elements
          case Done(QueueOfferResult.Failure(ex)) => failStage(ex)
          case NeedsActivation                    => throw new IllegalStateException // needs to be cleared before
        }
    }

    object Mat extends BoundedSourceQueue[T] {
      override def offer(elem: T): QueueOfferResult = state.get() match {
        case Running | NeedsActivation =>
          if (queue.add(elem)) {
            // need to query state again because stage might have switched from Running -> NeedsActivation only after
            // the last state.get but before queue.add.
            if (state.get() == NeedsActivation)
              // if this thread wins the race to toggle the flag, schedule async callback here
              if (clearNeedsActivation())
                Logic.callback.invoke(())

            QueueOfferResult.Enqueued
          } else
            QueueOfferResult.Dropped
        case Done(result) => result
      }

      override def complete(): Unit = {
        if (state.get().isInstanceOf[Done])
          throw new IllegalStateException("The queue has already been completed.")
        if (setDone(Done(QueueOfferResult.QueueClosed)))
          Logic.callback.invoke(()) // if this thread won the completion race also schedule an async callback
      }

      override def fail(ex: Throwable): Unit = {
        if (state.get().isInstanceOf[Done])
          throw new IllegalStateException("The queue has already been completed.")
        if (setDone(Done(QueueOfferResult.Failure(ex))))
          Logic.callback.invoke(()) // if this thread won the completion race also schedule an async callback
      }

      override def size(): Int = queue.size()
    }

    // some state transition helpers
    @tailrec
    def setDone(done: Done): Boolean =
      state.get() match {
        case _: Done => false
        case x =>
          if (!state.compareAndSet(x, done)) setDone(done)
          else true
      }

    @tailrec
    def clearNeedsActivation(): Boolean =
      state.get() match {
        case NeedsActivation =>
          if (!state.compareAndSet(NeedsActivation, Running)) clearNeedsActivation()
          else true

        case _ => false
      }
    @tailrec
    def setNeedsActivation(): Boolean =
      state.get() match {
        case Running =>
          if (!state.compareAndSet(Running, NeedsActivation)) setNeedsActivation()
          else true

        case _ => false
      }

    (Logic, Mat)
  }
}
