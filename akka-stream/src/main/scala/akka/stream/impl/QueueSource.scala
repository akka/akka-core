/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.concurrent.ExecutionContext
import scala.concurrent.{ Future, Promise }

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.OverflowStrategies._
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.stage._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object QueueSource {

  sealed trait Input[+T]
  final case class Offer[+T](elem: T, promise: Promise[QueueOfferResult]) extends Input[T]
  case object Completion extends Input[Nothing]
  final case class Failure(ex: Throwable) extends Input[Nothing]

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class QueueSource[T](
    maxBuffer: Int,
    overflowStrategy: OverflowStrategy,
    maxConcurrentOffers: Int)
    extends GraphStageWithMaterializedValue[SourceShape[T], SourceQueueWithComplete[T]] {
  import QueueSource._

  require(maxConcurrentOffers > 0, "Max concurrent offers must be greater than 0")

  val out = Outlet[T]("queueSource.out")
  override val shape: SourceShape[T] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, SourceQueueWithComplete[T]) = {
    val completion = Promise[Done]()
    val name = inheritedAttributes.nameOrDefault(getClass.toString)

    val stageLogic = new GraphStageLogic(shape) with OutHandler with SourceQueueWithComplete[T] with StageLogging {
      override protected def logSource: Class[_] = classOf[QueueSource[_]]

      var buffer: Buffer[T] = _
      var pendingOffers: Buffer[Offer[T]] = _
      var terminating = false

      override def preStart(): Unit = {
        if (maxBuffer > 0) buffer = Buffer(maxBuffer, inheritedAttributes)
        pendingOffers = Buffer(maxConcurrentOffers, inheritedAttributes)
      }
      override def postStop(): Unit = {
        val exception = new StreamDetachedException()
        completion.tryFailure(exception)
      }

      private def enqueueAndSuccess(offer: Offer[T]): Unit = {
        buffer.enqueue(offer.elem)
        offer.promise.success(QueueOfferResult.Enqueued)
      }

      private def bufferElem(offer: Offer[T]): Unit = {
        if (!buffer.isFull) {
          enqueueAndSuccess(offer)
        } else
          overflowStrategy match {
            case s: DropHead =>
              log.log(
                s.logLevel,
                "Dropping the head element because buffer is full and overflowStrategy is: [DropHead] in stream [{}]",
                name)
              buffer.dropHead()
              enqueueAndSuccess(offer)
            case s: DropTail =>
              log.log(
                s.logLevel,
                "Dropping the tail element because buffer is full and overflowStrategy is: [DropTail] in stream [{}]",
                name)
              buffer.dropTail()
              enqueueAndSuccess(offer)
            case s: DropBuffer =>
              log.log(
                s.logLevel,
                "Dropping all the buffered elements because buffer is full and overflowStrategy is: [DropBuffer] in stream [{}]",
                name)
              buffer.clear()
              enqueueAndSuccess(offer)
            case s: DropNew =>
              log.log(
                s.logLevel,
                "Dropping the new element because buffer is full and overflowStrategy is: [DropNew] in stream [{}]",
                name)
              offer.promise.success(QueueOfferResult.Dropped)
            case s: Fail =>
              log.log(s.logLevel, "Failing because buffer is full and overflowStrategy is: [Fail] in stream [{}]", name)
              val bufferOverflowException = BufferOverflowException(s"Buffer overflow (max capacity was: $maxBuffer)!")
              offer.promise.success(QueueOfferResult.Failure(bufferOverflowException))
              completion.failure(bufferOverflowException)
              failStage(bufferOverflowException)
            case s: Backpressure =>
              log.log(
                s.logLevel,
                "Backpressuring because buffer is full and overflowStrategy is: [Backpressure] in stream [{}]",
                name)
              if (pendingOffers.isFull)
                offer.promise.failure(
                  new IllegalStateException(
                    s"Too many concurrent offers. Specified maximum is $maxConcurrentOffers. " +
                    "You have to wait for one previous future to be resolved to send another request"))
              else pendingOffers.enqueue(offer)
          }
      }

      private val callback = getAsyncCallback[Input[T]] {
        case Offer(_, promise) if terminating =>
          promise.success(QueueOfferResult.Dropped)

        case offer @ Offer(elem, promise) =>
          if (maxBuffer != 0) {
            bufferElem(offer)
            if (isAvailable(out)) push(out, buffer.dequeue())
          } else if (isAvailable(out)) {
            push(out, elem)
            promise.success(QueueOfferResult.Enqueued)
          } else if (!pendingOffers.isFull)
            pendingOffers.enqueue(offer)
          else
            overflowStrategy match {
              case s @ (_: DropHead | _: DropBuffer) =>
                log.log(
                  s.logLevel,
                  "Dropping element because buffer is full and overflowStrategy is: [{}] in stream [{}]",
                  s,
                  name)
                pendingOffers.dequeue().promise.success(QueueOfferResult.Dropped)
                pendingOffers.enqueue(offer)
              case s @ (_: DropTail | _: DropNew) =>
                log.log(
                  s.logLevel,
                  "Dropping element because buffer is full and overflowStrategy is: [{}] in stream [{}]",
                  s,
                  name)
                promise.success(QueueOfferResult.Dropped)
              case s: Fail =>
                log.log(
                  s.logLevel,
                  "Failing because buffer is full and overflowStrategy is: [Fail] in stream [{}]",
                  name)
                val bufferOverflowException =
                  BufferOverflowException(s"Buffer overflow (max capacity was: $maxBuffer)!")
                promise.success(QueueOfferResult.Failure(bufferOverflowException))
                completion.failure(bufferOverflowException)
                failStage(bufferOverflowException)
              case s: Backpressure =>
                log.log(
                  s.logLevel,
                  "Failing because buffer is full and overflowStrategy is: [Backpressure] in stream [{}]",
                  name)
                promise.failure(
                  new IllegalStateException(
                    "You have to wait for previous offer to be resolved to send another request"))
            }

        case Completion =>
          if (maxBuffer != 0 && buffer.nonEmpty || pendingOffers.nonEmpty) terminating = true
          else {
            completion.success(Done)
            completeStage()
          }

        case Failure(ex) =>
          completion.failure(ex)
          failStage(ex)
      }

      setHandler(out, this)

      override def onDownstreamFinish(cause: Throwable): Unit = {
        while (pendingOffers.nonEmpty) pendingOffers.dequeue().promise.success(QueueOfferResult.QueueClosed)
        completion.success(Done)
        completeStage()
      }

      override def onPull(): Unit = {
        if (maxBuffer == 0) {
          if (pendingOffers.nonEmpty) {
            val offer = pendingOffers.dequeue()
            push(out, offer.elem)
            offer.promise.success(QueueOfferResult.Enqueued)
            if (terminating) {
              completion.success(Done)
              completeStage()
            }
          }
        } else if (buffer.nonEmpty) {
          push(out, buffer.dequeue())
          while (pendingOffers.nonEmpty && !buffer.isFull) enqueueAndSuccess(pendingOffers.dequeue())
          if (terminating && buffer.isEmpty) {
            completion.success(Done)
            completeStage()
          }
        }
      }

      override def watchCompletion() = completion.future
      override def offer(element: T): Future[QueueOfferResult] = {
        val p = Promise[QueueOfferResult]()
        callback
          .invokeWithFeedback(Offer(element, p))
          .onComplete {
            case scala.util.Success(_) =>
            case scala.util.Failure(e) => p.tryFailure(e)
          }(ExecutionContext.parasitic)
        p.future
      }
      override def complete(): Unit = callback.invoke(Completion)

      override def fail(ex: Throwable): Unit = callback.invoke(Failure(ex))

    }

    (stageLogic, stageLogic)
  }
}
