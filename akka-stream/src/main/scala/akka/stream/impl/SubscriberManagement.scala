/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.tailrec

import org.reactivestreams.{ Subscriber, Subscription }

/**
 * INTERNAL API
 */
private[akka] object SubscriberManagement {

  sealed trait EndOfStream {
    def apply[T](subscriber: Subscriber[T]): Unit
  }

  object NotReached extends EndOfStream {
    def apply[T](subscriber: Subscriber[T]): Unit = throw new IllegalStateException("Called apply on NotReached")
  }

  object Completed extends EndOfStream {
    import ReactiveStreamsCompliance._
    def apply[T](subscriber: Subscriber[T]): Unit = tryOnComplete(subscriber)
  }

  final case class ErrorCompleted(cause: Throwable) extends EndOfStream {
    import ReactiveStreamsCompliance._
    def apply[T](subscriber: Subscriber[T]): Unit = tryOnError(subscriber, cause)
  }

  val ShutDown = new ErrorCompleted(ActorPublisher.NormalShutdownReason)
}

/**
 * INTERNAL API
 */
private[akka] trait SubscriptionWithCursor[T] extends Subscription with ResizableMultiReaderRingBuffer.Cursor {
  import ReactiveStreamsCompliance._

  def subscriber: Subscriber[_ >: T]

  def dispatch(element: T): Unit = tryOnNext(subscriber, element)

  var active = true

  /** Do not increment directly, use `moreRequested(Long)` instead (it provides overflow protection)! */
  var totalDemand: Long = 0 // number of requested but not yet dispatched elements
  var cursor: Int = 0 // buffer cursor, managed by buffer
}

/**
 * INTERNAL API
 */
private[akka] trait SubscriberManagement[T] extends ResizableMultiReaderRingBuffer.Cursors {
  import SubscriberManagement._
  type S <: SubscriptionWithCursor[T]
  type Subscriptions = List[S]

  def initialBufferSize: Int
  def maxBufferSize: Int

  /**
   * called when we are ready to consume more elements from our upstream
   * MUST NOT call pushToDownstream
   */
  protected def requestFromUpstream(elements: Long): Unit

  /**
   * called before `shutdown()` if the stream is *not* being regularly completed
   * but shut-down due to the last subscriber having canceled its subscription
   */
  protected def cancelUpstream(): Unit

  /**
   * called when the spi.Publisher/Processor is ready to be shut down
   */
  protected def shutdown(completed: Boolean): Unit

  /**
   * Use to register a subscriber
   */
  protected def createSubscription(subscriber: Subscriber[_ >: T]): S

  private[this] val buffer = new ResizableMultiReaderRingBuffer[T](initialBufferSize, maxBufferSize, this)

  protected def bufferDebug: String = buffer.toString

  // optimize for small numbers of subscribers by keeping subscribers in a plain list
  private[this] var subscriptions: Subscriptions = Nil

  // number of elements already requested but not yet received from upstream
  private[this] var pendingFromUpstream: Long = 0

  // if non-null, holds the end-of-stream state
  private[this] var endOfStream: EndOfStream = NotReached

  def cursors = subscriptions

  /**
   * more demand was signaled from a given subscriber
   */
  protected def moreRequested(subscription: S, elements: Long): Unit =
    if (subscription.active) {
      import ReactiveStreamsCompliance._
      // check for illegal demand See 3.9
      if (elements < 1) {
        try tryOnError(subscription.subscriber, numberOfElementsInRequestMustBePositiveException)
        finally unregisterSubscriptionInternal(subscription)
      } else {
        endOfStream match {
          case eos @ (NotReached | Completed) =>
            val d = subscription.totalDemand + elements
            // Long overflow, Reactive Streams Spec 3:17: effectively unbounded
            val demand = if (d < 1) Long.MaxValue else d
            subscription.totalDemand = demand
            // returns Long.MinValue if the subscription is to be terminated
            @tailrec def dispatchFromBufferAndReturnRemainingRequested(requested: Long, eos: EndOfStream): Long =
              if (requested == 0) {
                // if we are at end-of-stream and have nothing more to read we complete now rather than after the next `requestMore`
                if ((eos ne NotReached) && buffer.count(subscription) == 0) Long.MinValue else 0
              } else if (buffer.count(subscription) > 0) {
                val goOn = try {
                  subscription.dispatch(buffer.read(subscription))
                  true
                } catch {
                  case _: SpecViolation =>
                    unregisterSubscriptionInternal(subscription)
                    false
                }
                if (goOn) dispatchFromBufferAndReturnRemainingRequested(requested - 1, eos)
                else Long.MinValue
              } else if (eos ne NotReached) Long.MinValue
              else requested

            dispatchFromBufferAndReturnRemainingRequested(demand, eos) match {
              case Long.MinValue =>
                eos(subscription.subscriber)
                unregisterSubscriptionInternal(subscription)
              case x =>
                subscription.totalDemand = x
                requestFromUpstreamIfRequired()
            }
          case ErrorCompleted(_) => // ignore, the Subscriber might not have seen our error event yet
        }
      }
    }

  private[this] final def requestFromUpstreamIfRequired(): Unit = {
    @tailrec def maxRequested(remaining: Subscriptions, result: Long = 0): Long =
      remaining match {
        case head :: tail => maxRequested(tail, math.max(head.totalDemand, result))
        case _            => result
      }
    val desired =
      Math.min(Int.MaxValue, Math.min(maxRequested(subscriptions), buffer.maxAvailable) - pendingFromUpstream).toInt
    if (desired > 0) {
      pendingFromUpstream += desired
      requestFromUpstream(desired)
    }
  }

  /**
   * this method must be called by the implementing class whenever a new value is available to be pushed downstream
   */
  protected def pushToDownstream(value: T): Unit = {
    @tailrec def dispatch(remaining: Subscriptions, sent: Boolean = false): Boolean =
      remaining match {
        case head :: tail =>
          if (head.totalDemand > 0) {
            val element = buffer.read(head)
            head.dispatch(element)
            head.totalDemand -= 1
            dispatch(tail, sent = true)
          } else dispatch(tail, sent)
        case _ => sent
      }

    endOfStream match {
      case NotReached =>
        pendingFromUpstream -= 1
        if (!buffer.write(value)) throw new IllegalStateException("Output buffer overflow")
        if (dispatch(subscriptions)) requestFromUpstreamIfRequired()
      case _ =>
        throw new IllegalStateException("pushToDownStream(...) after completeDownstream() or abortDownstream(...)")
    }
  }

  /**
   * this method must be called by the implementing class whenever
   * it has been determined that no more elements will be produced
   */
  protected def completeDownstream(): Unit = {
    if (endOfStream eq NotReached) {
      @tailrec def completeDoneSubscriptions(remaining: Subscriptions, result: Subscriptions = Nil): Subscriptions =
        remaining match {
          case head :: tail =>
            if (buffer.count(head) == 0) {
              head.active = false
              Completed(head.subscriber)
              completeDoneSubscriptions(tail, result)
            } else completeDoneSubscriptions(tail, head :: result)
          case _ => result
        }
      endOfStream = Completed
      subscriptions = completeDoneSubscriptions(subscriptions)
      if (subscriptions.isEmpty) shutdown(completed = true)
    } // else ignore, we need to be idempotent
  }

  /**
   * this method must be called by the implementing class to push an error downstream
   */
  protected def abortDownstream(cause: Throwable): Unit = {
    endOfStream = ErrorCompleted(cause)
    subscriptions.foreach(s => endOfStream(s.subscriber))
    subscriptions = Nil
  }

  /**
   * Register a new subscriber.
   */
  protected def registerSubscriber(subscriber: Subscriber[_ >: T]): Unit = endOfStream match {
    case NotReached if subscriptions.exists(_.subscriber == subscriber) =>
      ReactiveStreamsCompliance.rejectDuplicateSubscriber(subscriber)
    case NotReached                   => addSubscription(subscriber)
    case Completed if buffer.nonEmpty => addSubscription(subscriber)
    case eos                          => eos(subscriber)
  }

  private def addSubscription(subscriber: Subscriber[_ >: T]): Unit = {
    import ReactiveStreamsCompliance._
    val newSubscription = createSubscription(subscriber)
    subscriptions ::= newSubscription
    buffer.initCursor(newSubscription)
    try tryOnSubscribe(subscriber, newSubscription)
    catch {
      case _: SpecViolation => unregisterSubscriptionInternal(newSubscription)
    }
  }

  /**
   * called from `Subscription::cancel`, i.e. from another thread,
   * override to add synchronization with itself, `subscribe` and `moreRequested`
   */
  protected def unregisterSubscription(subscription: S): Unit =
    unregisterSubscriptionInternal(subscription)

  // must be idempotent
  private def unregisterSubscriptionInternal(subscription: S): Unit = {
    @tailrec def removeFrom(remaining: Subscriptions, result: Subscriptions = Nil): Subscriptions =
      remaining match {
        case head :: tail => if (head eq subscription) result.reverse_:::(tail) else removeFrom(tail, head :: result)
        case _            => throw new IllegalStateException("Subscription to unregister not found")
      }
    if (subscription.active) {
      subscriptions = removeFrom(subscriptions)
      buffer.onCursorRemoved(subscription)
      subscription.active = false
      if (subscriptions.isEmpty) {
        if (endOfStream eq NotReached) {
          endOfStream = ShutDown
          cancelUpstream()
        }
        shutdown(completed = false)
      } else requestFromUpstreamIfRequired() // we might have removed a "blocking" subscriber and can continue now
    } // else ignore, we need to be idempotent
  }
}
