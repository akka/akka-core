/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import org.reactivestreams.{ Publisher, Subscriber, Subscription }

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object EmptyPublisher extends Publisher[Nothing] {
  import ReactiveStreamsCompliance._
  override def subscribe(subscriber: Subscriber[_ >: Nothing]): Unit =
    try {
      requireNonNullSubscriber(subscriber)
      tryOnSubscribe(subscriber, CancelledSubscription)
      tryOnComplete(subscriber)
    } catch {
      case _: SpecViolation => // nothing we can do
    }
  def apply[T]: Publisher[T] = this.asInstanceOf[Publisher[T]]
  override def toString: String = "already-completed-publisher"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ErrorPublisher(t: Throwable, name: String) extends Publisher[Nothing] {
  ReactiveStreamsCompliance.requireNonNullElement(t)

  import ReactiveStreamsCompliance._
  override def subscribe(subscriber: Subscriber[_ >: Nothing]): Unit =
    try {
      requireNonNullSubscriber(subscriber)
      tryOnSubscribe(subscriber, CancelledSubscription)
      tryOnError(subscriber, t)
    } catch {
      case _: SpecViolation => // nothing we can do
    }
  def apply[T]: Publisher[T] = this.asInstanceOf[Publisher[T]]
  override def toString: String = name
}

/**
 * INTERNAL API
 * This is only a legal subscription when it is immediately followed by
 * a termination signal (onComplete, onError).
 */
@InternalApi private[akka] case object CancelledSubscription extends Subscription {
  override def request(elements: Long): Unit = ()
  override def cancel(): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class CancellingSubscriber[T] extends Subscriber[T] {
  override def onError(t: Throwable): Unit = ()
  override def onSubscribe(s: Subscription): Unit = s.cancel()
  override def onComplete(): Unit = ()
  override def onNext(t: T): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object RejectAdditionalSubscribers extends Publisher[Nothing] {
  import ReactiveStreamsCompliance._
  override def subscribe(subscriber: Subscriber[_ >: Nothing]): Unit =
    try rejectAdditionalSubscriber(subscriber, "Publisher")
    catch {
      case _: SpecViolation => // nothing we can do
    }
  def apply[T]: Publisher[T] = this.asInstanceOf[Publisher[T]]
  override def toString: String = "already-subscribed-publisher"
}
