/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.util.control.NonFatal

import org.reactivestreams.{ Subscriber, Subscription }

import akka.annotation.InternalApi
import akka.stream.SubscriptionWithCancelException

/**
 * INTERNAL API
 */
@InternalApi private[stream] object ReactiveStreamsCompliance {

  final val CanNotSubscribeTheSameSubscriberMultipleTimes =
    "can not subscribe the same subscriber multiple times (see reactive-streams specification, rules 1.10 and 2.12)"

  final val SupportsOnlyASingleSubscriber =
    "only supports one subscriber (which is allowed, see reactive-streams specification, rule 1.11)"

  final val NumberOfElementsInRequestMustBePositiveMsg =
    "The number of requested elements must be > 0 (see reactive-streams specification, rule 3.9)"

  final val SubscriberMustNotBeNullMsg = "Subscriber must not be null, rule 1.9"

  final val ExceptionMustNotBeNullMsg = "Exception must not be null, rule 2.13"

  final val ElementMustNotBeNullMsg = "Element must not be null, rule 2.13"

  final val SubscriptionMustNotBeNullMsg = "Subscription must not be null, rule 2.13"

  final def numberOfElementsInRequestMustBePositiveException: Throwable =
    new IllegalArgumentException(NumberOfElementsInRequestMustBePositiveMsg)

  final def canNotSubscribeTheSameSubscriberMultipleTimesException: Throwable =
    new IllegalStateException(CanNotSubscribeTheSameSubscriberMultipleTimes)

  final def subscriberMustNotBeNullException: Throwable =
    new NullPointerException(SubscriberMustNotBeNullMsg)

  final def exceptionMustNotBeNullException: Throwable =
    new NullPointerException(ExceptionMustNotBeNullMsg)

  final def elementMustNotBeNullException: Throwable =
    new NullPointerException(ElementMustNotBeNullMsg)

  final def subscriptionMustNotBeNullException: Throwable =
    new NullPointerException(SubscriptionMustNotBeNullMsg)

  final def rejectDuplicateSubscriber[T](subscriber: Subscriber[T]): Unit = {
    // since it is already subscribed it has received the subscription first
    // and we can emit onError immediately
    tryOnError(subscriber, canNotSubscribeTheSameSubscriberMultipleTimesException)
  }

  final def rejectAdditionalSubscriber[T](subscriber: Subscriber[T], rejector: String): Unit = {
    tryOnSubscribe(subscriber, CancelledSubscription)
    tryOnError(subscriber, new IllegalStateException(s"$rejector $SupportsOnlyASingleSubscriber"))
  }

  final def rejectDueToNonPositiveDemand[T](subscriber: Subscriber[T]): Unit =
    tryOnError(subscriber, numberOfElementsInRequestMustBePositiveException)

  final def requireNonNullSubscriber[T](subscriber: Subscriber[T]): Unit =
    if (subscriber eq null) throw subscriberMustNotBeNullException

  final def requireNonNullException(cause: Throwable): Unit =
    if (cause eq null) throw exceptionMustNotBeNullException

  final def requireNonNullElement[T](element: T): Unit =
    if (element == null) throw elementMustNotBeNullException

  final def requireNonNullSubscription(subscription: Subscription): Unit =
    if (subscription == null) throw subscriptionMustNotBeNullException

  sealed trait SpecViolation extends Throwable

  @SerialVersionUID(1L)
  final class SignalThrewException(message: String, cause: Throwable)
      extends IllegalStateException(message, cause)
      with SpecViolation

  final def tryOnError[T](subscriber: Subscriber[T], error: Throwable): Unit =
    error match {
      case sv: SpecViolation =>
        throw new IllegalStateException("It is not legal to try to signal onError with a SpecViolation", sv)
      case other =>
        try subscriber.onError(other)
        catch {
          case NonFatal(t) => throw new SignalThrewException(s"${subscriber}.onError", t)
        }
    }

  final def tryOnNext[T](subscriber: Subscriber[T], element: T): Unit = {
    requireNonNullElement(element)
    try subscriber.onNext(element)
    catch {
      case NonFatal(t) => throw new SignalThrewException(s"${subscriber}.onNext", t)
    }
  }

  final def tryOnSubscribe[T](subscriber: Subscriber[T], subscription: Subscription): Unit = {
    try subscriber.onSubscribe(subscription)
    catch {
      case NonFatal(t) => throw new SignalThrewException(s"${subscriber}.onSubscribe", t)
    }
  }

  final def tryOnComplete[T](subscriber: Subscriber[T]): Unit = {
    try subscriber.onComplete()
    catch {
      case NonFatal(t) => throw new SignalThrewException(s"${subscriber}.onComplete", t)
    }
  }

  final def tryRequest(subscription: Subscription, demand: Long): Unit = {
    if (subscription eq null)
      throw new IllegalStateException("Subscription must be not null on request() call, rule 1.3")
    try subscription.request(demand)
    catch {
      case NonFatal(t) =>
        throw new SignalThrewException("It is illegal to throw exceptions from request(), rule 3.16", t)
    }
  }

  final def tryCancel(subscription: Subscription, cause: Throwable): Unit = {
    if (subscription eq null)
      throw new IllegalStateException("Subscription must be not null on cancel() call, rule 1.3")
    try subscription match {
      case s: SubscriptionWithCancelException => s.cancel(cause)
      case s                                  => s.cancel()
    } catch {
      case NonFatal(t) =>
        throw new SignalThrewException("It is illegal to throw exceptions from cancel(), rule 3.15", t)
    }
  }

}
