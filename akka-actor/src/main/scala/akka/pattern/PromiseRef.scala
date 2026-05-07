/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.{ Future, Promise }

import akka.actor._
import akka.util.Timeout

/**
 * A combination of a Future and an ActorRef associated with it, which points
 * to an actor performing a task which will eventually resolve the Future.
 */
trait FutureRef[T] {

  /**
   * ActorRef associated with this FutureRef.
   */
  def ref: ActorRef

  /**
   * Future associated with this FutureRef.
   */
  def future: Future[T]
}

/**
 * A combination of a Promise and an ActorRef associated with it, which points
 * to an actor performing a task which will eventually resolve the Promise.
 */
trait PromiseRef[T] { this: FutureRef[T] =>

  /**
   * ActorRef associated with this PromiseRef.
   */
  def ref: ActorRef

  /**
   * Promise associated with this PromiseRef.
   */
  def promise: Promise[T]

  /**
   * Future containing the value of the Promise associated with this PromiseRef.
   */
  final def future = promise.future

  /**
   * Converts this PromiseRef to FutureRef, effectively narrowing it's API.
   */
  def toFutureRef: FutureRef[T]
}

object PromiseRef {

  /**
   * Wraps an ActorRef and a Promise into a PromiseRef.
   */
  private[akka] def wrap[T](actorRef: ActorRef, promise: Promise[T]): PromiseRef[T] = {
    new PromiseRefImpl(actorRef, promise)
  }

  /**
   * Constructs a new PromiseRef which will be completed with the first message sent to it.
   *
   * {{{
   * // enables transparent use of PromiseRef as ActorRef and Promise
   * import PromiseRef.Implicits._
   *
   * val promiseRef = PromiseRef(system, 5.seconds)
   * promiseRef ! "message"
   * promiseRef.onComplete(println)  // prints "message"
   * }}}
   */
  def apply(system: ActorSystem, timeout: Timeout): PromiseRef[Any] = {
    val provider = system.asInstanceOf[ExtendedActorSystem].provider
    AskPromiseRef(provider, timeout)
  }

  /**
   * Constructs a new PromiseRef which will be completed with the first message sent to it.
   *
   * {{{
   * // enables transparent use of PromiseRef as ActorRef and Promise
   * import PromiseRef.Implicits._
   *
   * // requires an implicit ActorSystem in scope
   * val promiseRef = PromiseRef(5.seconds)
   * promiseRef ! "message"
   * promiseRef.future.onComplete(println)  // prints "message"
   * }}}
   */
  def apply(timeout: Timeout)(implicit system: ActorSystem): PromiseRef[Any] = {
    PromiseRef(system, timeout)
  }
}

object FutureRef {

  /**
   * Wraps an ActorRef and a Future into a FutureRef.
   */
  private[akka] def wrap[T](actorRef: ActorRef, future: Future[T]): FutureRef[T] = {
    new FutureRefImpl(actorRef, future)
  }

  /**
   * Constructs a new FutureRef which will be completed with the first message sent to it.
   *
   * {{{
   * // enables transparent use of FutureRef as ActorRef and Future
   * import FutureRef.Implicits._
   *
   * val futureRef = FutureRef(system, 5.seconds)
   * futureRef ! "message"
   * futureRef.onComplete(println)  // prints "message"
   * }}}
   */
  def apply(system: ActorSystem, timeout: Timeout): FutureRef[Any] = {
    PromiseRef(system, timeout).toFutureRef
  }

  /**
   * Constructs a new PromiseRef which will be completed with the first message sent to it.
   *
   * {{{
   * // enables transparent use of FutureRef as ActorRef and Promise
   * import FutureRef.Implicits._
   *
   * // requires an implicit ActorSystem in scope
   * val futureRef = FutureRef(5.seconds)
   * futureRef ! "message"
   * futureRef.onComplete(println)  // prints "message"
   * }}}
   */
  def apply(timeout: Timeout)(implicit system: ActorSystem): FutureRef[Any] = {
    FutureRef(system, timeout)
  }
}

private[akka] class PromiseRefImpl[T](val ref: ActorRef, val promise: Promise[T])
    extends PromiseRef[T]
    with FutureRef[T] {
  def toFutureRef: FutureRef[T] = this
}

private[akka] final class FutureRefImpl[T](val ref: ActorRef, val future: Future[T]) extends FutureRef[T]

private[akka] final class AskPromiseRef private (promiseActorRef: PromiseActorRef)
    extends PromiseRefImpl[Any](promiseActorRef, promiseActorRef.result)

private[akka] object AskPromiseRef {
  def apply(provider: ActorRefProvider, timeout: Timeout): AskPromiseRef = {
    if (timeout.duration.length > 0) {
      val promiseActorRef =
        PromiseActorRef(provider, timeout, "unknown", "unknown", "deadLetters", provider.deadLetters)
      new AskPromiseRef(promiseActorRef)
    } else {
      throw new IllegalArgumentException(s"Timeout length must not be negative, was: $timeout")
    }
  }
}
