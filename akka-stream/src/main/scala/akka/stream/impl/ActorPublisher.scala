/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.control.NoStackTrace

import org.reactivestreams.{ Publisher, Subscriber }
import org.reactivestreams.Subscription

import akka.actor.{ Actor, ActorRef, Terminated }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorPublisher {
  val NormalShutdownReasonMessage = "Cannot subscribe to shut-down Publisher"
  class NormalShutdownException extends IllegalStateException(NormalShutdownReasonMessage) with NoStackTrace
  val NormalShutdownReason: Throwable = new NormalShutdownException
  val SomeNormalShutdownReason: Some[Throwable] = Some(NormalShutdownReason)

  def apply[T](impl: ActorRef): ActorPublisher[T] = {
    val a = new ActorPublisher[T](impl)
    // Resolve cyclic dependency with actor. This MUST be the first message no matter what.
    impl ! ExposedPublisher(a.asInstanceOf[ActorPublisher[Any]])
    a
  }

}

/**
 * INTERNAL API
 *
 * When you instantiate this class, or its subclasses, you MUST send an ExposedPublisher message to the wrapped
 * ActorRef! If you don't need to subclass, prefer the apply() method on the companion object which takes care of this.
 */
@InternalApi private[akka] class ActorPublisher[T](val impl: ActorRef) extends Publisher[T] {
  import ReactiveStreamsCompliance._

  // The subscriber of an subscription attempt is first placed in this list of pending subscribers.
  // The actor will call takePendingSubscribers to remove it from the list when it has received the
  // SubscribePending message. The AtomicReference is set to null by the shutdown method, which is
  // called by the actor from postStop. Pending (unregistered) subscription attempts are denied by
  // the shutdown method. Subscription attempts after shutdown can be denied immediately.
  private val pendingSubscribers = new AtomicReference[immutable.Seq[Subscriber[_ >: T]]](Nil)

  protected val wakeUpMsg: Any = SubscribePending

  override def subscribe(subscriber: Subscriber[_ >: T]): Unit = {
    requireNonNullSubscriber(subscriber)
    @tailrec def doSubscribe(): Unit = {
      val current = pendingSubscribers.get
      if (current eq null)
        reportSubscribeFailure(subscriber)
      else {
        if (pendingSubscribers.compareAndSet(current, subscriber +: current))
          impl ! wakeUpMsg
        else
          doSubscribe() // CAS retry
      }
    }

    doSubscribe()
  }

  def takePendingSubscribers(): immutable.Seq[Subscriber[_ >: T]] = {
    val pending = pendingSubscribers.getAndSet(Nil)
    if (pending eq null) Nil else pending.reverse
  }

  def shutdown(reason: Option[Throwable]): Unit = {
    shutdownReason = reason
    pendingSubscribers.getAndSet(null) match {
      case null    => // already called earlier
      case pending => pending.foreach(reportSubscribeFailure)
    }
  }

  @volatile private var shutdownReason: Option[Throwable] = None

  private def reportSubscribeFailure(subscriber: Subscriber[_ >: T]): Unit =
    try shutdownReason match {
      case Some(_: SpecViolation) => // ok, not allowed to call onError
      case Some(e) =>
        tryOnSubscribe(subscriber, CancelledSubscription)
        tryOnError(subscriber, e)
      case None =>
        tryOnSubscribe(subscriber, CancelledSubscription)
        tryOnComplete(subscriber)
    } catch {
      case _: SpecViolation => // nothing to do
    }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ActorSubscription[T](
    final val impl: ActorRef,
    final val subscriber: Subscriber[_ >: T])
    extends Subscription {
  override def request(elements: Long): Unit = impl ! RequestMore(this, elements)
  override def cancel(): Unit = impl ! Cancel(this)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ActorSubscriptionWithCursor[T](_impl: ActorRef, _subscriber: Subscriber[_ >: T])
    extends ActorSubscription[T](_impl, _subscriber)
    with SubscriptionWithCursor[T]

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait SoftShutdown { this: Actor =>
  def softShutdown(): Unit = {
    val children = context.children
    if (children.isEmpty) {
      context.stop(self)
    } else {
      context.children.foreach(context.watch)
      context.become {
        case Terminated(_) => if (context.children.isEmpty) context.stop(self)
        case _             => // ignore all the rest, we’re practically dead
      }
    }
  }
}
