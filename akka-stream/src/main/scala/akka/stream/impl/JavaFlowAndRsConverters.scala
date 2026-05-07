/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.Flow

import JavaFlowAndRsConverters.Implicits._
import org.{ reactivestreams => rs }

import akka.annotation.InternalApi

/**
 * INTERNAL API: Provides converters between Reactive Streams (reactive-streams.org) and their Java 9+ counter-parts,
 * defined in `java.util.concurrent.Flow.*`. This API is internal because Reactive Streams will ship with such
 * adapters itself at some point, and we'd not want to duplicate that effort for users to be confused about which ones
 * to use. These adapters are used internally by Akka Streams to convert between the standards but you should not touch
 * them directly - use thr `JavaFlowSupport` classes instead.
 *
 * Please note that either of these types are designed for *inter-op* and usually should not be used directly
 * in applications. The intended use case is for shared libraries, like database drivers or similar to provide
 * the inter-operable types, such that other libraries can co-operate with them directly, if that is your use case
 * and you're using the j.u.c.Flow types, use the [[akka.stream.scaladsl.JavaFlowSupport]] sources/sinks/flows instead.
 *
 * The interfaces as well as semantic contract of these sets of interfaces.
 * Please see https://github.com/reactive-streams/reactive-streams-jvm to learn more about the specification,
 * and TCK that is available to verify an implementation is a valid implementation.
 */
@InternalApi
private[akka] object JavaFlowAndRsConverters {

  /** Adds `asJava` and `asRs` enrichment methods to Reactive Streams and j.u.c.Flow types. */
  object Implicits {
    final implicit class FlowPublisherConverter[T](val p: rs.Publisher[T]) extends AnyVal {
      def asJava: Flow.Publisher[T] =
        if (p eq null) null else JavaFlowAndRsConverters.asJava(p)
    }
    final implicit class RsPublisherConverter[T](val p: Flow.Publisher[T]) extends AnyVal {
      def asRs: rs.Publisher[T] = JavaFlowAndRsConverters.asRs(p)
    }

    final implicit class FlowSubscriberConverter[T](val s: rs.Subscriber[T]) extends AnyVal {
      def asJava: Flow.Subscriber[T] = JavaFlowAndRsConverters.asJava(s)
    }
    final implicit class RsSubscriberConverter[T](val s: Flow.Subscriber[T]) extends AnyVal {
      def asRs: rs.Subscriber[T] = JavaFlowAndRsConverters.asRs(s)
    }

    final implicit class FlowProcessorConverter[T, R](val s: rs.Processor[T, R]) extends AnyVal {
      def asJava: Flow.Processor[T, R] = JavaFlowAndRsConverters.asJava(s)
    }
    final implicit class RsProcessorConverter[T, R](val s: Flow.Processor[T, R]) extends AnyVal {
      def asRs: rs.Processor[T, R] = JavaFlowAndRsConverters.asRs(s)
    }

    final implicit class FlowSubscriptionConverter[T](val s: rs.Subscription) extends AnyVal {
      def asJava: Flow.Subscription = JavaFlowAndRsConverters.asJava(s)
    }
    final implicit class RsSubscriptionConverter[T](val s: Flow.Subscription) extends AnyVal {
      def asRs: rs.Subscription = JavaFlowAndRsConverters.asRs(s)
    }
  }

  final def asJava[T](p: rs.Publisher[T]): Flow.Publisher[T] = p match {
    case null                                     => null // null remains null
    case adapter: JavaFlowPublisherToRsAdapter[T] => adapter.delegate // unwrap adapter instead of wrapping again
    case delegate                                 => new RsPublisherToJavaFlowAdapter(delegate) // adapt, it is a real Publisher
  }
  final def asRs[T](p: Flow.Publisher[T]): rs.Publisher[T] = p match {
    case null                                     => null // null remains null
    case adapter: RsPublisherToJavaFlowAdapter[T] => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                        => new JavaFlowPublisherToRsAdapter[T](p)
  }

  final def asJava[T](s: rs.Subscription): Flow.Subscription = s match {
    case null                                     => null // null remains null
    case adapter: JavaFlowSubscriptionToRsAdapter => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                        => new RsSubscriptionToJavaFlowAdapter(s)
  }
  final def asRs[T](s: Flow.Subscription): rs.Subscription = s match {
    case null                                     => null // null remains null
    case adapter: RsSubscriptionToJavaFlowAdapter => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                        => new JavaFlowSubscriptionToRsAdapter(s)
  }

  final def asJava[T](s: rs.Subscriber[T]): Flow.Subscriber[T] =
    s match {
      case null                                      => null // null remains null
      case adapter: JavaFlowSubscriberToRsAdapter[T] => adapter.delegate // unwrap adapter instead of wrapping again
      case _                                         => new RsSubscriberToJavaFlowAdapter[T](s)
    }
  final def asRs[T](s: Flow.Subscriber[T]): rs.Subscriber[T] = s match {
    case null                                      => null // null remains null
    case adapter: RsSubscriberToJavaFlowAdapter[T] => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                         => new JavaFlowSubscriberToRsAdapter[T](s)
  }

  final def asJava[T, R](p: rs.Processor[T, R]): Flow.Processor[T, R] = p match {
    case null                                        => null // null remains null
    case adapter: JavaFlowProcessorToRsAdapter[T, R] => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                           => new RsProcessorToJavaFlowAdapter[T, R](p)
  }
  final def asRs[T, R](p: Flow.Processor[T, R]): rs.Processor[T, R] = p match {
    case null                                        => null // null remains null
    case adapter: RsProcessorToJavaFlowAdapter[T, R] => adapter.delegate // unwrap adapter instead of wrapping again
    case _                                           => new JavaFlowProcessorToRsAdapter[T, R](p)
  }

}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class JavaFlowPublisherToRsAdapter[T](val delegate: Flow.Publisher[T])
    extends rs.Publisher[T] {
  override def subscribe(rsSubscriber: rs.Subscriber[_ >: T]): Unit =
    delegate.subscribe(rsSubscriber.asJava)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class RsPublisherToJavaFlowAdapter[T](val delegate: rs.Publisher[T])
    extends Flow.Publisher[T] {
  override def subscribe(javaSubscriber: Flow.Subscriber[_ >: T]): Unit =
    delegate.subscribe(javaSubscriber.asRs)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class RsSubscriberToJavaFlowAdapter[T](val delegate: rs.Subscriber[T])
    extends Flow.Subscriber[T] {
  override def onError(t: Throwable): Unit =
    delegate.onError(t)

  override def onComplete(): Unit =
    delegate.onComplete()

  override def onNext(elem: T): Unit =
    delegate.onNext(elem)

  override def onSubscribe(s: Flow.Subscription): Unit =
    delegate.onSubscribe(s.asRs)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class JavaFlowSubscriberToRsAdapter[T](val delegate: Flow.Subscriber[T])
    extends rs.Subscriber[T] {
  override def onError(t: Throwable): Unit =
    delegate.onError(t)

  override def onComplete(): Unit =
    delegate.onComplete()

  override def onNext(elem: T): Unit =
    delegate.onNext(elem)

  override def onSubscribe(s: rs.Subscription): Unit =
    delegate.onSubscribe(s.asJava)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class RsSubscriptionToJavaFlowAdapter(val delegate: rs.Subscription)
    extends Flow.Subscription {
  override def cancel(): Unit = delegate.cancel()

  override def request(n: Long): Unit = delegate.request(n)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class JavaFlowSubscriptionToRsAdapter(val delegate: Flow.Subscription)
    extends rs.Subscription {
  override def cancel(): Unit = delegate.cancel()

  override def request(n: Long): Unit = delegate.request(n)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class RsProcessorToJavaFlowAdapter[T, R](val delegate: rs.Processor[T, R])
    extends Flow.Processor[T, R] {
  override def onError(t: Throwable): Unit =
    delegate.onError(t)

  override def onComplete(): Unit =
    delegate.onComplete()

  override def onNext(elem: T): Unit =
    delegate.onNext(elem)

  override def onSubscribe(s: Flow.Subscription): Unit =
    delegate.onSubscribe(s.asRs)

  override def subscribe(javaSubscriber: Flow.Subscriber[_ >: R]): Unit =
    delegate.subscribe(javaSubscriber.asRs)
}

/** INTERNAL API: Adapters are not meant to be touched directly */
@InternalApi private[akka] final class JavaFlowProcessorToRsAdapter[T, R](val delegate: Flow.Processor[T, R])
    extends rs.Processor[T, R] {
  override def onError(t: Throwable): Unit =
    delegate.onError(t)

  override def onComplete(): Unit =
    delegate.onComplete()

  override def onNext(elem: T): Unit =
    delegate.onNext(elem)

  override def onSubscribe(s: rs.Subscription): Unit =
    delegate.onSubscribe(s.asJava)

  override def subscribe(rsSubscriber: rs.Subscriber[_ >: R]): Unit =
    delegate.subscribe(rsSubscriber.asJava)
}
