/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.{ concurrent => juc }

import scala.annotation.unchecked.uncheckedVariance

import akka.NotUsed
import akka.stream.impl.JavaFlowAndRsConverters
import akka.stream.scaladsl

/**
 * For use only with `JDK 9+`.
 *
 * Provides support for `java.util.concurrent.Flow.*` interfaces which mirror the Reactive Streams
 * interfaces from `org.reactivestreams`. See: [http://www.reactive-streams.org/](reactive-streams.org).
 */
object JavaFlowSupport {
  import JavaFlowAndRsConverters.Implicits._

  /**
   * [[akka.stream.scaladsl.Source]] factories operating with `java.util.concurrent.Flow.*` interfaces.
   */
  object Source {

    /**
     * Helper to create [[Source]] from [[java.util.concurrent.Flow.Publisher]].
     *
     * Construct a transformation starting with given publisher. The transformation steps
     * are executed by a series of [[java.util.concurrent.Flow.Processor]] instances
     * that mediate the flow of elements downstream and the propagation of
     * back-pressure upstream.
     *
     * @see See also [[Source.fromPublisher]] if wanting to integrate with [[org.reactivestreams.Publisher]] instead
     *      (which carries the same semantics, however existed before RS's inclusion in Java 9).
     */
    final
    //#fromPublisher
    def fromPublisher[T](publisher: java.util.concurrent.Flow.Publisher[T]): Source[T, NotUsed] =
      //#fromPublisher
      scaladsl.Source.fromPublisher(publisher.asRs)

    /**
     * Creates a `Source` that is materialized as a [[java.util.concurrent.Flow.Subscriber]]
     *
     * @see See also [[Source.asSubscriber]] if wanting to integrate with [[org.reactivestreams.Subscriber]] instead
     *      (which carries the same semantics, however existed before RS's inclusion in Java 9).
     */
    final
    //#asSubscriber
    def asSubscriber[T]: Source[T, java.util.concurrent.Flow.Subscriber[T]] =
      //#asSubscriber
      scaladsl.Source.asSubscriber[T].mapMaterializedValue(_.asJava)
  }

  /**
   * [[akka.stream.scaladsl.Flow]] factories operating with `java.util.concurrent.Flow.*` interfaces.
   */
  object Flow {

    /**
     * Creates a Flow from a Reactive Streams [[org.reactivestreams.Processor]]
     */
    def fromProcessor[I, O](processorFactory: () => juc.Flow.Processor[I, O]): Flow[I, O, NotUsed] = {
      fromProcessorMat(() => (processorFactory(), NotUsed))
    }

    /**
     * Creates a Flow from a Reactive Streams [[java.util.concurrent.Flow.Processor]] and returns a materialized value.
     */
    def fromProcessorMat[I, O, M](processorFactory: () => (juc.Flow.Processor[I, O], M)): Flow[I, O, M] =
      scaladsl.Flow.fromProcessorMat { () =>
        val (processor, mat) = processorFactory()
        (processor.asRs, mat)
      }

    /**
     * Converts this Flow to a [[RunnableGraph]] that materializes to a Reactive Streams [[java.util.concurrent.Flow.Processor]]
     * which implements the operations encapsulated by this Flow. Every materialization results in a new Processor
     * instance, i.e. the returned [[RunnableGraph]] is reusable.
     *
     * @return A [[RunnableGraph]] that materializes to a Processor when run() is called on it.
     */
    def toProcessor[In, Out, Mat](
        self: Flow[In, Out, Mat]): RunnableGraph[juc.Flow.Processor[In @uncheckedVariance, Out @uncheckedVariance]] =
      Source.asSubscriber[In].via(self).toMat(Sink.asPublisher[Out](fanout = false))(Keep.both).mapMaterializedValue {
        case (sub, pub) =>
          new juc.Flow.Processor[In, Out] {
            override def onError(t: Throwable): Unit = sub.onError(t)
            override def onSubscribe(s: juc.Flow.Subscription): Unit = sub.onSubscribe(s)
            override def onComplete(): Unit = sub.onComplete()
            override def onNext(t: In): Unit = sub.onNext(t)
            override def subscribe(s: juc.Flow.Subscriber[_ >: Out]): Unit = pub.subscribe(s)
          }
      }
  }

  /**
   * [[akka.stream.scaladsl.Sink]] factories operating with `java.util.concurrent.Flow.*` interfaces.
   */
  object Sink {

    /**
     * A `Sink` that materializes into a [[java.util.concurrent.Flow.Publisher]].
     *
     * If `fanout` is `WITH_FANOUT`, the materialized `Publisher` will support multiple `Subscriber`s and
     * the size of the `inputBuffer` configured for this operator becomes the maximum number of elements that
     * the fastest [[java.util.concurrent.Flow.Subscriber]] can be ahead of the slowest one before slowing
     * the processing down due to back pressure.
     *
     * If `fanout` is `WITHOUT_FANOUT` then the materialized `Publisher` will only support a single `Subscriber` and
     * reject any additional `Subscriber`s.
     */
    final def asPublisher[T](fanout: Boolean): Sink[T, juc.Flow.Publisher[T]] =
      scaladsl.Sink.asPublisher[T](fanout).mapMaterializedValue(_.asJava)

    /**
     * Helper to create [[Sink]] from [[java.util.concurrent.Flow.Subscriber]].
     */
    final def fromSubscriber[T](s: juc.Flow.Subscriber[T]): Sink[T, NotUsed] =
      scaladsl.Sink.fromSubscriber(s.asRs)
  }

}
