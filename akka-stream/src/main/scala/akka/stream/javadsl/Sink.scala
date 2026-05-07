/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.BiFunction
import java.util.stream.Collector

import scala.annotation.nowarn
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.util.Try

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

import akka._
import akka.actor.ActorRef
import akka.actor.ClassicActorSystemProvider
import akka.actor.Status
import akka.japi.function
import akka.japi.function.Creator
import akka.stream._
import akka.stream.impl.LinearTraversalBuilder
import akka.stream.javadsl
import akka.stream.scaladsl
import akka.stream.scaladsl.SinkToCompletionStage
import scala.jdk.CollectionConverters._

/** Java API */
object Sink {

  /**
   * A `Sink` that will invoke the given function for every received element, giving it its previous
   * output (or the given `zero` value) and the element as input.
   * The returned [[java.util.concurrent.CompletionStage]] will be completed with value of the final
   * function evaluation when the input stream ends, or completed with `Failure`
   * if there is a failure is signaled in the stream.
   */
  def fold[U, In](zero: U, f: function.Function2[U, In, U]): javadsl.Sink[In, CompletionStage[U]] =
    new Sink(scaladsl.Sink.fold[U, In](zero)(f.apply).toCompletionStage())

  /**
   * A `Sink` that will invoke the given asynchronous function for every received element, giving it its previous
   * output (or the given `zero` value) and the element as input.
   * The returned [[java.util.concurrent.CompletionStage]] will be completed with value of the final
   * function evaluation when the input stream ends, or completed with `Failure`
   * if there is a failure is signaled in the stream.
   */
  def foldAsync[U, In](
      zero: U,
      f: function.Function2[U, In, CompletionStage[U]]): javadsl.Sink[In, CompletionStage[U]] =
    new Sink(scaladsl.Sink.foldAsync[U, In](zero)(f(_, _).asScala).toCompletionStage())

  /**
   * Creates a sink which materializes into a ``CompletionStage`` which will be completed with a result of the Java ``Collector``
   * transformation and reduction operations. This allows usage of Java streams transformations for reactive streams.
   * The ``Collector`` will trigger demand downstream. Elements emitted through the stream will be accumulated into a mutable
   * result container, optionally transformed into a final representation after all input elements have been processed.
   * The ``Collector`` can also do reduction at the end. Reduction processing is performed sequentially.
   */
  def collect[U, In](collector: Collector[In, _ <: Any, U]): Sink[In, CompletionStage[U]] =
    StreamConverters.javaCollector(() => collector)

  /**
   * A `Sink` that will invoke the given function for every received element, giving it its previous
   * output (from the second element) and the element as input.
   * The returned [[java.util.concurrent.CompletionStage]] will be completed with value of the final
   * function evaluation when the input stream ends, or completed with `Failure`
   * if there is a failure signaled in the stream.
   *
   * If the stream is empty (i.e. completes before signalling any elements),
   * the reduce operator will fail its downstream with a [[NoSuchElementException]],
   * which is semantically in-line with that Scala's standard library collections
   * do in such situations.
   */
  def reduce[In](f: function.Function2[In, In, In]): Sink[In, CompletionStage[In]] =
    new Sink(scaladsl.Sink.reduce[In](f.apply).toCompletionStage())

  /**
   * Helper to create [[Sink]] from `Subscriber`.
   */
  def fromSubscriber[In](subs: Subscriber[In]): Sink[In, NotUsed] =
    new Sink(scaladsl.Sink.fromSubscriber(subs))

  /**
   * A `Sink` that immediately cancels its upstream after materialization.
   */
  def cancelled[T](): Sink[T, NotUsed] =
    new Sink(scaladsl.Sink.cancelled)

  /**
   * A `Sink` that will consume the stream and discard the elements.
   */
  def ignore[T](): Sink[T, CompletionStage[Done]] =
    new Sink(scaladsl.Sink.ignore.toCompletionStage())

  /**
   * A [[Sink]] that will always backpressure never cancel and never consume any elements from the stream.
   * */
  def never[T]: Sink[T, CompletionStage[Done]] =
    new Sink(scaladsl.Sink.never.toCompletionStage())

  /**
   * A `Sink` that materializes into a [[org.reactivestreams.Publisher]].
   *
   * If `fanout` is `true`, the materialized `Publisher` will support multiple `Subscriber`s and
   * the size of the `inputBuffer` configured for this operator becomes the maximum number of elements that
   * the fastest [[org.reactivestreams.Subscriber]] can be ahead of the slowest one before slowing
   * the processing down due to back pressure.
   *
   * If `fanout` is `false` then the materialized `Publisher` will only support a single `Subscriber` and
   * reject any additional `Subscriber`s.
   */
  def asPublisher[T](fanout: AsPublisher): Sink[T, Publisher[T]] =
    new Sink(scaladsl.Sink.asPublisher(fanout == AsPublisher.WITH_FANOUT))

  /**
   * A `Sink` that will invoke the given procedure for each received element. The sink is materialized
   * into a [[java.util.concurrent.CompletionStage]] which will be completed with `Success` when reaching the
   * normal end of the stream, or completed with `Failure` if there is a failure signaled in
   * the stream.
   */
  def foreach[T](f: function.Procedure[T]): Sink[T, CompletionStage[Done]] =
    new Sink(scaladsl.Sink.foreach(f.apply).toCompletionStage())

  /**
   * A `Sink` that will invoke the given procedure asynchronously for each received element. The sink is materialized
   * into a [[java.util.concurrent.CompletionStage]] which will be completed with `Success` when reaching the
   * normal end of the stream, or completed with `Failure` if there is a failure signaled in
   * the stream.
   */
  def foreachAsync[T](parallelism: Int)(
      f: function.Function[T, CompletionStage[Void]]): Sink[T, CompletionStage[Done]] =
    new Sink(
      scaladsl.Sink
        .foreachAsync(parallelism)((x: T) => f(x).asScala.map(_ => ())(ExecutionContext.parasitic))
        .toCompletionStage())

  /**
   * A `Sink` that when the flow is completed, either through a failure or normal
   * completion, apply the provided function with [[scala.util.Success]]
   * or [[scala.util.Failure]].
   */
  def onComplete[In](callback: function.Procedure[Try[Done]]): Sink[In, NotUsed] =
    new Sink(scaladsl.Sink.onComplete[In](x => callback.apply(x)))

  /**
   * A `Sink` that materializes into a `CompletionStage` of the first value received.
   * If the stream completes before signaling at least a single element, the CompletionStage will be failed with a [[NoSuchElementException]].
   * If the stream signals an error before signaling at least a single element, the CompletionStage will be failed with the streams exception.
   *
   * See also [[headOption]].
   */
  def head[In](): Sink[In, CompletionStage[In]] =
    new Sink(scaladsl.Sink.head[In].toCompletionStage())

  /**
   * A `Sink` that materializes into a `CompletionStage` of the optional first value received.
   * If the stream completes before signaling at least a single element, the value of the CompletionStage will be an empty [[java.util.Optional]].
   * If the stream signals an error errors before signaling at least a single element, the CompletionStage will be failed with the streams exception.
   *
   * See also [[head]].
   */
  def headOption[In](): Sink[In, CompletionStage[Optional[In]]] =
    new Sink(scaladsl.Sink.headOption[In].mapMaterializedValue(_.map(_.toJava)(ExecutionContext.parasitic).asJava))

  /**
   * A `Sink` that materializes into a `CompletionStage` of the last value received.
   * If the stream completes before signaling at least a single element, the CompletionStage will be failed with a [[NoSuchElementException]].
   * If the stream signals an error errors before signaling at least a single element, the CompletionStage will be failed with the streams exception.
   *
   * See also [[lastOption]], [[takeLast]].
   */
  def last[In](): Sink[In, CompletionStage[In]] =
    new Sink(scaladsl.Sink.last[In].toCompletionStage())

  /**
   * A `Sink` that materializes into a `CompletionStage` of the optional last value received.
   * If the stream completes before signaling at least a single element, the value of the CompletionStage will be an empty [[java.util.Optional]].
   * If the stream signals an error errors before signaling at least a single element, the CompletionStage will be failed with the streams exception.
   *
   * See also [[head]], [[takeLast]].
   */
  def lastOption[In](): Sink[In, CompletionStage[Optional[In]]] =
    new Sink(scaladsl.Sink.lastOption[In].mapMaterializedValue(_.map(_.toJava)(ExecutionContext.parasitic).asJava))

  /**
   * A `Sink` that materializes into a a `CompletionStage` of `List<In>` containing the last `n` collected elements.
   *
   * If the stream completes before signaling at least n elements, the `CompletionStage` will complete with all elements seen so far.
   * If the stream never completes the `CompletionStage` will never complete.
   * If there is a failure signaled in the stream the `CompletionStage` will be completed with failure.
   */
  def takeLast[In](n: Int): Sink[In, CompletionStage[java.util.List[In]]] = {
    import scala.jdk.CollectionConverters._
    new Sink(
      scaladsl.Sink
        .takeLast[In](n)
        .mapMaterializedValue(fut => fut.map(sq => sq.asJava)(ExecutionContext.parasitic).asJava))
  }

  /**
   * A `Sink` that keeps on collecting incoming elements until upstream terminates.
   * As upstream may be unbounded, `Flow[T].take` or the stricter `Flow[T].limit` (and their variants)
   * may be used to ensure boundedness.
   * Materializes into a `CompletionStage` of `Seq[T]` containing all the collected elements.
   * `List` is limited to `Integer.MAX_VALUE` elements, this Sink will cancel the stream
   * after having received that many elements.
   *
   * See also [[Flow.limit]], [[Flow.limitWeighted]], [[Flow.take]], [[Flow.takeWithin]], [[Flow.takeWhile]]
   */
  def seq[In]: Sink[In, CompletionStage[java.util.List[In]]] = {
    import scala.jdk.CollectionConverters._
    new Sink(
      scaladsl.Sink.seq[In].mapMaterializedValue(fut => fut.map(sq => sq.asJava)(ExecutionContext.parasitic).asJava))
  }

  /**
   * Sends the elements of the stream to the given `ActorRef`.
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure a [[akka.actor.Status.Failure]]
   * message will be sent to the destination actor.
   *
   * It will request at most `maxInputBufferSize` number of elements from
   * upstream, but there is no back-pressure signal from the destination actor,
   * i.e. if the actor is not consuming the messages fast enough the mailbox
   * of the actor will grow. For potentially slow consumer actors it is recommended
   * to use a bounded mailbox with zero `mailbox-push-timeout-time` or use a rate
   * limiting operator in front of this `Sink`.
   *
   */
  def actorRef[In](ref: ActorRef, onCompleteMessage: Any): Sink[In, NotUsed] =
    new Sink(scaladsl.Sink.actorRef[In](ref, onCompleteMessage, (t: Throwable) => Status.Failure(t)))

  /**
   * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
   * First element is always `onInitMessage`, then stream is waiting for acknowledgement message
   * `ackMessage` from the given actor which means that it is ready to process
   * elements. It also requires `ackMessage` message after each stream element
   * to make backpressure work.
   *
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure - result of `onFailureMessage(throwable)`
   * message will be sent to the destination actor.
   */
  def actorRefWithBackpressure[In](
      ref: ActorRef,
      onInitMessage: Any,
      ackMessage: Any,
      onCompleteMessage: Any,
      onFailureMessage: function.Function[Throwable, Any]): Sink[In, NotUsed] =
    new Sink(
      scaladsl.Sink
        .actorRefWithBackpressure[In](ref, onInitMessage, ackMessage, onCompleteMessage, t => onFailureMessage(t)))

  /**
   * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
   * First element is always `onInitMessage`, then stream is waiting for acknowledgement message
   * from the given actor which means that it is ready to process
   * elements. It also requires an ack message after each stream element
   * to make backpressure work. This variant will consider any message as ack message.
   *
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure - result of `onFailureMessage(throwable)`
   * message will be sent to the destination actor.
   */
  def actorRefWithBackpressure[In](
      ref: ActorRef,
      onInitMessage: Any,
      onCompleteMessage: Any,
      onFailureMessage: function.Function[Throwable, Any]): Sink[In, NotUsed] =
    new Sink(
      scaladsl.Sink.actorRefWithBackpressure[In](ref, onInitMessage, onCompleteMessage, t => onFailureMessage(t)))

  /**
   * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
   * First element is always `onInitMessage`, then stream is waiting for acknowledgement message
   * `ackMessage` from the given actor which means that it is ready to process
   * elements. It also requires `ackMessage` message after each stream element
   * to make backpressure work.
   *
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure - result of `onFailureMessage(throwable)`
   * message will be sent to the destination actor.
   *
   * @deprecated Use actorRefWithBackpressure instead
   */
  @Deprecated
  @deprecated("Use actorRefWithBackpressure instead", "2.6.0")
  def actorRefWithAck[In](
      ref: ActorRef,
      onInitMessage: Any,
      ackMessage: Any,
      onCompleteMessage: Any,
      onFailureMessage: function.Function[Throwable, Any]): Sink[In, NotUsed] =
    new Sink(
      scaladsl.Sink
        .actorRefWithBackpressure[In](ref, onInitMessage, ackMessage, onCompleteMessage, t => onFailureMessage(t)))

  /**
   * A graph with the shape of a sink logically is a sink, this method makes
   * it so also in type.
   */
  def fromGraph[T, M](g: Graph[SinkShape[T], M]): Sink[T, M] =
    g match {
      case s: Sink[T, M] @unchecked => s
      case other                    => new Sink(scaladsl.Sink.fromGraph(other))
    }

  /**
   * Defers the creation of a [[Sink]] until materialization. The `factory` function
   * exposes [[Materializer]] which is going to be used during materialization and
   * [[Attributes]] of the [[Sink]] returned by this method.
   */
  def fromMaterializer[T, M](factory: BiFunction[Materializer, Attributes, Sink[T, M]]): Sink[T, CompletionStage[M]] =
    scaladsl.Sink.fromMaterializer((mat, attr) => factory(mat, attr).asScala).mapMaterializedValue(_.asJava).asJava

  /**
   * Defers the creation of a [[Sink]] until materialization. The `factory` function
   * exposes [[ActorMaterializer]] which is going to be used during materialization and
   * [[Attributes]] of the [[Sink]] returned by this method.
   */
  @deprecated("Use 'fromMaterializer' instead", "2.6.0")
  def setup[T, M](factory: BiFunction[ActorMaterializer, Attributes, Sink[T, M]]): Sink[T, CompletionStage[M]] =
    scaladsl.Sink.setup((mat, attr) => factory(mat, attr).asScala).mapMaterializedValue(_.asJava).asJava

  /**
   * Combine several sinks with fan-out strategy like `Broadcast` or `Balance` and returns `Sink`.
   */
  def combine[T, U](
      output1: Sink[U, _],
      output2: Sink[U, _],
      rest: java.util.List[Sink[U, _]],
      @nowarn
      @deprecatedName(Symbol("strategy"))
      fanOutStrategy: function.Function[java.lang.Integer, Graph[UniformFanOutShape[T, U], NotUsed]])
      : Sink[T, NotUsed] = {
    import scala.jdk.CollectionConverters._
    val seq = if (rest != null) rest.asScala.map(_.asScala).toSeq else immutable.Seq()
    new Sink(scaladsl.Sink.combine(output1.asScala, output2.asScala, seq: _*)(num => fanOutStrategy.apply(num)))
  }

  /**
   * Combine two sinks with fan-out strategy like `Broadcast` or `Balance` and returns `Sink` with 2 outlets.
   */
  def combineMat[T, U, M1, M2, M](
      first: Sink[U, M1],
      second: Sink[U, M2],
      fanOutStrategy: function.Function[java.lang.Integer, Graph[UniformFanOutShape[T, U], NotUsed]],
      matF: function.Function2[M1, M2, M]): Sink[T, M] = {
    new Sink(
      scaladsl.Sink.combineMat(first.asScala, second.asScala)(size => fanOutStrategy(size))(combinerToScala(matF)))
  }

  /**
   * Combine several sinks with fan-out strategy like `Broadcast` or `Balance` and returns `Sink`.
   * The fanoutGraph's outlets size must match the provides sinks'.
   */
  def combine[T, U, M](
      sinks: java.util.List[_ <: Graph[SinkShape[U], M]],
      fanOutStrategy: function.Function[java.lang.Integer, Graph[UniformFanOutShape[T, U], NotUsed]])
      : Sink[T, java.util.List[M]] = {
    val seq = if (sinks != null) sinks.asScala.collect {
      case sink: Sink[U @unchecked, M @unchecked] => sink.asScala
      case other                                  => other
    }.toSeq
    else immutable.Seq()
    import scala.jdk.CollectionConverters._
    new Sink(scaladsl.Sink.combine(seq)(size => fanOutStrategy(size)).mapMaterializedValue(_.asJava))
  }

  /**
   * Creates a `Sink` that is materialized as an [[akka.stream.javadsl.SinkQueueWithCancel]].
   * [[akka.stream.javadsl.SinkQueueWithCancel.pull]] method is pulling element from the stream and returns ``CompletionStage[Option[T]]``.
   * `CompletionStage` completes when element is available.
   *
   * Before calling pull method second time you need to ensure that number of pending pulls is less then ``maxConcurrentPulls``
   * or wait until some of the previous Futures completes.
   * Pull returns Failed future with ''IllegalStateException'' if there will be more then ``maxConcurrentPulls`` number of pending pulls.
   *
   * `Sink` will request at most number of elements equal to size of `inputBuffer` from
   * upstream and then stop back pressure.  You can configure size of input
   * buffer by using [[Sink.withAttributes]] method.
   *
   * For stream completion you need to pull all elements from [[akka.stream.javadsl.SinkQueueWithCancel]] including last None
   * as completion marker
   *
   * @see [[akka.stream.javadsl.SinkQueueWithCancel]]
   */
  def queue[T](maxConcurrentPulls: Int): Sink[T, SinkQueueWithCancel[T]] =
    new Sink(scaladsl.Sink.queue[T](maxConcurrentPulls).mapMaterializedValue(_.asJava))

  /**
   * Creates a `Sink` that is materialized as an [[akka.stream.javadsl.SinkQueueWithCancel]].
   * [[akka.stream.javadsl.SinkQueueWithCancel.pull]] method is pulling element from the stream and returns ``CompletionStage[Option[T]]``.
   * `CompletionStage` completes when element is available.
   *
   * Before calling pull method second time you need to wait until previous CompletionStage completes.
   * Pull returns Failed future with ''IllegalStateException'' if previous future has not yet completed.
   *
   * `Sink` will request at most number of elements equal to size of `inputBuffer` from
   * upstream and then stop back pressure.  You can configure size of input
   * buffer by using [[Sink.withAttributes]] method.
   *
   * For stream completion you need to pull all elements from [[akka.stream.javadsl.SinkQueueWithCancel]] including last None
   * as completion marker
   *
   * @see [[akka.stream.javadsl.SinkQueueWithCancel]]
   */
  def queue[T](): Sink[T, SinkQueueWithCancel[T]] = queue(1)

  /**
   * Creates a real `Sink` upon receiving the first element. Internal `Sink` will not be created if there are no elements,
   * because of completion or error.
   *
   * If upstream completes before an element was received then the `Future` is completed with the value created by fallback.
   * If upstream fails before an element was received, `sinkFactory` throws an exception, or materialization of the internal
   * sink fails then the `Future` is completed with the exception.
   * Otherwise the `Future` is completed with the materialized value of the internal sink.
   */
  @deprecated("Use 'Sink.lazyCompletionStageSink' in combination with 'Flow.prefixAndTail(1)' instead", "2.6.0")
  def lazyInit[T, M](
      sinkFactory: function.Function[T, CompletionStage[Sink[T, M]]],
      fallback: function.Creator[M]): Sink[T, CompletionStage[M]] =
    new Sink(
      scaladsl.Sink
        .lazyInit[T, M](
          t => sinkFactory.apply(t).asScala.map(_.asScala)(ExecutionContext.parasitic),
          () => fallback.create())
        .mapMaterializedValue(_.asJava))

  /**
   * Creates a real `Sink` upon receiving the first element. Internal `Sink` will not be created if there are no elements,
   * because of completion or error.
   *
   * If upstream completes before an element was received then the `Future` is completed with `None`.
   * If upstream fails before an element was received, `sinkFactory` throws an exception, or materialization of the internal
   * sink fails then the `Future` is completed with the exception.
   * Otherwise the `Future` is completed with the materialized value of the internal sink.
   */
  @deprecated("Use 'Sink.lazyCompletionStageSink' instead", "2.6.0")
  def lazyInitAsync[T, M](
      sinkFactory: function.Creator[CompletionStage[Sink[T, M]]]): Sink[T, CompletionStage[Optional[M]]] = {
    val sSink = scaladsl.Sink
      .lazyInitAsync[T, M](() => sinkFactory.create().asScala.map(_.asScala)(ExecutionContext.parasitic))
      .mapMaterializedValue(fut =>
        fut.map(_.fold(Optional.empty[M]())(m => Optional.ofNullable(m)))(ExecutionContext.parasitic).asJava)
    new Sink(sSink)
  }

  /**
   * Turn a `Future[Sink]` into a Sink that will consume the values of the source when the future completes successfully.
   * If the `Future` is completed with a failure the stream is failed.
   *
   * The materialized future value is completed with the materialized value of the future sink or failed with a
   * [[NeverMaterializedException]] if upstream fails or downstream cancels before the future has completed.
   */
  def completionStageSink[T, M](future: CompletionStage[Sink[T, M]]): Sink[T, CompletionStage[M]] =
    lazyCompletionStageSink[T, M](() => future)

  /**
   * Defers invoking the `create` function to create a sink until there is a first element passed from upstream.
   *
   * The materialized future value is completed with the materialized value of the created sink when that has successfully
   * been materialized.
   *
   * If the `create` function throws or returns or the stream fails to materialize, in this
   * case the materialized future value is failed with a [[akka.stream.NeverMaterializedException]].
   */
  def lazySink[T, M](create: Creator[Sink[T, M]]): Sink[T, CompletionStage[M]] =
    lazyCompletionStageSink(() => CompletableFuture.completedFuture(create.create()))

  /**
   * Defers invoking the `create` function to create a future sink until there is a first element passed from upstream.
   *
   * The materialized future value is completed with the materialized value of the created sink when that has successfully
   * been materialized.
   *
   * If the `create` function throws or returns a future that is failed, or the stream fails to materialize, in this
   * case the materialized future value is failed with a [[akka.stream.NeverMaterializedException]].
   */
  def lazyCompletionStageSink[T, M](create: Creator[CompletionStage[Sink[T, M]]]): Sink[T, CompletionStage[M]] =
    new Sink(scaladsl.Sink.lazyFutureSink { () =>
      create.create().asScala.map(_.asScala)((ExecutionContext.parasitic))
    }).mapMaterializedValue(_.asJava)
}

/**
 * Java API
 *
 * A `Sink` is a set of stream processing steps that has one open input.
 * Can be used as a `Subscriber`
 */
final class Sink[In, Mat](delegate: scaladsl.Sink[In, Mat]) extends Graph[SinkShape[In], Mat] {

  override def shape: SinkShape[In] = delegate.shape
  override def traversalBuilder: LinearTraversalBuilder = delegate.traversalBuilder

  override def toString: String = delegate.toString

  /**
   * Converts this Sink to its Scala DSL counterpart.
   */
  def asScala: scaladsl.Sink[In, Mat] = delegate

  /**
   * Connect this `Sink` to a `Source` and run it.
   *
   * Note that the `ActorSystem` can be used as the `systemProvider` parameter.
   */
  def runWith[M](source: Graph[SourceShape[In], M], systemProvider: ClassicActorSystemProvider): M =
    asScala.runWith(source)(SystemMaterializer(systemProvider.classicSystem).materializer)

  /**
   * Connect this `Sink` to a `Source` and run it.
   */
  def runWith[M](source: Graph[SourceShape[In], M], materializer: Materializer): M =
    asScala.runWith(source)(materializer)

  /**
   * Transform this Sink by applying a function to each *incoming* upstream element before
   * it is passed to the [[Sink]]
   *
   * '''Backpressures when''' original [[Sink]] backpressures
   *
   * '''Cancels when''' original [[Sink]] backpressures
   */
  def contramap[In2](f: function.Function[In2, In]): Sink[In2, Mat] =
    javadsl.Flow.fromFunction(f).toMat(this, Keep.right[NotUsed, Mat])

  /**
   * Transform only the materialized value of this Sink, leaving all other properties as they were.
   */
  def mapMaterializedValue[Mat2](f: function.Function[Mat, Mat2]): Sink[In, Mat2] =
    new Sink(delegate.mapMaterializedValue(f.apply _))

  /**
   * Materializes this Sink, immediately returning (1) its materialized value, and (2) a new Sink
   * that can be consume elements 'into' the pre-materialized one.
   *
   * Useful for when you need a materialized value of a Sink when handing it out to someone to materialize it for you.
   *
   * Note that the `ActorSystem` can be used as the `systemProvider` parameter.
   *
   * Note that `preMaterialize` is implemented through a reactive streams `Subscriber` which means that a buffer is introduced
   * and that errors are not propagated upstream but are turned into cancellations without error details.
   */
  def preMaterialize(systemProvider: ClassicActorSystemProvider)
      : japi.Pair[Mat @uncheckedVariance, Sink[In @uncheckedVariance, NotUsed]] = {
    val (mat, sink) = delegate.preMaterialize()(SystemMaterializer(systemProvider.classicSystem).materializer)
    akka.japi.Pair(mat, sink.asJava)
  }

  /**
   * Materializes this Sink, immediately returning (1) its materialized value, and (2) a new Sink
   * that can be consume elements 'into' the pre-materialized one.
   *
   * Useful for when you need a materialized value of a Sink when handing it out to someone to materialize it for you.
   *
   * Prefer the method taking an ActorSystem unless you have special requirements.
   */
  def preMaterialize(
      materializer: Materializer): japi.Pair[Mat @uncheckedVariance, Sink[In @uncheckedVariance, NotUsed]] = {
    val (mat, sink) = delegate.preMaterialize()(materializer)
    akka.japi.Pair(mat, sink.asJava)
  }

  /**
   * Replace the attributes of this [[Sink]] with the given ones. If this Sink is a composite
   * of multiple graphs, new attributes on the composite will be less specific than attributes
   * set directly on the individual graphs of the composite.
   */
  override def withAttributes(attr: Attributes): javadsl.Sink[In, Mat] =
    new Sink(delegate.withAttributes(attr))

  /**
   * Add the given attributes to this [[Sink]]. If the specific attribute was already present
   * on this graph this means the added attribute will be more specific than the existing one.
   * If this Sink is a composite of multiple graphs, new attributes on the composite will be
   * less specific than attributes set directly on the individual graphs of the composite.
   */
  override def addAttributes(attr: Attributes): javadsl.Sink[In, Mat] =
    new Sink(delegate.addAttributes(attr))

  /**
   * Add a ``name`` attribute to this Sink.
   */
  override def named(name: String): javadsl.Sink[In, Mat] =
    new Sink(delegate.named(name))

  /**
   * Put an asynchronous boundary around this `Sink`
   */
  override def async: javadsl.Sink[In, Mat] =
    new Sink(delegate.async)

  /**
   * Put an asynchronous boundary around this `Sink`
   *
   * @param dispatcher Run the graph on this dispatcher
   */
  override def async(dispatcher: String): javadsl.Sink[In, Mat] =
    new Sink(delegate.async(dispatcher))

  /**
   * Put an asynchronous boundary around this `Sink`
   *
   * @param dispatcher      Run the graph on this dispatcher
   * @param inputBufferSize Set the input buffer to this size for the graph
   */
  override def async(dispatcher: String, inputBufferSize: Int): javadsl.Sink[In, Mat] =
    new Sink(delegate.async(dispatcher, inputBufferSize))

  override def getAttributes: Attributes = delegate.getAttributes

}
