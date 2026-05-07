/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import scala.annotation.unchecked.uncheckedVariance
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.event.{ LogMarker, LoggingAdapter, MarkerLoggingAdapter }
import akka.japi.Pair
import akka.japi.function
import akka.stream._
import akka.util.ConstantFun
import scala.jdk.CollectionConverters._

object SourceWithContext {

  /**
   * Creates a SourceWithContext from a regular flow that operates on `Pair<data, context>` elements.
   */
  def fromPairs[Out, CtxOut, Mat](under: Source[Pair[Out, CtxOut], Mat]): SourceWithContext[Out, CtxOut, Mat] = {
    new SourceWithContext(scaladsl.SourceWithContext.fromTuples(under.asScala.map(_.toScala)))
  }
}

/**
 * A source that provides operations which automatically propagate the context of an element.
 * Only a subset of common operations from [[Source]] is supported. As an escape hatch you can
 * use [[SourceWithContext#via]] to manually provide the context propagation for otherwise unsupported
 * operations.
 *
 * Can be created by calling [[Source.asSourceWithContext]]
 */
final class SourceWithContext[Out, Ctx, +Mat](delegate: scaladsl.SourceWithContext[Out, Ctx, Mat])
    extends GraphDelegate(delegate) {

  /**
   * Transform this flow by the regular flow. The given flow must support manual context propagation by
   * taking and producing tuples of (data, context).
   *
   *  It is up to the implementer to ensure the inner flow does not exhibit any behaviour that is not expected
   *  by the downstream elements, such as reordering. For more background on these requirements
   *  see https://doc.akka.io/libraries/akka-core/current/stream/stream-context.html.
   *
   * This can be used as an escape hatch for operations that are not (yet) provided with automatic
   * context propagation here.
   *
   * @see [[akka.stream.javadsl.Flow.via]]
   */
  def via[Out2, Ctx2, Mat2](
      viaFlow: Graph[FlowShape[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance], Pair[Out2, Ctx2]], Mat2])
      : SourceWithContext[Out2, Ctx2, Mat] =
    viaScala(_.via(akka.stream.scaladsl.Flow[(Out, Ctx)].map { case (o, c) => Pair(o, c) }.via(viaFlow).map(_.toScala)))

  /**
   * Transform this flow by the regular flow. The given flow works on the data portion of the stream and
   * ignores the context.
   *
   * The given flow *must* not re-order, drop or emit multiple elements for one incoming
   * element, the sequence of incoming contexts is re-combined with the outgoing
   * elements of the stream. If a flow not fulfilling this requirement is used the stream
   * will not fail but continue running in a corrupt state and re-combine incorrect pairs
   * of elements and contexts or deadlock.
   *
   * For more background on these requirements
   *  see https://doc.akka.io/libraries/akka-core/current/stream/stream-context.html.
   */
  @ApiMayChange def unsafeDataVia[Out2, Mat2](
      viaFlow: Graph[FlowShape[Out @uncheckedVariance, Out2], Mat2]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.unsafeDataVia(viaFlow))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.withAttributes]].
   *
   * @see [[akka.stream.javadsl.Source.withAttributes]]
   */
  override def withAttributes(attr: Attributes): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.withAttributes(attr))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.mapError]].
   *
   * @see [[akka.stream.javadsl.Source.mapError]]
   */
  def mapError(pf: PartialFunction[Throwable, Throwable]): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.mapError(pf))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.mapMaterializedValue]].
   *
   * @see [[akka.stream.javadsl.Flow.mapMaterializedValue]]
   */
  def mapMaterializedValue[Mat2](f: function.Function[Mat, Mat2]): SourceWithContext[Out, Ctx, Mat2] =
    viaScala(_.mapMaterializedValue(f.apply _))

  /**
   * Stops automatic context propagation from here and converts this to a regular
   * stream of a pair of (data, context).
   */
  def asSource(): Source[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance], Mat @uncheckedVariance] =
    delegate.asSource.map { case (o, c) => Pair(o, c) }.asJava

  // remaining operations in alphabetic order

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.collect]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.javadsl.Source.collect]]
   */
  def collect[Out2](pf: PartialFunction[Out, Out2]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.collect(pf))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.filter]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.javadsl.Source.filter]]
   */
  def filter(p: function.Predicate[Out]): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.filter(p.test))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.filterNot]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.javadsl.Source.filterNot]]
   */
  def filterNot(p: function.Predicate[Out]): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.filterNot(p.test))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.grouped]].
   *
   * Each output group will be associated with a `Seq` of corresponding context elements.
   *
   * @see [[akka.stream.javadsl.Source.grouped]]
   */
  def grouped(
      n: Int): SourceWithContext[java.util.List[Out @uncheckedVariance], java.util.List[Ctx @uncheckedVariance], Mat] =
    viaScala(_.grouped(n).map(_.asJava).mapContext(_.asJava))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.map]].
   *
   * @see [[akka.stream.javadsl.Source.map]]
   */
  def map[Out2](f: function.Function[Out, Out2]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.map(f.apply))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.mapAsync]].
   *
   * @see [[akka.stream.javadsl.Source.mapAsync]]
   */
  def mapAsync[Out2](
      parallelism: Int,
      f: function.Function[Out, CompletionStage[Out2]]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.mapAsync[Out2](parallelism)(o => f.apply(o).asScala))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.mapAsyncPartitioned]].
   *
   * @see [[akka.stream.javadsl.Source.mapAsyncPartitioned]]
   */
  def mapAsyncPartitioned[Out2, P](
      parallelism: Int,
      perPartition: Int,
      partitioner: function.Function[Out, P],
      f: BiFunction[Out, P, CompletionStage[Out2]]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.mapAsyncPartitioned[Out2, P](parallelism, perPartition)(x => partitioner(x)) { (x, p) =>
      f(x, p).asScala
    })

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.mapConcat]].
   *
   * The context of the input element will be associated with each of the output elements calculated from
   * this input element.
   *
   * Example:
   *
   * ```
   * def dup(element: String) = Seq(element, element)
   *
   * Input:
   *
   * ("a", 1)
   * ("b", 2)
   *
   * inputElements.mapConcat(dup)
   *
   * Output:
   *
   * ("a", 1)
   * ("a", 1)
   * ("b", 2)
   * ("b", 2)
   * ```
   *
   * @see [[akka.stream.javadsl.Source.mapConcat]]
   */
  def mapConcat[Out2](f: function.Function[Out, _ <: java.lang.Iterable[Out2]]): SourceWithContext[Out2, Ctx, Mat] =
    viaScala(_.mapConcat(elem => f.apply(elem).asScala))

  /**
   * Apply the given function to each context element (leaving the data elements unchanged).
   */
  def mapContext[Ctx2](extractContext: function.Function[Ctx, Ctx2]): SourceWithContext[Out, Ctx2, Mat] =
    viaScala(_.mapContext(extractContext.apply))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.sliding]].
   *
   * Each output group will be associated with a `Seq` of corresponding context elements.
   *
   * @see [[akka.stream.javadsl.Source.sliding]]
   */
  def sliding(n: Int, step: Int = 1)
      : SourceWithContext[java.util.List[Out @uncheckedVariance], java.util.List[Ctx @uncheckedVariance], Mat] =
    viaScala(_.sliding(n, step).map(_.asJava).mapContext(_.asJava))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.log]].
   *
   * @see [[akka.stream.javadsl.Source.log]]
   */
  def log(name: String, extract: function.Function[Out, Any], log: LoggingAdapter): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.log(name, e => extract.apply(e))(log))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.log]].
   *
   * @see [[akka.stream.javadsl.Flow.log]]
   */
  def log(name: String, extract: function.Function[Out, Any]): SourceWithContext[Out, Ctx, Mat] =
    this.log(name, extract, null)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.log]].
   *
   * @see [[akka.stream.javadsl.Flow.log]]
   */
  def log(name: String, log: LoggingAdapter): SourceWithContext[Out, Ctx, Mat] =
    this.log(name, ConstantFun.javaIdentityFunction[Out], log)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.log]].
   *
   * @see [[akka.stream.javadsl.Flow.log]]
   */
  def log(name: String): SourceWithContext[Out, Ctx, Mat] =
    this.log(name, ConstantFun.javaIdentityFunction[Out], null)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.logWithMarker]].
   *
   * @see [[akka.stream.javadsl.Source.logWithMarker]]
   */
  def logWithMarker(
      name: String,
      marker: function.Function2[Out, Ctx, LogMarker],
      extract: function.Function[Out, Any],
      log: MarkerLoggingAdapter): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.logWithMarker(name, (e, c) => marker.apply(e, c), e => extract.apply(e))(log))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.logWithMarker]].,
   *
   * @see [[akka.stream.javadsl.Flow.logWithMarker]]
   */
  def logWithMarker(
      name: String,
      marker: function.Function2[Out, Ctx, LogMarker],
      extract: function.Function[Out, Any]): SourceWithContext[Out, Ctx, Mat] =
    this.logWithMarker(name, marker, extract, null)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.logWithMarker]].
   *
   * @see [[akka.stream.javadsl.Flow.logWithMarker]]
   */
  def logWithMarker(
      name: String,
      marker: function.Function2[Out, Ctx, LogMarker],
      log: MarkerLoggingAdapter): SourceWithContext[Out, Ctx, Mat] =
    this.logWithMarker(name, marker, ConstantFun.javaIdentityFunction[Out], log)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Flow.logWithMarker]].
   *
   * @see [[akka.stream.javadsl.Flow.logWithMarker]]
   */
  def logWithMarker(name: String, marker: function.Function2[Out, Ctx, LogMarker]): SourceWithContext[Out, Ctx, Mat] =
    this.logWithMarker(name, marker, ConstantFun.javaIdentityFunction[Out], null)

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(elements: Int, per: java.time.Duration): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(elements, per.toScala))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(
      elements: Int,
      per: java.time.Duration,
      maximumBurst: Int,
      mode: ThrottleMode): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(elements, per.toScala, maximumBurst, mode))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(
      cost: Int,
      per: java.time.Duration,
      costCalculation: function.Function[Out, Integer]): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(cost, per.toScala, costCalculation.apply))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(
      cost: Int,
      per: java.time.Duration,
      maximumBurst: Int,
      costCalculation: function.Function[Out, Integer],
      mode: ThrottleMode): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(cost, per.toScala, maximumBurst, costCalculation.apply, mode))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(control: ThrottleControl): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(control.asScala))

  /**
   * Context-preserving variant of [[akka.stream.javadsl.Source.throttle]].
   *
   * @see [[akka.stream.javadsl.Source.throttle]]
   */
  def throttle(
      control: ThrottleControl,
      costCalculation: function.Function[Out, Integer]): SourceWithContext[Out, Ctx, Mat] =
    viaScala(_.throttle(control.asScala, elem => costCalculation.apply(elem).intValue()))

  /**
   * Connect this [[akka.stream.javadsl.SourceWithContext]] to a [[akka.stream.javadsl.Sink]],
   * concatenating the processing steps of both.
   */
  def to[Mat2](
      sink: Graph[SinkShape[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance]], Mat2]): javadsl.RunnableGraph[Mat] =
    RunnableGraph.fromGraph(asScala.asSource.map { case (o, e) => Pair(o, e) }.to(sink))

  /**
   * Connect this [[akka.stream.javadsl.SourceWithContext]] to a [[akka.stream.javadsl.Sink]],
   * concatenating the processing steps of both.
   */
  def toMat[Mat2, Mat3](
      sink: Graph[SinkShape[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance]], Mat2],
      combine: function.Function2[Mat, Mat2, Mat3]): javadsl.RunnableGraph[Mat3] =
    RunnableGraph.fromGraph(asScala.asSource.map { case (o, e) => Pair(o, e) }.toMat(sink)(combinerToScala(combine)))

  /**
   * Connect this [[akka.stream.javadsl.SourceWithContext]] to a [[akka.stream.javadsl.Sink]] and run it.
   * The returned value is the materialized value of the `Sink`.
   */
  def runWith[M](
      sink: Graph[SinkShape[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance]], M],
      systemProvider: ClassicActorSystemProvider): M =
    toMat(sink, Keep.right[Mat, M]).run(systemProvider.classicSystem)

  /**
   * Connect this [[akka.stream.javadsl.SourceWithContext]] to a [[akka.stream.javadsl.Sink]] and run it.
   * The returned value is the materialized value of the `Sink`.
   *
   * Prefer the method taking an ActorSystem unless you have special requirements.
   */
  def runWith[M](
      sink: Graph[SinkShape[Pair[Out @uncheckedVariance, Ctx @uncheckedVariance]], M],
      materializer: Materializer): M =
    toMat(sink, Keep.right[Mat, M]).run(materializer)

  def asScala: scaladsl.SourceWithContext[Out, Ctx, Mat] = delegate

  private[this] def viaScala[Out2, Ctx2, Mat2](
      f: scaladsl.SourceWithContext[Out, Ctx, Mat] => scaladsl.SourceWithContext[Out2, Ctx2, Mat2])
      : SourceWithContext[Out2, Ctx2, Mat2] =
    new SourceWithContext(f(delegate))
}
