/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.event.{ LogMarker, LoggingAdapter, MarkerLoggingAdapter }
import akka.stream._
import akka.stream.impl.Throttle
import akka.util.ConstantFun

/**
 * Shared stream operations for [[FlowWithContext]] and [[SourceWithContext]] that automatically propagate a context
 * element with each data element.
 *
 */
trait FlowWithContextOps[+Out, +Ctx, +Mat] {
  type ReprMat[+O, +C, +M] <: FlowWithContextOps[O, C, M] {
    type ReprMat[+OO, +CC, +MatMat] = FlowWithContextOps.this.ReprMat[OO, CC, MatMat]
  }
  type Repr[+O, +C] = ReprMat[O, C, Mat @uncheckedVariance]

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
   * @see [[akka.stream.scaladsl.FlowOps.via]]
   */
  def via[Out2, Ctx2, Mat2](flow: Graph[FlowShape[(Out, Ctx), (Out2, Ctx2)], Mat2]): Repr[Out2, Ctx2]

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
  @ApiMayChange def unsafeDataVia[Out2, Mat2](viaFlow: Graph[FlowShape[Out, Out2], Mat2]): Repr[Out2, Ctx]

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
   * The `combine` function is used to compose the materialized values of this flow and that
   * flow into the materialized value of the resulting Flow.
   *
   * @see [[akka.stream.scaladsl.FlowOpsMat.viaMat]]
   */
  def viaMat[Out2, Ctx2, Mat2, Mat3](flow: Graph[FlowShape[(Out, Ctx), (Out2, Ctx2)], Mat2])(
      combine: (Mat, Mat2) => Mat3): ReprMat[Out2, Ctx2, Mat3]

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.map]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.map]]
   */
  def map[Out2](f: Out => Out2): Repr[Out2, Ctx] =
    via(flow.map { case (e, ctx) => (f(e), ctx) })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.mapError]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.mapError]]
   */
  def mapError(pf: PartialFunction[Throwable, Throwable]): Repr[Out, Ctx] =
    via(flow.mapError(pf))

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.mapAsync]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.mapAsync]]
   */
  def mapAsync[Out2](parallelism: Int)(f: Out => Future[Out2]): Repr[Out2, Ctx] =
    via(flow.mapAsync(parallelism) {
      case (e, ctx) => f(e).map(o => (o, ctx))(ExecutionContext.parasitic)
    })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.mapAsyncPartitioned]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.mapAsyncPartitioned]]
   */
  def mapAsyncPartitioned[Out2, P](parallelism: Int, perPartition: Int)(partitioner: Out => P)(
      f: (Out, P) => Future[Out2]): Repr[Out2, Ctx] = {
    val pairPartitioner = { (pair: (Out, Ctx)) =>
      partitioner(pair._1)
    }
    val pairF = { (pair: (Out, Ctx), partition: P) =>
      val (elem, context) = pair
      f(elem, partition).map(_ -> context)(ExecutionContext.parasitic)
    }

    via(flow.mapAsyncPartitioned(parallelism, perPartition)(pairPartitioner)(pairF))
  }

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.collect]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.scaladsl.FlowOps.collect]]
   */
  def collect[Out2](f: PartialFunction[Out, Out2]): Repr[Out2, Ctx] =
    via(flow.collect {
      case (e, ctx) if f.isDefinedAt(e) => (f(e), ctx)
    })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.filter]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.scaladsl.FlowOps.filter]]
   */
  def filter(pred: Out => Boolean): Repr[Out, Ctx] =
    collect { case e if pred(e) => e }

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.filterNot]].
   *
   * Note, that the context of elements that are filtered out is skipped as well.
   *
   * @see [[akka.stream.scaladsl.FlowOps.filterNot]]
   */
  def filterNot(pred: Out => Boolean): Repr[Out, Ctx] =
    collect { case e if !pred(e) => e }

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.grouped]].
   *
   * Each output group will be associated with a `Seq` of corresponding context elements.
   *
   * @see [[akka.stream.scaladsl.FlowOps.grouped]]
   */
  def grouped(n: Int): Repr[immutable.Seq[Out], immutable.Seq[Ctx]] =
    via(flow.grouped(n).map { elsWithContext =>
      val (els, ctxs) = elsWithContext.unzip
      (els, ctxs)
    })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.sliding]].
   *
   * Each output group will be associated with a `Seq` of corresponding context elements.
   *
   * @see [[akka.stream.scaladsl.FlowOps.sliding]]
   */
  def sliding(n: Int, step: Int = 1): Repr[immutable.Seq[Out], immutable.Seq[Ctx]] =
    via(flow.sliding(n, step).map { elsWithContext =>
      val (els, ctxs) = elsWithContext.unzip
      (els, ctxs)
    })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.mapConcat]].
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
   * @see [[akka.stream.scaladsl.FlowOps.mapConcat]]
   */
  def mapConcat[Out2](f: Out => IterableOnce[Out2]): Repr[Out2, Ctx] =
    via(flow.mapConcat {
      case (e, ctx) => f(e).iterator.map(_ -> ctx)
    })

  /**
   * Apply the given function to each context element (leaving the data elements unchanged).
   */
  def mapContext[Ctx2](f: Ctx => Ctx2): Repr[Out, Ctx2] =
    via(flow.map { case (e, ctx) => (e, f(ctx)) })

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.log]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.log]]
   */
  def log(name: String, extract: Out => Any = ConstantFun.scalaIdentityFunction)(
      implicit log: LoggingAdapter = null): Repr[Out, Ctx] = {
    val extractWithContext: ((Out, Ctx)) => Any = { case (e, _) => extract(e) }
    via(flow.log(name, extractWithContext)(log))
  }

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.logWithMarker]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.logWithMarker]]
   */
  def logWithMarker(
      name: String,
      marker: (Out, Ctx) => LogMarker,
      extract: Out => Any = ConstantFun.scalaIdentityFunction)(
      implicit log: MarkerLoggingAdapter = null): Repr[Out, Ctx] = {
    val extractWithContext: ((Out, Ctx)) => Any = { case (e, _) => extract(e) }
    via(flow.logWithMarker(name, marker.tupled, extractWithContext)(log))
  }

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(elements: Int, per: FiniteDuration): Repr[Out, Ctx] =
    throttle(elements, per, Throttle.AutomaticMaximumBurst, ConstantFun.oneInt, ThrottleMode.Shaping)

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(elements: Int, per: FiniteDuration, maximumBurst: Int, mode: ThrottleMode): Repr[Out, Ctx] =
    throttle(elements, per, maximumBurst, ConstantFun.oneInt, mode)

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(cost: Int, per: FiniteDuration, costCalculation: (Out) => Int): Repr[Out, Ctx] =
    throttle(cost, per, Throttle.AutomaticMaximumBurst, costCalculation, ThrottleMode.Shaping)

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(
      cost: Int,
      per: FiniteDuration,
      maximumBurst: Int,
      costCalculation: (Out) => Int,
      mode: ThrottleMode): Repr[Out, Ctx] =
    via(flow.throttle(cost, per, maximumBurst, a => costCalculation(a._1), mode))

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(control: ThrottleControl): Repr[Out, Ctx] =
    via(flow.throttle(control))

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.FlowOps.throttle]].
   *
   * @see [[akka.stream.scaladsl.FlowOps.throttle]]
   */
  def throttle(control: ThrottleControl, costCalculation: (Out) => Int): Repr[Out, Ctx] =
    via(flow.throttle(control, a => costCalculation(a._1)))

  private[akka] def flow[T, C]: Flow[(T, C), (T, C), NotUsed] = Flow[(T, C)]
}
