/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.unchecked.uncheckedVariance

import akka.NotUsed
import akka.japi.Pair
import akka.stream._

object FlowWithContext {

  /**
   * Creates an "empty" FlowWithContext that passes elements through with their context unchanged.
   */
  def apply[In, Ctx]: FlowWithContext[In, Ctx, In, Ctx, akka.NotUsed] = {
    val under = Flow[(In, Ctx)]
    new FlowWithContext[In, Ctx, In, Ctx, akka.NotUsed](under)
  }

  /**
   * Creates a FlowWithContext from a regular flow that operates on a tuple of `(data, context)` elements.
   */
  def fromTuples[In, CtxIn, Out, CtxOut, Mat](
      flow: Flow[(In, CtxIn), (Out, CtxOut), Mat]): FlowWithContext[In, CtxIn, Out, CtxOut, Mat] =
    new FlowWithContext(flow)
}

/**
 * A flow that provides operations which automatically propagate the context of an element.
 * Only a subset of common operations from [[FlowOps]] is supported. As an escape hatch you can
 * use [[FlowWithContextOps.via]] to manually provide the context propagation for otherwise unsupported
 * operations.
 *
 * An "empty" flow can be created by calling `FlowWithContext[Ctx, T]`.
 *
 */
final class FlowWithContext[-In, -CtxIn, +Out, +CtxOut, +Mat](delegate: Flow[(In, CtxIn), (Out, CtxOut), Mat])
    extends GraphDelegate(delegate)
    with FlowWithContextOps[Out, CtxOut, Mat] {
  override type ReprMat[+O, +C, +M] =
    FlowWithContext[In @uncheckedVariance, CtxIn @uncheckedVariance, O, C, M @uncheckedVariance]

  override def via[Out2, Ctx2, Mat2](viaFlow: Graph[FlowShape[(Out, CtxOut), (Out2, Ctx2)], Mat2]): Repr[Out2, Ctx2] =
    new FlowWithContext(delegate.via(viaFlow))

  override def unsafeDataVia[Out2, Mat2](viaFlow: Graph[FlowShape[Out, Out2], Mat2]): Repr[Out2, CtxOut] =
    FlowWithContext.fromTuples(Flow.fromGraph(GraphDSL.createGraph(delegate) { implicit b => d =>
      import GraphDSL.Implicits._

      val unzip = b.add(Unzip[Out, CtxOut]())
      val zipper = b.add(Zip[Out2, CtxOut]())

      d ~> unzip.in

      unzip.out0.via(viaFlow) ~> zipper.in0
      unzip.out1 ~> zipper.in1

      FlowShape(d.in, zipper.out)
    }))

  override def viaMat[Out2, Ctx2, Mat2, Mat3](flow: Graph[FlowShape[(Out, CtxOut), (Out2, Ctx2)], Mat2])(
      combine: (Mat, Mat2) => Mat3): FlowWithContext[In, CtxIn, Out2, Ctx2, Mat3] =
    new FlowWithContext(delegate.viaMat(flow)(combine))

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.Flow.withAttributes]].
   *
   * @see [[akka.stream.scaladsl.Flow.withAttributes]]
   */
  override def withAttributes(attr: Attributes): FlowWithContext[In, CtxIn, Out, CtxOut, Mat] =
    new FlowWithContext(delegate.withAttributes(attr))

  /**
   * Context-preserving variant of [[akka.stream.scaladsl.Flow.mapMaterializedValue]].
   *
   * @see [[akka.stream.scaladsl.Flow.mapMaterializedValue]]
   */
  def mapMaterializedValue[Mat2](f: Mat => Mat2): FlowWithContext[In, CtxIn, Out, CtxOut, Mat2] =
    new FlowWithContext(delegate.mapMaterializedValue(f))

  def asFlow: Flow[(In, CtxIn), (Out, CtxOut), Mat] = delegate

  def asJava[JIn <: In, JCtxIn <: CtxIn, JOut >: Out, JCtxOut >: CtxOut, JMat >: Mat]
      : javadsl.FlowWithContext[JIn, JCtxIn, JOut, JCtxOut, JMat] =
    new javadsl.FlowWithContext(
      javadsl.Flow
        .create[Pair[JIn, JCtxIn]]()
        .map(_.toScala)
        .viaMat(delegate.map {
          case (first, second) =>
            Pair[JOut, JCtxOut](first, second)
        }.asJava, javadsl.Keep.right[NotUsed, JMat]))
}
