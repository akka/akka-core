/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.scaladsl._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SubFlowImpl {
  trait MergeBack[In, F[+_]] {
    def apply[T](f: Flow[In, T, NotUsed], breadth: Int): F[T]
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SubFlowImpl[In, Out, Mat, F[+_], C](
    val subFlow: Flow[In, Out, NotUsed],
    mergeBackFunction: SubFlowImpl.MergeBack[In, F],
    finishFunction: Sink[In, NotUsed] => C)
    extends SubFlow[Out, Mat, F, C] {

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T] =
    new SubFlowImpl[In, T, Mat, F, C](subFlow.via(flow), mergeBackFunction, finishFunction)

  override def withAttributes(attr: Attributes): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C](subFlow.withAttributes(attr), mergeBackFunction, finishFunction)

  override def addAttributes(attr: Attributes): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C](subFlow.addAttributes(attr), mergeBackFunction, finishFunction)

  override def named(name: String): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C](subFlow.named(name), mergeBackFunction, finishFunction)

  override def async: Repr[Out] = new SubFlowImpl[In, Out, Mat, F, C](subFlow.async, mergeBackFunction, finishFunction)

  override def mergeSubstreamsWithParallelism(breadth: Int): F[Out] = mergeBackFunction(subFlow, breadth)

  def to[M](sink: Graph[SinkShape[Out], M]): C = finishFunction(subFlow.to(sink))
}
