/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.unchecked.uncheckedVariance

import akka.annotation.DoNotInherit
import akka.stream._

/**
 * A “stream of streams” sub-flow of data elements, e.g. produced by `groupBy`.
 * SubFlows cannot contribute to the super-flow’s materialized value since they
 * are materialized later, during the runtime of the flow graph processing.
 *
 * Not for user extension
 */
@DoNotInherit
trait SubFlow[+Out, +Mat, +F[+_], C] extends FlowOps[Out, Mat] {

  override type Repr[+T] = SubFlow[T, Mat @uncheckedVariance, F @uncheckedVariance, C @uncheckedVariance]
  override type Closed = C

  /**
   * Attach a [[Sink]] to each sub-flow, closing the overall Graph that is being
   * constructed.
   *
   * Note that attributes set on the returned graph, including async boundaries are now for the entire graph and not
   * the `SubFlow`. for example `async` will not have any effect as the returned graph is the entire, closed graph.
   */
  def to[M](sink: Graph[SinkShape[Out], M]): C

  /**
   * Flatten the sub-flows back into the super-flow by performing a merge
   * without parallelism limit (i.e. having an unbounded number of sub-flows
   * active concurrently).
   *
   * This is identical in effect to `mergeSubstreamsWithParallelism(Integer.MAX_VALUE)`.
   */
  def mergeSubstreams: F[Out] = mergeSubstreamsWithParallelism(Int.MaxValue)

  /**
   * Flatten the sub-flows back into the super-flow by performing a merge
   * with the given parallelism limit. This means that only up to `parallelism`
   * substreams will be executed at any given time. Substreams that are not
   * yet executed are also not materialized, meaning that back-pressure will
   * be exerted at the operator that creates the substreams when the parallelism
   * limit is reached.
   */
  def mergeSubstreamsWithParallelism(parallelism: Int): F[Out]

  /**
   * Flatten the sub-flows back into the super-flow by concatenating them.
   * This is usually a bad idea when combined with `groupBy` since it can
   * easily lead to deadlock—the concatenation does not consume from the second
   * substream until the first has finished and the `groupBy` operator will get
   * back-pressure from the second stream.
   *
   * This is identical in effect to `mergeSubstreamsWithParallelism(1)`.
   */
  def concatSubstreams: F[Out] = mergeSubstreamsWithParallelism(1)
}
