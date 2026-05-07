/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import akka.NotUsed
import akka.japi.function
import akka.stream._

object BidiFlow {

  private[this] val _identity: BidiFlow[Object, Object, Object, Object, NotUsed] =
    BidiFlow.fromFlows(Flow.of(classOf[Object]), Flow.of(classOf[Object]))

  def identity[A, B]: BidiFlow[A, A, B, B, NotUsed] = _identity.asInstanceOf[BidiFlow[A, A, B, B, NotUsed]]

  /**
   * A graph with the shape of a BidiFlow logically is a BidiFlow, this method makes
   * it so also in type.
   */
  def fromGraph[I1, O1, I2, O2, M](g: Graph[BidiShape[I1, O1, I2, O2], M]): BidiFlow[I1, O1, I2, O2, M] =
    g match {
      case bidi: BidiFlow[I1, O1, I2, O2, M] @unchecked => bidi
      case other                                        => new BidiFlow(scaladsl.BidiFlow.fromGraph(other))
    }

  /**
   * Wraps two Flows to create a ''BidiFlow''. The materialized value of the resulting BidiFlow is determined
   * by the combiner function passed in the second argument list.
   *
   * {{{
   *     +----------------------------+
   *     | Resulting BidiFlow         |
   *     |                            |
   *     |  +----------------------+  |
   * I1 ~~> |        Flow1         | ~~> O1
   *     |  +----------------------+  |
   *     |                            |
   *     |  +----------------------+  |
   * O2 <~~ |        Flow2         | <~~ I2
   *     |  +----------------------+  |
   *     +----------------------------+
   * }}}
   *
   */
  def fromFlowsMat[I1, O1, I2, O2, M1, M2, M](
      flow1: Graph[FlowShape[I1, O1], M1],
      flow2: Graph[FlowShape[I2, O2], M2],
      combine: function.Function2[M1, M2, M]): BidiFlow[I1, O1, I2, O2, M] = {
    new BidiFlow(scaladsl.BidiFlow.fromFlowsMat(flow1, flow2)(combinerToScala(combine)))
  }

  /**
   * Wraps two Flows to create a ''BidiFlow''. The materialized value of the resulting BidiFlow is NotUsed.
   *
   * {{{
   *     +----------------------------+
   *     | Resulting BidiFlow         |
   *     |                            |
   *     |  +----------------------+  |
   * I1 ~~> |        Flow1         | ~~> O1
   *     |  +----------------------+  |
   *     |                            |
   *     |  +----------------------+  |
   * O2 <~~ |        Flow2         | <~~ I2
   *     |  +----------------------+  |
   *     +----------------------------+
   * }}}
   *
   */
  def fromFlows[I1, O1, I2, O2, M1, M2](
      flow1: Graph[FlowShape[I1, O1], M1],
      flow2: Graph[FlowShape[I2, O2], M2]): BidiFlow[I1, O1, I2, O2, NotUsed] =
    new BidiFlow(scaladsl.BidiFlow.fromFlows(flow1, flow2))

  /**
   * Create a BidiFlow where the top and bottom flows are just one simple mapping
   * operator each, expressed by the two functions.
   */
  def fromFunctions[I1, O1, I2, O2](
      top: function.Function[I1, O1],
      bottom: function.Function[I2, O2]): BidiFlow[I1, O1, I2, O2, NotUsed] =
    new BidiFlow(scaladsl.BidiFlow.fromFunctions(top.apply _, bottom.apply _))

  /**
   * If the time between two processed elements *in any direction* exceed the provided timeout, the stream is failed
   * with a [[akka.stream.StreamIdleTimeoutException]].
   *
   * There is a difference between this operator and having two idleTimeout Flows assembled into a BidiStage.
   * If the timeout is configured to be 1 seconds, then this operator will not fail even though there are elements flowing
   * every second in one direction, but no elements are flowing in the other direction. I.e. this operator considers
   * the *joint* frequencies of the elements in both directions.
   */
  def bidirectionalIdleTimeout[I, O](timeout: java.time.Duration): BidiFlow[I, I, O, O, NotUsed] = {
    import scala.jdk.DurationConverters._
    new BidiFlow(scaladsl.BidiFlow.bidirectionalIdleTimeout(timeout.toScala))
  }
}

final class BidiFlow[I1, O1, I2, O2, Mat](delegate: scaladsl.BidiFlow[I1, O1, I2, O2, Mat])
    extends Graph[BidiShape[I1, O1, I2, O2], Mat] {
  override def traversalBuilder = delegate.traversalBuilder
  override def shape = delegate.shape

  def asScala: scaladsl.BidiFlow[I1, O1, I2, O2, Mat] = delegate

  /**
   * Add the given BidiFlow as the next step in a bidirectional transformation
   * pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
   * layer, the closest to the metal.
   * {{{
   *     +----------------------------+
   *     | Resulting BidiFlow         |
   *     |                            |
   *     |  +------+        +------+  |
   * I1 ~~> |      |  ~O1~> |      | ~~> OO1
   *     |  | this |        | bidi |  |
   * O2 <~~ |      | <~I2~  |      | <~~ II2
   *     |  +------+        +------+  |
   *     +----------------------------+
   * }}}
   * The materialized value of the combined [[BidiFlow]] will be the materialized
   * value of the current flow (ignoring the other BidiFlow’s value), use
   * [[BidiFlow#atopMat atopMat]] if a different strategy is needed.
   */
  def atop[OO1, II2, Mat2](bidi: BidiFlow[O1, OO1, II2, I2, Mat2]): BidiFlow[I1, OO1, II2, O2, Mat] =
    new BidiFlow(delegate.atop(bidi.asScala))

  /**
   * Add the given BidiFlow as the next step in a bidirectional transformation  161
   *
   * pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
   * layer, the closest to the metal.
   * {{{
   *     +----------------------------+
   *     | Resulting BidiFlow         |
   *     |                            |
   *     |  +------+        +------+  |
   * I1 ~~> |      |  ~O1~> |      | ~~> OO1
   *     |  | this |        | bidi |  |
   * O2 <~~ |      | <~I2~  |      | <~~ II2
   *     |  +------+        +------+  |
   *     +----------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * flow into the materialized value of the resulting BidiFlow.
   */
  def atop[OO1, II2, Mat2, M](
      bidi: BidiFlow[O1, OO1, II2, I2, Mat2],
      combine: function.Function2[Mat, Mat2, M]): BidiFlow[I1, OO1, II2, O2, M] =
    new BidiFlow(delegate.atopMat(bidi.asScala)(combinerToScala(combine)))

  /**
   * Add the given Flow as the final step in a bidirectional transformation
   * pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
   * layer, the closest to the metal.
   * {{{
   *     +---------------------------+
   *     | Resulting Flow            |
   *     |                           |
   *     |  +------+        +------+ |
   * I1 ~~> |      |  ~O1~> |      | |
   *     |  | this |        | flow | |
   * O2 <~~ |      | <~I2~  |      | |
   *     |  +------+        +------+ |
   *     +---------------------------+
   * }}}
   * The materialized value of the combined [[Flow]] will be the materialized
   * value of the current flow (ignoring the other Flow’s value), use
   * [[BidiFlow#joinMat joinMat]] if a different strategy is needed.
   */
  def join[Mat2](flow: Flow[O1, I2, Mat2]): Flow[I1, O2, Mat] =
    new Flow(delegate.join(flow.asScala))

  /**
   * Add the given Flow as the final step in a bidirectional transformation
   * pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
   * layer, the closest to the metal.
   * {{{
   *     +---------------------------+
   *     | Resulting Flow            |
   *     |                           |
   *     |  +------+        +------+ |
   * I1 ~~> |      |  ~O1~> |      | |
   *     |  | this |        | flow | |
   * O2 <~~ |      | <~I2~  |      | |
   *     |  +------+        +------+ |
   *     +---------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * flow into the materialized value of the resulting [[Flow]].
   */
  def join[Mat2, M](flow: Flow[O1, I2, Mat2], combine: function.Function2[Mat, Mat2, M]): Flow[I1, O2, M] =
    new Flow(delegate.joinMat(flow.asScala)(combinerToScala(combine)))

  /**
   * Turn this BidiFlow around by 180 degrees, logically flipping it upside down in a protocol stack.
   */
  def reversed: BidiFlow[I2, O2, I1, O1, Mat] = new BidiFlow(delegate.reversed)

  /**
   * Transform only the materialized value of this BidiFlow, leaving all other properties as they were.
   */
  def mapMaterializedValue[Mat2](f: function.Function[Mat, Mat2]): BidiFlow[I1, O1, I2, O2, Mat2] =
    new BidiFlow(delegate.mapMaterializedValue(f.apply _))

  /**
   * Change the attributes of this [[Source]] to the given ones and seal the list
   * of attributes. This means that further calls will not be able to remove these
   * attributes, but instead add new ones. Note that this
   * operation has no effect on an empty Flow (because the attributes apply
   * only to the contained processing operators).
   */
  override def withAttributes(attr: Attributes): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.withAttributes(attr))

  /**
   * Add the given attributes to this Source. Further calls to `withAttributes`
   * will not remove these attributes. Note that this
   * operation has no effect on an empty Flow (because the attributes apply
   * only to the contained processing operators).
   */
  override def addAttributes(attr: Attributes): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.addAttributes(attr))

  /**
   * Add a ``name`` attribute to this Flow.
   */
  override def named(name: String): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.named(name))

  /**
   * Put an asynchronous boundary around this `Flow`
   */
  override def async: BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.async)

  /**
   * Put an asynchronous boundary around this `Flow`
   *
   * @param dispatcher Run the graph on this dispatcher
   */
  override def async(dispatcher: String): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.async(dispatcher))

  /**
   * Put an asynchronous boundary around this `Flow`
   *
   * @param dispatcher      Run the graph on this dispatcher
   * @param inputBufferSize Set the input buffer to this size for the graph
   */
  override def async(dispatcher: String, inputBufferSize: Int): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(delegate.async(dispatcher, inputBufferSize))

  override def getAttributes: Attributes = delegate.getAttributes
}
