/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.stream.{ BidiShape, _ }
import akka.stream.impl.{ LinearTraversalBuilder, Timers, TraversalBuilder }

final class BidiFlow[-I1, +O1, -I2, +O2, +Mat](
    override val traversalBuilder: TraversalBuilder,
    override val shape: BidiShape[I1, O1, I2, O2])
    extends Graph[BidiShape[I1, O1, I2, O2], Mat] {

  def asJava[JI1 <: I1, JO1 >: O1, JI2 <: I2, JO2 >: O2, JMat >: Mat]: javadsl.BidiFlow[JI1, JO1, JI2, JO2, JMat] =
    new javadsl.BidiFlow(this)

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
  def atop[OO1, II2, Mat2](bidi: Graph[BidiShape[O1, OO1, II2, I2], Mat2]): BidiFlow[I1, OO1, II2, O2, Mat] =
    atopMat(bidi)(Keep.left)

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
   * The `combine` function is used to compose the materialized values of this flow and that
   * flow into the materialized value of the resulting BidiFlow.
   */
  def atopMat[OO1, II2, Mat2, M](bidi: Graph[BidiShape[O1, OO1, II2, I2], Mat2])(
      combine: (Mat, Mat2) => M): BidiFlow[I1, OO1, II2, O2, M] =
    if (this eq BidiFlow.identity) {
      // optimizations possible since we know that identity matval is NotUsed
      if (combine eq Keep.right)
        BidiFlow.fromGraph(bidi).asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      else if ((combine eq Keep.left) || (combine eq Keep.none))
        BidiFlow.fromGraph(bidi).mapMaterializedValue(_ => NotUsed).asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      else {
        BidiFlow
          .fromGraph(bidi)
          .mapMaterializedValue(mat2 => combine(NotUsed.asInstanceOf[Mat], mat2))
          .asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      }
    } else if (bidi eq BidiFlow.identity) {
      // optimizations possible since we know that identity matval is NotUsed
      if (combine eq Keep.left)
        this.asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      else if ((combine eq Keep.right) || (combine eq Keep.none))
        this.mapMaterializedValue(_ => NotUsed).asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      else {
        this
          .mapMaterializedValue(mat => combine(mat, NotUsed.asInstanceOf[Mat2]))
          .asInstanceOf[BidiFlow[I1, OO1, II2, O2, M]]
      }
    } else {
      val newBidi1Shape = shape.deepCopy()
      val newBidi2Shape = bidi.shape.deepCopy()

      // We MUST add the current module as an explicit submodule. The composite builder otherwise *grows* the
      // existing module, which is not good if there are islands present (the new module will "join" the island).
      val newTraversalBuilder =
        TraversalBuilder
          .empty()
          .add(traversalBuilder, newBidi1Shape, Keep.right)
          .add(bidi.traversalBuilder, newBidi2Shape, combine)
          .wire(newBidi1Shape.out1, newBidi2Shape.in1)
          .wire(newBidi2Shape.out2, newBidi1Shape.in2)

      new BidiFlow(
        newTraversalBuilder,
        BidiShape(newBidi1Shape.in1, newBidi2Shape.out1, newBidi2Shape.in2, newBidi1Shape.out2))
    }

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
  def join[Mat2](flow: Graph[FlowShape[O1, I2], Mat2]): Flow[I1, O2, Mat] = joinMat(flow)(Keep.left)

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
  def joinMat[Mat2, M](flow: Graph[FlowShape[O1, I2], Mat2])(combine: (Mat, Mat2) => M): Flow[I1, O2, M] = {
    val newBidiShape = shape.deepCopy()
    val newFlowShape = flow.shape.deepCopy()

    // We MUST add the current module as an explicit submodule. The composite builder otherwise *grows* the
    // existing module, which is not good if there are islands present (the new module will "join" the island).
    val resultBuilder = TraversalBuilder
      .empty()
      .add(traversalBuilder, newBidiShape, Keep.right)
      .add(flow.traversalBuilder, newFlowShape, combine)
      .wire(newBidiShape.out1, newFlowShape.in)
      .wire(newFlowShape.out, newBidiShape.in2)

    val newShape = FlowShape(newBidiShape.in1, newBidiShape.out2)

    new Flow(LinearTraversalBuilder.fromBuilder(resultBuilder, newShape, Keep.right), newShape)
  }

  /**
   * Turn this BidiFlow around by 180 degrees, logically flipping it upside down in a protocol stack.
   */
  def reversed: BidiFlow[I2, O2, I1, O1, Mat] =
    new BidiFlow(traversalBuilder, BidiShape(shape.in2, shape.out2, shape.in1, shape.out1))

  /**
   * Transform only the materialized value of this BidiFlow, leaving all other properties as they were.
   */
  def mapMaterializedValue[Mat2](f: Mat => Mat2): BidiFlow[I1, O1, I2, O2, Mat2] =
    new BidiFlow(traversalBuilder.transformMat(f.asInstanceOf[Any => Any]), shape)

  /**
   * Change the attributes of this [[Source]] to the given ones and seal the list
   * of attributes. This means that further calls will not be able to remove these
   * attributes, but instead add new ones. Note that this
   * operation has no effect on an empty Flow (because the attributes apply
   * only to the contained processing operators).
   */
  override def withAttributes(attr: Attributes): BidiFlow[I1, O1, I2, O2, Mat] =
    new BidiFlow(traversalBuilder.setAttributes(attr), shape)

  /**
   * Add the given attributes to this Source. Further calls to `withAttributes`
   * will not remove these attributes. Note that this
   * operation has no effect on an empty Flow (because the attributes apply
   * only to the contained processing operators).
   */
  override def addAttributes(attr: Attributes): BidiFlow[I1, O1, I2, O2, Mat] =
    withAttributes(traversalBuilder.attributes and attr)

  /**
   * Add a ``name`` attribute to this Flow.
   */
  override def named(name: String): BidiFlow[I1, O1, I2, O2, Mat] =
    addAttributes(Attributes.name(name))

  /**
   * Put an asynchronous boundary around this `BidiFlow`
   */
  override def async: BidiFlow[I1, O1, I2, O2, Mat] =
    super.async.asInstanceOf[BidiFlow[I1, O1, I2, O2, Mat]]

  /**
   * Put an asynchronous boundary around this `BidiFlow`
   *
   * @param dispatcher Run the graph on this dispatcher
   */
  override def async(dispatcher: String): BidiFlow[I1, O1, I2, O2, Mat] =
    super.async(dispatcher).asInstanceOf[BidiFlow[I1, O1, I2, O2, Mat]]

  /**
   * Put an asynchronous boundary around this `BidiFlow`
   *
   * @param dispatcher      Run the graph on this dispatcher
   * @param inputBufferSize Set the input buffer to this size for the graph
   */
  override def async(dispatcher: String, inputBufferSize: Int): BidiFlow[I1, O1, I2, O2, Mat] =
    super.async(dispatcher, inputBufferSize).asInstanceOf[BidiFlow[I1, O1, I2, O2, Mat]]

  override def getAttributes: Attributes = traversalBuilder.attributes

}

object BidiFlow {
  private[this] val _identity: BidiFlow[Any, Any, Any, Any, NotUsed] =
    BidiFlow.fromFlows(Flow[Any], Flow[Any])

  def identity[A, B]: BidiFlow[A, A, B, B, NotUsed] = _identity.asInstanceOf[BidiFlow[A, A, B, B, NotUsed]]

  /**
   * A graph with the shape of a flow logically is a flow, this method makes
   * it so also in type.
   */
  def fromGraph[I1, O1, I2, O2, Mat](graph: Graph[BidiShape[I1, O1, I2, O2], Mat]): BidiFlow[I1, O1, I2, O2, Mat] =
    graph match {
      case bidi: BidiFlow[I1, O1, I2, O2, Mat]                    => bidi
      case bidi: javadsl.BidiFlow[I1, O1, I2, O2, Mat] @unchecked => bidi.asScala
      case other =>
        new BidiFlow(other.traversalBuilder, other.shape)
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
  def fromFlowsMat[I1, O1, I2, O2, M1, M2, M](flow1: Graph[FlowShape[I1, O1], M1], flow2: Graph[FlowShape[I2, O2], M2])(
      combine: (M1, M2) => M): BidiFlow[I1, O1, I2, O2, M] = {
    val newFlow1Shape = flow1.shape.deepCopy()
    val newFlow2Shape = flow2.shape.deepCopy()

    new BidiFlow(
      TraversalBuilder
        .empty()
        .add(flow1.traversalBuilder, newFlow1Shape, Keep.right)
        .add(flow2.traversalBuilder, newFlow2Shape, combine),
      BidiShape(newFlow1Shape.in, newFlow1Shape.out, newFlow2Shape.in, newFlow2Shape.out))
  }

  /**
   * Wraps two Flows to create a ''BidiFlow''. The materialized value of the resulting BidiFlow is Unit.
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
    fromFlowsMat(flow1, flow2)(Keep.none)

  /**
   * Create a BidiFlow where the top and bottom flows are just one simple mapping
   * operator each, expressed by the two functions.
   */
  def fromFunctions[I1, O1, I2, O2](outbound: I1 => O1, inbound: I2 => O2): BidiFlow[I1, O1, I2, O2, NotUsed] =
    fromFlows(Flow[I1].map(outbound), Flow[I2].map(inbound))

  /**
   * If the time between two processed elements *in any direction* exceed the provided timeout, the stream is failed
   * with a [[akka.stream.StreamIdleTimeoutException]].
   *
   * There is a difference between this operator and having two idleTimeout Flows assembled into a BidiStage.
   * If the timeout is configured to be 1 seconds, then this operator will not fail even though there are elements flowing
   * every second in one direction, but no elements are flowing in the other direction. I.e. this operator considers
   * the *joint* frequencies of the elements in both directions.
   */
  def bidirectionalIdleTimeout[I, O](timeout: FiniteDuration): BidiFlow[I, I, O, O, NotUsed] =
    fromGraph(new Timers.IdleTimeoutBidi(timeout))

}
