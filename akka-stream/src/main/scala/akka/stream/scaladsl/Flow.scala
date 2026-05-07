/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.annotation.implicitNotFound
import scala.annotation.nowarn
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import org.reactivestreams.Processor
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import akka.Done
import akka.NotUsed
import akka.actor.ActorRef
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.event.LogMarker
import akka.event.LoggingAdapter
import akka.event.MarkerLoggingAdapter
import akka.stream._
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.CoupledTerminationBidi
import akka.stream.impl.LinearTraversalBuilder
import akka.stream.impl.ProcessorModule
import akka.stream.impl.SetupFlowStage
import akka.stream.impl.SingleConcat
import akka.stream.impl.SubFlowImpl
import akka.stream.impl.Throttle
import akka.stream.impl.Timers
import akka.stream.impl.TraversalBuilder
import akka.stream.impl.fusing
import akka.stream.impl.fusing._
import akka.stream.impl.fusing.FlattenMerge
import akka.stream.stage._
import akka.util.ConstantFun
import akka.util.OptionVal
import akka.util.Timeout

/**
 * A `Flow` is a set of stream processing steps that has one open input and one open output.
 */
final class Flow[-In, +Out, +Mat](
    override val traversalBuilder: LinearTraversalBuilder,
    override val shape: FlowShape[In, Out])
    extends FlowOpsMat[Out, Mat]
    with Graph[FlowShape[In, Out], Mat] {

  override def toString: String = s"Flow($shape)"

  override type Repr[+O] = Flow[In @uncheckedVariance, O, Mat @uncheckedVariance]
  override type ReprMat[+O, +M] = Flow[In @uncheckedVariance, O, M]

  override type Closed = Sink[In @uncheckedVariance, Mat @uncheckedVariance]
  override type ClosedMat[+M] = Sink[In @uncheckedVariance, M]

  private[stream] def isIdentity: Boolean = this.traversalBuilder eq Flow.identityTraversalBuilder

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T] = viaMat(flow)(Keep.left)

  override def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(
      combine: (Mat, Mat2) => Mat3): Flow[In, T, Mat3] = {
    if (this.isIdentity) {
      // optimization by returning flow if possible since we know Mat2 == Mat3 from flow
      if (combine == Keep.right) Flow.fromGraph(flow).asInstanceOf[Flow[In, T, Mat3]]
      else {
        // Keep.none is optimized and we know left means Mat3 == NotUsed
        val useCombine =
          if (combine == Keep.left) Keep.none
          else combine
        new Flow(LinearTraversalBuilder.empty().append(flow.traversalBuilder, flow.shape, useCombine), flow.shape)
          .asInstanceOf[Flow[In, T, Mat3]]
      }
    } else if (flow.traversalBuilder eq Flow.identityTraversalBuilder) {
      // optimization by returning this if possible since we know Mat2 == Mat from this
      if (combine == Keep.left) this.asInstanceOf[Flow[In, T, Mat3]]
      else {
        // Keep.none is somewhat optimized and we know Mat == NotUsed
        val useCombine =
          if (combine == Keep.right) Keep.none
          else combine
        new Flow(
          traversalBuilder.append(LinearTraversalBuilder.empty(), shape, useCombine),
          FlowShape[In, T](shape.in, flow.shape.out))
      }
    } else {
      new Flow(
        traversalBuilder.append(flow.traversalBuilder, flow.shape, combine),
        FlowShape[In, T](shape.in, flow.shape.out))
    }
  }

  /**
   * Connect this [[Flow]] to a [[Sink]], concatenating the processing steps of both.
   * {{{
   *     +------------------------------+
   *     | Resulting Sink[In, Mat]      |
   *     |                              |
   *     |  +------+          +------+  |
   *     |  |      |          |      |  |
   * In ~~> | flow | ~~Out~~> | sink |  |
   *     |  |   Mat|          |     M|  |
   *     |  +------+          +------+  |
   *     +------------------------------+
   * }}}
   * The materialized value of the combined [[Sink]] will be the materialized
   * value of the current flow (ignoring the given Sink’s value), use
   * [[Flow#toMat[Mat2* toMat]] if a different strategy is needed.
   *
   * See also [[toMat]] when access to materialized values of the parameter is needed.
   */
  def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Sink[In, Mat] = toMat(sink)(Keep.left)

  /**
   * Connect this [[Flow]] to a [[Sink]], concatenating the processing steps of both.
   * {{{
   *     +----------------------------+
   *     | Resulting Sink[In, M2]     |
   *     |                            |
   *     |  +------+        +------+  |
   *     |  |      |        |      |  |
   * In ~~> | flow | ~Out~> | sink |  |
   *     |  |   Mat|        |     M|  |
   *     |  +------+        +------+  |
   *     +----------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * Sink into the materialized value of the resulting Sink.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): Sink[In, Mat3] = {
    if (isIdentity) {
      new Sink(LinearTraversalBuilder.fromBuilder(sink.traversalBuilder, sink.shape, combine), SinkShape(sink.shape.in))
        .asInstanceOf[Sink[In, Mat3]]
    } else {
      new Sink(traversalBuilder.append(sink.traversalBuilder, sink.shape, combine), SinkShape(shape.in))
    }
  }

  /**
   * Transform the materialized value of this Flow, leaving all other properties as they were.
   */
  override def mapMaterializedValue[Mat2](f: Mat => Mat2): ReprMat[Out, Mat2] =
    new Flow(traversalBuilder.transformMat(f), shape)

  /**
   * Materializes this [[Flow]], immediately returning (1) its materialized value, and (2) a newly materialized [[Flow]].
   * The returned flow is partial materialized and do not support multiple times materialization.
   *
   * Note that `preMaterialize` is implemented through a reactive streams `Publisher` and `Subscriber` pair which means that
   * a buffer is introduced and that errors are not propagated upstream but are turned into cancellations without error details.
   */
  def preMaterialize()(implicit materializer: Materializer): (Mat, ReprMat[Out, NotUsed]) = {
    val ((sub, mat), pub) =
      Source.asSubscriber[In].viaMat(this)(Keep.both).toMat(Sink.asPublisher(false))(Keep.both).run()
    (mat, Flow.fromSinkAndSource(Sink.fromSubscriber(sub), Source.fromPublisher(pub)))
  }

  /**
   * Transform this Flow by applying a function to each *incoming* upstream element before
   * it is passed to the [[Flow]]
   *
   * '''Backpressures when''' original [[Flow]] backpressures
   *
   * '''Cancels when''' original [[Flow]] cancels
   */
  def contramap[In2](f: In2 => In): Flow[In2, Out, Mat] =
    Flow.fromFunction(f).viaMat(this)(Keep.right)

  /**
   * Join this [[Flow]] to another [[Flow]], by cross connecting the inputs and outputs, creating a [[RunnableGraph]].
   * {{{
   * +------+        +-------+
   * |      | ~Out~> |       |
   * | this |        | other |
   * |      | <~In~  |       |
   * +------+        +-------+
   * }}}
   * The materialized value of the combined [[Flow]] will be the materialized
   * value of the current flow (ignoring the other Flow’s value), use
   * [[Flow#joinMat[Mat2* joinMat]] if a different strategy is needed.
   */
  def join[Mat2](flow: Graph[FlowShape[Out, In], Mat2]): RunnableGraph[Mat] = joinMat(flow)(Keep.left)

  /**
   * Join this [[Flow]] to another [[Flow]], by cross connecting the inputs and outputs, creating a [[RunnableGraph]]
   * {{{
   * +------+        +-------+
   * |      | ~Out~> |       |
   * | this |        | other |
   * |      | <~In~  |       |
   * +------+        +-------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * Flow into the materialized value of the resulting Flow.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def joinMat[Mat2, Mat3](flow: Graph[FlowShape[Out, In], Mat2])(combine: (Mat, Mat2) => Mat3): RunnableGraph[Mat3] = {
    val resultBuilder =
      traversalBuilder.append(flow.traversalBuilder, flow.shape, combine).wire(flow.shape.out, shape.in)

    RunnableGraph(resultBuilder)
  }

  /**
   * Join this [[Flow]] to a [[BidiFlow]] to close off the “top” of the protocol stack:
   * {{{
   * +---------------------------+
   * | Resulting Flow            |
   * |                           |
   * | +------+        +------+  |
   * | |      | ~Out~> |      | ~~> O1
   * | | flow |        | bidi |  |
   * | |      | <~In~  |      | <~~ I2
   * | +------+        +------+  |
   * +---------------------------+
   * }}}
   * The materialized value of the combined [[Flow]] will be the materialized
   * value of the current flow (ignoring the [[BidiFlow]]’s value), use
   * [[Flow#joinMat[I2* joinMat]] if a different strategy is needed.
   */
  def join[I2, O1, Mat2](bidi: Graph[BidiShape[Out, O1, I2, In], Mat2]): Flow[I2, O1, Mat] = joinMat(bidi)(Keep.left)

  /**
   * Join this [[Flow]] to a [[BidiFlow]] to close off the “top” of the protocol stack:
   * {{{
   * +---------------------------+
   * | Resulting Flow            |
   * |                           |
   * | +------+        +------+  |
   * | |      | ~Out~> |      | ~~> O1
   * | | flow |        | bidi |  |
   * | |      | <~In~  |      | <~~ I2
   * | +------+        +------+  |
   * +---------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * [[BidiFlow]] into the materialized value of the resulting [[Flow]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def joinMat[I2, O1, Mat2, M](bidi: Graph[BidiShape[Out, O1, I2, In], Mat2])(
      combine: (Mat, Mat2) => M): Flow[I2, O1, M] = {
    val newBidiShape = bidi.shape.deepCopy()
    val newFlowShape = shape.deepCopy()

    val resultBuilder =
      TraversalBuilder
        .empty()
        .add(traversalBuilder, newFlowShape, Keep.right)
        .add(bidi.traversalBuilder, newBidiShape, combine)
        .wire(newFlowShape.out, newBidiShape.in1)
        .wire(newBidiShape.out2, newFlowShape.in)

    val newShape = FlowShape(newBidiShape.in2, newBidiShape.out1)

    new Flow(LinearTraversalBuilder.fromBuilder(resultBuilder, newShape, Keep.right), newShape)
  }

  /**
   * Replace the attributes of this [[Flow]] with the given ones. If this Flow is a composite
   * of multiple graphs, new attributes on the composite will be less specific than attributes
   * set directly on the individual graphs of the composite.
   *
   * Note that this operation has no effect on an empty Flow (because the attributes apply
   * only to the contained processing operators).
   */
  override def withAttributes(attr: Attributes): Repr[Out] =
    new Flow(traversalBuilder.setAttributes(attr), shape)

  /**
   * Add the given attributes to this [[Flow]]. If the specific attribute was already present
   * on this graph this means the added attribute will be more specific than the existing one.
   * If this Flow is a composite of multiple graphs, new attributes on the composite will be
   * less specific than attributes set directly on the individual graphs of the composite.
   */
  override def addAttributes(attr: Attributes): Repr[Out] = withAttributes(traversalBuilder.attributes and attr)

  /**
   * Add a ``name`` attribute to this Flow.
   */
  override def named(name: String): Repr[Out] = addAttributes(Attributes.name(name))

  /**
   * Put an asynchronous boundary around this `Flow`
   */
  override def async: Repr[Out] = super.async.asInstanceOf[Repr[Out]]

  /**
   * Put an asynchronous boundary around this `Flow`
   *
   * @param dispatcher Run the graph on this dispatcher
   */
  override def async(dispatcher: String): Repr[Out] =
    super.async(dispatcher).asInstanceOf[Repr[Out]]

  /**
   * Put an asynchronous boundary around this `Flow`
   *
   * @param dispatcher      Run the graph on this dispatcher
   * @param inputBufferSize Set the input buffer to this size for the graph
   */
  override def async(dispatcher: String, inputBufferSize: Int): Repr[Out] =
    super.async(dispatcher, inputBufferSize).asInstanceOf[Repr[Out]]

  /**
   * Connect the `Source` to this `Flow` and then connect it to the `Sink` and run it. The returned tuple contains
   * the materialized values of the `Source` and `Sink`, e.g. the `Subscriber` of a of a [[Source#subscriber]] and
   * and `Publisher` of a [[Sink#publisher]].
   *
   * Note that the `ActorSystem` can be used as the implicit `materializer` parameter to use the
   * [[akka.stream.SystemMaterializer]] for running the stream.
   */
  def runWith[Mat1, Mat2](source: Graph[SourceShape[In], Mat1], sink: Graph[SinkShape[Out], Mat2])(
      implicit materializer: Materializer): (Mat1, Mat2) =
    Source.fromGraph(source).via(this).toMat(sink)(Keep.both).run()

  /**
   * Converts this Flow to a [[RunnableGraph]] that materializes to a Reactive Streams [[org.reactivestreams.Processor]]
   * which implements the operations encapsulated by this Flow. Every materialization results in a new Processor
   * instance, i.e. the returned [[RunnableGraph]] is reusable.
   *
   * @return A [[RunnableGraph]] that materializes to a Processor when run() is called on it.
   */
  def toProcessor: RunnableGraph[Processor[In @uncheckedVariance, Out @uncheckedVariance]] =
    Source
      .asSubscriber[In]
      .via(this)
      .toMat(Sink.asPublisher[Out](false))(Keep.both[Subscriber[In], Publisher[Out]])
      .mapMaterializedValue {
        case (sub, pub) =>
          new Processor[In, Out] {
            override def onError(t: Throwable): Unit = sub.onError(t)
            override def onSubscribe(s: Subscription): Unit = sub.onSubscribe(s)
            override def onComplete(): Unit = sub.onComplete()
            override def onNext(t: In): Unit = sub.onNext(t)
            override def subscribe(s: Subscriber[_ >: Out]): Unit = pub.subscribe(s)
          }
      }

  /**
   * Turns a Flow into a FlowWithContext which manages a context per element along a stream.
   *
   * @param collapseContext turn each incoming pair of element and context value into an element of this Flow
   * @param extractContext turn each outgoing element of this Flow into an outgoing context value
   */
  def asFlowWithContext[U, CtxU, CtxOut](collapseContext: (U, CtxU) => In)(
      extractContext: Out => CtxOut): FlowWithContext[U, CtxU, Out, CtxOut, Mat] =
    new FlowWithContext(
      Flow[(U, CtxU)]
        .map {
          case (e, ctx) =>
            collapseContext(e, ctx)
        }
        .viaMat(this)(Keep.right)
        .map(e => (e, extractContext(e))))

  /** Converts this Scala DSL element to it's Java DSL counterpart. */
  def asJava[JIn <: In]: javadsl.Flow[JIn, Out @uncheckedVariance, Mat @uncheckedVariance] =
    new javadsl.Flow(this)
}

object Flow {
  private[stream] val identityTraversalBuilder =
    LinearTraversalBuilder.fromBuilder(GraphStages.identity.traversalBuilder, GraphStages.identity.shape, Keep.right)

  private[this] val identity: Flow[Any, Any, NotUsed] =
    new Flow[Any, Any, NotUsed](identityTraversalBuilder, GraphStages.identity.shape)

  /**
   * Creates a Flow from a Reactive Streams [[org.reactivestreams.Processor]]
   */
  def fromProcessor[I, O](processorFactory: () => Processor[I, O]): Flow[I, O, NotUsed] = {
    fromProcessorMat(() => (processorFactory(), NotUsed))
  }

  /**
   * Creates a Flow from a Reactive Streams [[org.reactivestreams.Processor]] and returns a materialized value.
   */
  def fromProcessorMat[I, O, M](processorFactory: () => (Processor[I, O], M)): Flow[I, O, M] =
    fromGraph(ProcessorModule(processorFactory))

  /**
   * Returns a `Flow` which outputs all its inputs.
   */
  def apply[T]: Flow[T, T, NotUsed] = identity.asInstanceOf[Flow[T, T, NotUsed]]

  /**
   * Creates a [Flow] which will use the given function to transform its inputs to outputs. It is equivalent
   * to `Flow[T].map(f)`
   */
  def fromFunction[A, B](f: A => B): Flow[A, B, NotUsed] = apply[A].map(f)

  /**
   * A graph with the shape of a flow logically is a flow, this method makes
   * it so also in type.
   */
  def fromGraph[I, O, M](g: Graph[FlowShape[I, O], M]): Flow[I, O, M] =
    g match {
      case f: Flow[I, O, M]                                       => f
      case f: javadsl.Flow[I, O, M] @unchecked                    => f.asScala
      case g: GraphStageWithMaterializedValue[FlowShape[I, O], M] =>
        // move these from the operator itself to make the returned source
        // behave as it is the operator with regards to attributes
        val attrs = g.traversalBuilder.attributes
        val noAttrStage = g.withAttributes(Attributes.none)
        new Flow(
          LinearTraversalBuilder.fromBuilder(noAttrStage.traversalBuilder, noAttrStage.shape, Keep.right),
          noAttrStage.shape).withAttributes(attrs)

      case _ => new Flow(LinearTraversalBuilder.fromBuilder(g.traversalBuilder, g.shape, Keep.right), g.shape)
    }

  /**
   * Defers the creation of a [[Flow]] until materialization. The `factory` function
   * exposes [[Materializer]] which is going to be used during materialization and
   * [[Attributes]] of the [[Flow]] returned by this method.
   */
  def fromMaterializer[T, U, M](factory: (Materializer, Attributes) => Flow[T, U, M]): Flow[T, U, Future[M]] =
    Flow.fromGraph(new SetupFlowStage(factory))

  /**
   * Defers the creation of a [[Flow]] until materialization. The `factory` function
   * exposes [[ActorMaterializer]] which is going to be used during materialization and
   * [[Attributes]] of the [[Flow]] returned by this method.
   */
  @deprecated("Use 'fromMaterializer' instead", "2.6.0")
  def setup[T, U, M](factory: (ActorMaterializer, Attributes) => Flow[T, U, M]): Flow[T, U, Future[M]] =
    Flow.fromGraph(new SetupFlowStage((materializer, attributes) =>
      factory(ActorMaterializerHelper.downcast(materializer), attributes)))

  /**
   * Creates a `Flow` from a `Sink` and a `Source` where the Flow's input
   * will be sent to the Sink and the Flow's output will come from the Source.
   *
   * The resulting flow can be visualized as:
   * {{{
   *     +----------------------------------------------+
   *     | Resulting Flow[I, O, NotUsed]                |
   *     |                                              |
   *     |  +---------+                  +-----------+  |
   *     |  |         |                  |           |  |
   * I  ~~> | Sink[I] | [no-connection!] | Source[O] | ~~> O
   *     |  |         |                  |           |  |
   *     |  +---------+                  +-----------+  |
   *     +----------------------------------------------+
   * }}}
   *
   * The completion of the Sink and Source sides of a Flow constructed using
   * this method are independent. So if the Sink receives a completion signal,
   * the Source side will remain unaware of that. If you are looking to couple
   * the termination signals of the two sides use `Flow.fromSinkAndSourceCoupled` instead.
   *
   * See also [[fromSinkAndSourceMat]] when access to materialized values of the parameters is needed.
   */
  def fromSinkAndSource[I, O](sink: Graph[SinkShape[I], _], source: Graph[SourceShape[O], _]): Flow[I, O, NotUsed] =
    fromSinkAndSourceMat(sink, source)(Keep.none)

  /**
   * Creates a `Flow` from a `Sink` and a `Source` where the Flow's input
   * will be sent to the Sink and the Flow's output will come from the Source.
   *
   * The resulting flow can be visualized as:
   * {{{
   *     +-------------------------------------------------------+
   *     | Resulting Flow[I, O, M]                              |
   *     |                                                      |
   *     |  +-------------+                  +---------------+  |
   *     |  |             |                  |               |  |
   * I  ~~> | Sink[I, M1] | [no-connection!] | Source[O, M2] | ~~> O
   *     |  |             |                  |               |  |
   *     |  +-------------+                  +---------------+  |
   *     +------------------------------------------------------+
   * }}}
   *
   * The completion of the Sink and Source sides of a Flow constructed using
   * this method are independent. So if the Sink receives a completion signal,
   * the Source side will remain unaware of that. If you are looking to couple
   * the termination signals of the two sides use `Flow.fromSinkAndSourceCoupledMat` instead.
   *
   * The `combine` function is used to compose the materialized values of the `sink` and `source`
   * into the materialized value of the resulting [[Flow]].
   */
  def fromSinkAndSourceMat[I, O, M1, M2, M](sink: Graph[SinkShape[I], M1], source: Graph[SourceShape[O], M2])(
      combine: (M1, M2) => M): Flow[I, O, M] =
    fromGraph(GraphDSL.createGraph(sink, source)(combine) { _ => (in, out) =>
      FlowShape(in.in, out.out)
    })

  /**
   * Allows coupling termination (cancellation, completion, erroring) of Sinks and Sources while creating a Flow from them.
   * Similar to [[Flow.fromSinkAndSource]] however couples the termination of these two operators.
   *
   * The resulting flow can be visualized as:
   * {{{
   *     +---------------------------------------------+
   *     | Resulting Flow[I, O, NotUsed]               |
   *     |                                             |
   *     |  +---------+                 +-----------+  |
   *     |  |         |                 |           |  |
   * I  ~~> | Sink[I] | ~~~(coupled)~~~ | Source[O] | ~~> O
   *     |  |         |                 |           |  |
   *     |  +---------+                 +-----------+  |
   *     +---------------------------------------------+
   * }}}
   *
   * E.g. if the emitted [[Flow]] gets a cancellation, the [[Source]] of course is cancelled,
   * however the Sink will also be completed. The table below illustrates the effects in detail:
   *
   * <table>
   *   <tr>
   *     <th>Returned Flow</th>
   *     <th>Sink (<code>in</code>)</th>
   *     <th>Source (<code>out</code>)</th>
   *   </tr>
   *   <tr>
   *     <td><i>cause:</i> upstream (sink-side) receives completion</td>
   *     <td><i>effect:</i> receives completion</td>
   *     <td><i>effect:</i> receives cancel</td>
   *   </tr>
   *   <tr>
   *     <td><i>cause:</i> upstream (sink-side) receives error</td>
   *     <td><i>effect:</i> receives error</td>
   *     <td><i>effect:</i> receives cancel</td>
   *   </tr>
   *   <tr>
   *     <td><i>cause:</i> downstream (source-side) receives cancel</td>
   *     <td><i>effect:</i> completes</td>
   *     <td><i>effect:</i> receives cancel</td>
   *   </tr>
   *   <tr>
   *     <td><i>effect:</i> cancels upstream, completes downstream</td>
   *     <td><i>effect:</i> completes</td>
   *     <td><i>cause:</i> signals complete</td>
   *   </tr>
   *   <tr>
   *     <td><i>effect:</i> cancels upstream, errors downstream</td>
   *     <td><i>effect:</i> receives error</td>
   *     <td><i>cause:</i> signals error or throws</td>
   *   </tr>
   *   <tr>
   *     <td><i>effect:</i> cancels upstream, completes downstream</td>
   *     <td><i>cause:</i> cancels</td>
   *     <td><i>effect:</i> receives cancel</td>
   *   </tr>
   * </table>
   *
   * See also [[fromSinkAndSourceCoupledMat]] when access to materialized values of the parameters is needed.
   */
  def fromSinkAndSourceCoupled[I, O](
      sink: Graph[SinkShape[I], _],
      source: Graph[SourceShape[O], _]): Flow[I, O, NotUsed] =
    fromSinkAndSourceCoupledMat(sink, source)(Keep.none)

  /**
   * Allows coupling termination (cancellation, completion, erroring) of Sinks and Sources while creating a Flow from them.
   * Similar to [[Flow.fromSinkAndSource]] however couples the termination of these two operators.
   *
   * The resulting flow can be visualized as:
   * {{{
   *     +-----------------------------------------------------+
   *     | Resulting Flow[I, O, M]                             |
   *     |                                                     |
   *     |  +-------------+                 +---------------+  |
   *     |  |             |                 |               |  |
   * I  ~~> | Sink[I, M1] | ~~~(coupled)~~~ | Source[O, M2] | ~~> O
   *     |  |             |                 |               |  |
   *     |  +-------------+                 +---------------+  |
   *     +-----------------------------------------------------+
   * }}}
   *
   * E.g. if the emitted [[Flow]] gets a cancellation, the [[Source]] of course is cancelled,
   * however the Sink will also be completed. The table on [[Flow.fromSinkAndSourceCoupled]]
   * illustrates the effects in detail.
   *
   * The `combine` function is used to compose the materialized values of the `sink` and `source`
   * into the materialized value of the resulting [[Flow]].
   */
  def fromSinkAndSourceCoupledMat[I, O, M1, M2, M](sink: Graph[SinkShape[I], M1], source: Graph[SourceShape[O], M2])(
      combine: (M1, M2) => M): Flow[I, O, M] =
    // format: OFF
    Flow.fromGraph(GraphDSL.createGraph(sink, source)(combine) { implicit b => (i, o) =>
      import GraphDSL.Implicits._
      val bidi = b.add(new CoupledTerminationBidi[I, O])
      /* bidi.in1 ~> */ bidi.out1 ~> i; o ~> bidi.in2 /* ~> bidi.out2 */
      FlowShape(bidi.in1, bidi.out2)
    })
  // format: ON

  /**
   * Creates a real `Flow` upon receiving the first element. Internal `Flow` will not be created
   * if there are no elements, because of completion, cancellation, or error.
   *
   * The materialized value of the `Flow` is the value that is created by the `fallback` function.
   *
   * '''Emits when''' the internal flow is successfully created and it emits
   *
   * '''Backpressures when''' the internal flow is successfully created and it backpressures
   *
   * '''Completes when''' upstream completes and all elements have been emitted from the internal flow
   *
   * '''Cancels when''' downstream cancels (see below)
   *
   * The operator's default behaviour in case of downstream cancellation before nested flow materialization (future completion) is to cancel immediately.
   *  This behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy.PropagateToNested]] attribute,
   * this will delay downstream cancellation until nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   */
  @deprecated(
    "Use 'Flow.futureFlow' in combination with prefixAndTail(1) instead, see `futureFlow` operator docs for details",
    "2.6.0")
  def lazyInit[I, O, M](flowFactory: I => Future[Flow[I, O, M]], fallback: () => M): Flow[I, O, M] =
    Flow[I]
      .flatMapPrefix(1) {
        case Seq(a) => futureFlow(flowFactory(a)).mapMaterializedValue(_ => NotUsed)
        case Nil    => Flow[I].asInstanceOf[Flow[I, O, NotUsed]]
        case _      => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
      }
      .mapMaterializedValue(_ => fallback())

  /**
   * Creates a real `Flow` upon receiving the first element. Internal `Flow` will not be created
   * if there are no elements, because of completion, cancellation, or error.
   *
   * The materialized value of the `Flow` is a `Future[Option[M]]` that is completed with `Some(mat)` when the internal
   * flow gets materialized or with `None` when there where no elements. If the flow materialization (including
   * the call of the `flowFactory`) fails then the future is completed with a failure.
   *
   * '''Emits when''' the internal flow is successfully created and it emits
   *
   * '''Backpressures when''' the internal flow is successfully created and it backpressures
   *
   * '''Completes when''' upstream completes and all elements have been emitted from the internal flow
   *
   * '''Cancels when''' downstream cancels (see below)
   *
   * The operator's default behaviour in case of downstream cancellation before nested flow materialization (future completion) is to cancel immediately.
   *  This behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy.PropagateToNested]] attribute,
   * this will delay downstream cancellation until nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   */
  @deprecated("Use 'Flow.lazyFutureFlow' instead", "2.6.0")
  def lazyInitAsync[I, O, M](flowFactory: () => Future[Flow[I, O, M]]): Flow[I, O, Future[Option[M]]] =
    Flow.lazyFutureFlow(flowFactory).mapMaterializedValue {
      implicit val ec = ExecutionContext.parasitic
      _.map(Some.apply).recover { case _: NeverMaterializedException => None }
    }

  /**
   * Turn a `Future[Flow]` into a flow that will consume the values of the source when the future completes successfully.
   * If the `Future` is completed with a failure the stream is failed.
   *
   * The materialized future value is completed with the materialized value of the future flow or failed with a
   * [[NeverMaterializedException]] if upstream fails or downstream cancels before the future has completed.
   *
   * The operator's default behaviour in case of downstream cancellation before nested flow materialization (future completion) is to cancel immediately.
   *  This behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy.PropagateToNested]] attribute,
   * this will delay downstream cancellation until nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   */
  def futureFlow[I, O, M](flow: Future[Flow[I, O, M]]): Flow[I, O, Future[M]] =
    Flow.fromGraph(new FutureFlow(flow))

  /**
   * Defers invoking the `create` function to create a future flow until there is downstream demand and passing
   * that downstream demand upstream triggers the first element.
   *
   * The materialized future value is completed with the materialized value of the created flow when that has successfully
   * been materialized.
   *
   * If the `create` function throws or returns a future that fails the stream is failed, in this case the materialized
   * future value is failed with a [[NeverMaterializedException]].
   *
   * Note that asynchronous boundaries (and other operators) in the stream may do pre-fetching which counter acts
   * the laziness and can trigger the factory earlier than expected.
   *
   * '''Emits when''' the internal flow is successfully created and it emits
   *
   * '''Backpressures when''' the internal flow is successfully created and it backpressures or downstream backpressures
   *
   * '''Completes when''' upstream completes and all elements have been emitted from the internal flow
   *
   * '''Cancels when''' downstream cancels (see below)
   *
   * The operator's default behaviour in case of downstream cancellation before nested flow materialization (future completion) is to cancel immediately.
   *  This behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy.PropagateToNested]] attribute,
   * this will delay downstream cancellation until nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   */
  def lazyFlow[I, O, M](create: () => Flow[I, O, M]): Flow[I, O, Future[M]] =
    lazyFutureFlow(() => Future.successful(create())).addAttributes(Attributes(SourceLocation.forLambda(create)))

  /**
   * Defers invoking the `create` function to create a future flow until there downstream demand has caused upstream
   * to send a first element.
   *
   * The materialized future value is completed with the materialized value of the created flow when that has successfully
   * been materialized.
   *
   * If the `create` function throws or returns a future that fails the stream is failed, in this case the materialized
   * future value is failed with a [[NeverMaterializedException]].
   *
   * Note that asynchronous boundaries (and other operators) in the stream may do pre-fetching which counter acts
   * the laziness and can trigger the factory earlier than expected.
   *
   * '''Emits when''' the internal flow is successfully created and it emits
   *
   * '''Backpressures when''' the internal flow is successfully created and it backpressures or downstream backpressures
   *
   * '''Completes when''' upstream completes and all elements have been emitted from the internal flow
   *
   * '''Cancels when''' downstream cancels (see below)
   *
   * The operator's default behaviour in case of downstream cancellation before nested flow materialization (future completion) is to cancel immediately.
   *  This behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy.PropagateToNested]] attribute,
   * this will delay downstream cancellation until nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   */
  def lazyFutureFlow[I, O, M](create: () => Future[Flow[I, O, M]]): Flow[I, O, Future[M]] =
    Flow[I]
      .flatMapPrefixMat(1) {
        case Seq(a) =>
          val f: Flow[I, O, Future[M]] =
            futureFlow(
              create().map(Flow[I].prepend(Source.single(a)).viaMat(_)(Keep.right))(ExecutionContext.parasitic))
          f
        case Nil =>
          val f: Flow[I, O, Future[M]] = Flow[I]
            .asInstanceOf[Flow[I, O, NotUsed]]
            .mapMaterializedValue(_ => Future.failed[M](new NeverMaterializedException()))
          f
        case _ => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
      }(Keep.right)
      .addAttributes(Attributes(SourceLocation.forLambda(create)))
      .mapMaterializedValue(_.flatten)

}

object RunnableGraph {

  /**
   * A graph with a closed shape is logically a runnable graph, this method makes
   * it so also in type.
   */
  def fromGraph[Mat](g: Graph[ClosedShape, Mat]): RunnableGraph[Mat] =
    g match {
      case r: RunnableGraph[Mat] => r
      case other                 => RunnableGraph(other.traversalBuilder)
    }
}

/**
 * Flow with attached input and output, can be executed.
 */
final case class RunnableGraph[+Mat](override val traversalBuilder: TraversalBuilder) extends Graph[ClosedShape, Mat] {
  override def shape = ClosedShape

  /**
   * Transform only the materialized value of this RunnableGraph, leaving all other properties as they were.
   */
  def mapMaterializedValue[Mat2](f: Mat => Mat2): RunnableGraph[Mat2] =
    copy(traversalBuilder.transformMat(f.asInstanceOf[Any => Any]))

  /**
   * Run this flow and return the materialized instance from the flow.
   *
   * Note that the `ActorSystem` can be used as the implicit `materializer` parameter to use the
   * [[akka.stream.SystemMaterializer]] for running the stream.
   */
  def run()(implicit materializer: Materializer): Mat = materializer.materialize(this)

  override def addAttributes(attr: Attributes): RunnableGraph[Mat] =
    withAttributes(traversalBuilder.attributes and attr)

  override def withAttributes(attr: Attributes): RunnableGraph[Mat] =
    new RunnableGraph(traversalBuilder.setAttributes(attr))

  override def named(name: String): RunnableGraph[Mat] =
    addAttributes(Attributes.name(name))

  /**
   * Note that an async boundary around a runnable graph does not make sense
   */
  override def async: RunnableGraph[Mat] =
    super.async.asInstanceOf[RunnableGraph[Mat]]

  /**
   * Note that an async boundary around a runnable graph does not make sense
   */
  override def async(dispatcher: String): RunnableGraph[Mat] =
    super.async(dispatcher).asInstanceOf[RunnableGraph[Mat]]

  /**
   * Note that an async boundary around a runnable graph does not make sense
   */
  override def async(dispatcher: String, inputBufferSize: Int): RunnableGraph[Mat] =
    super.async(dispatcher, inputBufferSize).asInstanceOf[RunnableGraph[Mat]]

  /** Converts this Scala DSL element to it's Java DSL counterpart. */
  def asJava: javadsl.RunnableGraph[Mat] = javadsl.RunnableGraph.fromGraph(this)

  override def getAttributes: Attributes = traversalBuilder.attributes
}

/**
 * Scala API: Operations offered by Sources and Flows with a free output side: the DSL flows left-to-right only.
 *
 * INTERNAL API: this trait will be changed in binary-incompatible ways for classes that are derived from it!
 * Do not implement this interface outside the Akka code base!
 *
 * Binary compatibility is only maintained for callers of this trait’s interface.
 */
@DoNotInherit
trait FlowOps[+Out, +Mat] {
  import GraphDSL.Implicits._

  import akka.stream.impl.Stages._

  type Repr[+O] <: FlowOps[O, Mat] {
    type Repr[+OO] = FlowOps.this.Repr[OO]
    type Closed = FlowOps.this.Closed
  }

  // result of closing a Source is RunnableGraph, closing a Flow is Sink
  type Closed

  /**
   * Transform this [[Flow]] by appending the given processing steps.
   * {{{
   *     +---------------------------------+
   *     | Resulting Flow[In, T, Mat]  |
   *     |                                 |
   *     |  +------+             +------+  |
   *     |  |      |             |      |  |
   * In ~~> | this |  ~~Out~~>   | flow | ~~> T
   *     |  |   Mat|             |     M|  |
   *     |  +------+             +------+  |
   *     +---------------------------------+
   * }}}
   * The materialized value of the combined [[Flow]] will be the materialized
   * value of the current flow (ignoring the other Flow’s value), use
   * [[Flow#viaMat viaMat]] if a different strategy is needed.
   *
   * See also [[FlowOpsMat.viaMat]] when access to materialized values of the parameter is needed.
   */
  def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T]

  /**
   * Recover allows to send last element on failure and gracefully complete the stream
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * Throwing an exception inside `recover` _will_ be logged on ERROR level automatically.
   *
   * '''Emits when''' element is available from the upstream or upstream is failed and pf returns an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or upstream failed with exception pf can handle
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def recover[T >: Out](pf: PartialFunction[Throwable, T]): Repr[T] = via(Recover(pf))

  /**
   * RecoverWith allows to switch to alternative Source on flow failure. It will stay in effect after
   * a failure has been recovered so that each time there is a failure it is fed into the `pf` and a new
   * Source may be materialized.
   *
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * Throwing an exception inside `recoverWith` _will_ be logged on ERROR level automatically.
   *
   * '''Emits when''' element is available from the upstream or upstream is failed and element is available
   * from alternative Source
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or upstream failed with exception pf can handle
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def recoverWith[T >: Out](pf: PartialFunction[Throwable, Graph[SourceShape[T], NotUsed]]): Repr[T] =
    via(new RecoverWith(-1, pf))

  /**
   * onErrorComplete allows to complete the stream when an upstream error occurs.
   *
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * '''Emits when''' element is available from the upstream
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or failed with exception is an instance of the provided type
   *
   * '''Cancels when''' downstream cancels
   */
  def onErrorComplete[T <: Throwable]()(implicit tag: ClassTag[T]): Repr[Out] = onErrorComplete {
    case ex if tag.runtimeClass.isInstance(ex) => true
  }

  /**
   * onErrorComplete allows to complete the stream when an upstream error occurs.
   *
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * '''Emits when''' element is available from the upstream
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or failed with exception pf can handle
   *
   * '''Cancels when''' downstream cancels
   */
  def onErrorComplete(pf: PartialFunction[Throwable, Boolean]): Repr[Out] =
    via(
      Flow[Out]
        .recoverWith(pf.andThen({
          case true => Source.empty[Out]
        }: PartialFunction[Boolean, Graph[SourceShape[Out], NotUsed]]))
        .withAttributes(DefaultAttributes.onErrorComplete and SourceLocation.forLambda(pf)))

  /**
   * RecoverWithRetries allows to switch to alternative Source on flow failure. It will stay in effect after
   * a failure has been recovered up to `attempts` number of times so that each time there is a failure
   * it is fed into the `pf` and a new Source may be materialized. Note that if you pass in 0, this won't
   * attempt to recover at all.
   *
   * A negative `attempts` number is interpreted as "infinite", which results in the exact same behavior as `recoverWith`.
   *
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * Throwing an exception inside `recoverWithRetries` _will_ be logged on ERROR level automatically.
   *
   * '''Emits when''' element is available from the upstream or upstream is failed and element is available
   * from alternative Source
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or upstream failed with exception pf can handle
   *
   * '''Cancels when''' downstream cancels
   *
   * @param attempts Maximum number of retries or -1 to retry indefinitely
   * @param pf Receives the failure cause and returns the new Source to be materialized if any
   *
   */
  def recoverWithRetries[T >: Out](
      attempts: Int,
      pf: PartialFunction[Throwable, Graph[SourceShape[T], NotUsed]]): Repr[T] =
    via(new RecoverWith(attempts, pf))

  /**
   * While similar to [[recover]] this operator can be used to transform an error signal to a different one *without* logging
   * it as an error in the process. So in that sense it is NOT exactly equivalent to `recover(t => throw t2)` since recover
   * would log the `t2` error.
   *
   * Since the underlying failure signal onError arrives out-of-band, it might jump over existing elements.
   * This operator can recover the failure signal, but not the skipped elements, which will be dropped.
   *
   * Similarly to [[recover]] throwing an exception inside `mapError` _will_ be logged.
   *
   * '''Emits when''' element is available from the upstream or upstream is failed and pf returns an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or upstream failed with exception pf can handle
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def mapError(pf: PartialFunction[Throwable, Throwable]): Repr[Out] = via(MapError(pf))

  /**
   * Transform this stream by applying the given function to each of the elements
   * as they pass through this processing step.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the mapping function returns an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def map[T](f: Out => T): Repr[T] = via(Map(f))

  /**
   * This is a simplified version of `wireTap(Sink)` that takes only a simple function.
   * Elements will be passed into this "side channel" function, and any of its results will be ignored.
   *
   * If the wire-tap operation is slow (it backpressures), elements that would've been sent to it will be dropped instead.
   * It is similar to [[#alsoTo]] which does backpressure instead of dropping elements.
   *
   * This operation is useful for inspecting the passed through element, usually by means of side-effecting
   * operations (such as `println`, or emitting metrics), for each element without having to modify it.
   *
   * For logging signals (elements, completion, error) consider using the [[log]] operator instead,
   * along with appropriate `ActorAttributes.logLevels`.
   *
   * '''Emits when''' upstream emits an element; the same element will be passed to the attached function,
   *                  as well as to the downstream operator
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def wireTap(f: Out => Unit): Repr[Out] =
    wireTap(Sink.foreach(f)).named("wireTap")

  /**
   * Transform each input element into an `Iterable` of output elements that is
   * then flattened into the output stream.
   *
   * The returned `Iterable` MUST NOT contain `null` values,
   * as they are illegal as stream elements - according to the Reactive Streams specification.
   *
   * '''Emits when''' the mapping function returns an element or there are still remaining elements
   * from the previously calculated collection
   *
   * '''Backpressures when''' downstream backpressures or there are still remaining elements from the
   * previously calculated collection
   *
   * '''Completes when''' upstream completes and all remaining elements have been emitted
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def mapConcat[T](f: Out => IterableOnce[T]): Repr[T] = statefulMapConcat(() => f)

  /**
   * Transform each stream element with the help of a state.
   *
   * The state creation function is invoked once when the stream is materialized and the returned state is passed to
   * the mapping function for mapping the first element. The mapping function returns a mapped element to emit
   * downstream and a state to pass to the next mapping function. The state can be the same for each mapping return,
   * be a new immutable state but it is also safe to use a mutable state. The returned `T` MUST NOT be `null` as it is
   * illegal as stream element - according to the Reactive Streams specification. A `null` state is not allowed and will fail the stream.
   *
   * For stateless variant see [[FlowOps.map]].
   *
   * The `onComplete` function is called only once when the upstream or downstream finished, You can do some clean-up here,
   * and if the returned value is not empty, it will be emitted to the downstream if available, otherwise the value will be dropped.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the mapping function returns an element and downstream is ready to consume it
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * @tparam S the type of the state
   * @tparam T the type of the output elements
   * @param create a function that creates the initial state
   * @param f a function that transforms the upstream element and the state into a pair of next state and output element
   * @param onComplete a function that transforms the ongoing state into an optional output element
   */
  def statefulMap[S, T](create: () => S)(f: (S, Out) => (S, T), onComplete: S => Option[T]): Repr[T] =
    via(
      new StatefulMap[S, Out, T](create, f, onComplete)
        .withAttributes(DefaultAttributes.statefulMap and SourceLocation.forLambda(f)))

  /**
   * Transform each stream element with the help of a resource.
   *
   * The resource creation function is invoked once when the stream is materialized and the returned resource is passed to
   * the mapping function for mapping the first element. The mapping function returns a mapped element to emit
   * downstream. The returned `T` MUST NOT be `null` as it is illegal as stream element - according to the Reactive Streams specification.
   *
   * The `close` function is called only once when the upstream or downstream finishes or fails. You can do some clean-up here,
   * and if the returned value is not empty, it will be emitted to the downstream if available, otherwise the value will be dropped.
   *
   * Early completion can be done with combination of the [[takeWhile]] operator.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[ActorAttributes]].
   *
   * '''Emits when''' the mapping function returns an element and downstream is ready to consume it
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * @tparam R the type of the resource
   * @tparam T the type of the output elements
   * @param create function that creates the resource
   * @param f function that transforms the upstream element and the resource to output element
   * @param close function that closes the resource, optionally outputting a last element
   */
  def mapWithResource[R, T](create: () => R)(f: (R, Out) => T, close: R => Option[T]): Repr[T] =
    via(
      new StatefulMap[R, Out, T](create, (resource, out) => (resource, f(resource, out)), resource => close(resource))
        .withAttributes(DefaultAttributes.mapWithResource and SourceLocation.forLambda(f)))

  /**
   * Transform each input element into an `Iterable` of output elements that is
   * then flattened into the output stream. The transformation is meant to be stateful,
   * which is enabled by creating the transformation function anew for every materialization —
   * the returned function will typically close over mutable objects to store state between
   * invocations. For the stateless variant see [[FlowOps.mapConcat]].
   *
   * The returned `Iterable` MUST NOT contain `null` values,
   * as they are illegal as stream elements - according to the Reactive Streams specification.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the mapping function returns an element or there are still remaining elements
   * from the previously calculated collection
   *
   * '''Backpressures when''' downstream backpressures or there are still remaining elements from the
   * previously calculated collection
   *
   * '''Completes when''' upstream completes and all remaining elements has been emitted
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.mapConcat]]
   */
  def statefulMapConcat[T](f: () => Out => IterableOnce[T]): Repr[T] =
    via(new StatefulMapConcat(f))

  /**
   * Transform this stream by applying the given function to each of the elements
   * as they pass through this processing step. The function returns a `Future` and the
   * value of that future will be emitted downstream. The number of Futures
   * that shall run in parallel is given as the first argument to ``mapAsync``.
   * These Futures may complete in any order, but the elements that
   * are emitted downstream are in the same order as received from upstream.
   *
   * If the function `f` throws an exception or if the `Future` is completed
   * with failure and the supervision decision is [[akka.stream.Supervision.Stop]]
   * the stream will be completed with failure.
   *
   * If the function `f` throws an exception or if the `Future` is completed
   * with failure and the supervision decision is [[akka.stream.Supervision.Resume]] or
   * [[akka.stream.Supervision.Restart]] the element is dropped and the stream continues.
   *
   * If the `Future` is completed with `null`, it is ignored and the next element is processed.
   *
   * The function `f` is always invoked on the elements in the order they arrive.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the Future returned by the provided function finishes for the next element in sequence
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream
   * backpressures or the first future is not completed
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Cancels when''' downstream cancels
   *
   * @see [[#mapAsyncUnordered]] and [[#mapAsyncPartitioned]]
   */
  def mapAsync[T](parallelism: Int)(f: Out => Future[T]): Repr[T] =
    if (parallelism == 1) mapAsyncUnordered[T](parallelism = 1)(f) // optimization for parallelism 1
    else via(MapAsync(parallelism, f))

  /**
   * Transform this stream by partitioning elements based on the provided partitioner as they
   * pass through this step and then applying a given `Future`-returning function to each element
   * and its partition key.  The value of the returned future, if successful, will be emitted
   * downstream.
   *
   * The number of Futures running at any given time is bounded by the 'parallelism' and 'perPartition'
   * values.  The futures may complete in any order, but the results are emitted in the same order
   * as the corresponding elements were received.
   *
   * If the functions 'partitioner' or 'f' throw an exception, or the `Future` completes with failure,
   * supervision will be applied to determine a decision.  If the decision is [[akka.stream.Supervision.Stop]]
   * the stream will be stopped with failure; otherwise, the element will be dropped and the stream continues.
   *
   * The function 'partitioner' is always invoked on the elements in the order they arrive.
   *
   * The function 'f' is invoked on elements with the same partition key in the order they arrive.  The order
   * of invocation of 'f' for elements with different partition keys is undefined and subject to factors
   * including, but not limited to, the distribution of partition keys within the stream.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the Future returned by 'f' finishes for the next element in sequence
   *
   * '''Backpressures when''' the number of elements for which no resulting future has completed reaches the
   * configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all results have been emitted
   *
   * '''Cancels when''' downstream cancels
   *
   * @see [[#mapAsync]] and [[#mapAsyncUnordered]]
   *
   * @param parallelism at most this many futures will be incomplete at any time
   * @param perPartition at most this many futures will be incomplete for a given partition key at any time
   * @param partitioner function to generate a partition key
   * @param f function to generate a Future
   */
  def mapAsyncPartitioned[T, P](parallelism: Int, perPartition: Int)(partitioner: Out => P)(
      f: (Out, P) => Future[T]): Repr[T] =
    if (parallelism == 1) mapAsyncUnordered[T](parallelism = 1) { elem =>
      f(elem, partitioner(elem))
    } else via(MapAsyncPartitioned(parallelism, perPartition, partitioner, f))

  /**
   * Transform this stream by applying the given function to each of the elements
   * as they pass through this processing step. The function returns a `Future` and the
   * value of that future will be emitted downstream. The number of Futures
   * that shall run in parallel is given as the first argument to ``mapAsyncUnordered``.
   * Each processed element will be emitted downstream as soon as it is ready, i.e. it is possible
   * that the elements are not emitted downstream in the same order as received from upstream.
   *
   * If the function `f` throws an exception or if the `Future` is completed
   * with failure and the supervision decision is [[akka.stream.Supervision.Stop]]
   * the stream will be completed with failure.
   *
   * If the function `f` throws an exception or if the `Future` is completed
   * with failure and the supervision decision is [[akka.stream.Supervision.Resume]] or
   * [[akka.stream.Supervision.Restart]] the element is dropped and the stream continues.
   *
   * If the `Future` is completed with `null`, it is ignored and the next element is processed.
   *
   * The function `f` is always invoked on the elements in the order they arrive (even though the result of the futures
   * returned by `f` might be emitted in a different order).
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' any of the Futures returned by the provided function complete
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Cancels when''' downstream cancels
   *
   * @see [[#mapAsync]] and [[#mapAsyncPartitioned]]
   */
  def mapAsyncUnordered[T](parallelism: Int)(f: Out => Future[T]): Repr[T] = via(MapAsyncUnordered(parallelism, f))

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor.
   * If any of the asks times out it will fail the stream with a [[akka.pattern.AskTimeoutException]].
   *
   * Do not forget to include the expected response type in the method call, like so:
   *
   * {{{
   * flow.ask[ExpectedReply](ref)
   * }}}
   *
   * otherwise `Nothing` will be assumed, which is most likely not what you want.
   *
   * Defaults to parallelism of 2 messages in flight, since while one ask message may be being worked on, the second one
   * still be in the mailbox, so defaulting to sending the second one a bit earlier than when first ask has replied maintains
   * a slightly healthier throughput.
   *
   * Similar to the plain ask pattern, the target actor is allowed to reply with `akka.util.Status`.
   * An `akka.util.Status#Failure` will cause the operator to fail with the cause carried in the `Failure` message.
   *
   * The operator fails with an [[akka.stream.WatchedActorTerminatedException]] if the target actor is terminated.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the futures (in submission order) created by the ask pattern internally are completed
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Fails when''' the passed in actor terminates, or a timeout is exceeded in any of the asks performed
   *
   * '''Cancels when''' downstream cancels
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() operator")
  def ask[S](ref: ActorRef)(implicit timeout: Timeout, tag: ClassTag[S]): Repr[S] =
    ask(2)(ref)(timeout, tag)

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor.
   * If any of the asks times out it will fail the stream with a [[akka.pattern.AskTimeoutException]].
   *
   * Do not forget to include the expected response type in the method call, like so:
   *
   * {{{
   * flow.ask[ExpectedReply](parallelism = 4)(ref)
   * }}}
   *
   * otherwise `Nothing` will be assumed, which is most likely not what you want.
   *
   * Parallelism limits the number of how many asks can be "in flight" at the same time.
   * Please note that the elements emitted by this operator are in-order with regards to the asks being issued
   * (i.e. same behaviour as mapAsync).
   *
   * The operator fails with an [[akka.stream.WatchedActorTerminatedException]] if the target actor is terminated,
   * or with an [[java.util.concurrent.TimeoutException]] in case the ask exceeds the timeout passed in.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the futures (in submission order) created by the ask pattern internally are completed
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Fails when''' the passed in actor terminates, or a timeout is exceeded in any of the asks performed
   *
   * '''Cancels when''' downstream cancels
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() operator")
  def ask[S](parallelism: Int)(ref: ActorRef)(implicit timeout: Timeout, tag: ClassTag[S]): Repr[S] = {
    val askFlow = Flow[Out]
      .watch(ref)
      .mapAsync(parallelism) { el =>
        akka.pattern.ask(ref).?(el)(timeout)
      }
      .map {
        case e: S => e
        case o =>
          throw new ClassCastException(
            s"'Flow.ask' failed: expected response of type [${tag.runtimeClass}], got [${o.getClass}]")
      }
      .mapError {
        // the purpose of this recovery is to change the name of the stage in that exception
        // we do so in order to help users find which stage caused the failure -- "the ask stage"
        case ex: WatchedActorTerminatedException =>
          throw new WatchedActorTerminatedException("ask()", ex.ref)
      }
      .named("ask")

    via(askFlow)
  }

  /**
   * The operator fails with an [[akka.stream.WatchedActorTerminatedException]] if the target actor is terminated.
   *
   * '''Emits when''' upstream emits
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Fails when''' the watched actor terminates
   *
   * '''Cancels when''' downstream cancels
   */
  def watch(ref: ActorRef): Repr[Out] =
    via(Watch(ref))

  /**
   * Only pass on those elements that satisfy the given predicate.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the given predicate returns true for the element
   *
   * '''Backpressures when''' the given predicate returns true for the element and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def filter(p: Out => Boolean): Repr[Out] = via(Filter(p))

  /**
   * Only pass on those elements that NOT satisfy the given predicate.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the given predicate returns false for the element
   *
   * '''Backpressures when''' the given predicate returns false for the element and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def filterNot(p: Out => Boolean): Repr[Out] =
    via(Flow[Out].filter(!p(_)).withAttributes(DefaultAttributes.filterNot and SourceLocation.forLambda(p)))

  /**
   * Terminate processing (and cancel the upstream publisher) after predicate
   * returns false for the first time,
   * Due to input buffering some elements may have been requested from upstream publishers
   * that will then not be processed downstream of this step.
   *
   * The stream will be completed without producing any elements if predicate is false for
   * the first stream element.
   *
   * '''Emits when''' the predicate is true
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' predicate returned false (or 1 after predicate returns false if `inclusive` or upstream completes
   *
   * '''Cancels when''' predicate returned false or downstream cancels
   *
   * See also [[FlowOps.limit]], [[FlowOps.limitWeighted]]
   */
  def takeWhile(p: Out => Boolean): Repr[Out] = takeWhile(p, inclusive = false)

  /**
   * Terminate processing (and cancel the upstream publisher) after predicate
   * returns false for the first time, including the first failed element iff inclusive is true
   * Due to input buffering some elements may have been requested from upstream publishers
   * that will then not be processed downstream of this step.
   *
   * The stream will be completed without producing any elements if predicate is false for
   * the first stream element.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the predicate is true
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' predicate returned false (or 1 after predicate returns false if `inclusive` or upstream completes
   *
   * '''Cancels when''' predicate returned false or downstream cancels
   *
   * See also [[FlowOps.limit]], [[FlowOps.limitWeighted]]
   */
  def takeWhile(p: Out => Boolean, inclusive: Boolean): Repr[Out] = via(TakeWhile(p, inclusive))

  /**
   * Discard elements at the beginning of the stream while predicate is true.
   * All elements will be taken after predicate returns false first time.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' predicate returned false and for all following stream elements
   *
   * '''Backpressures when''' predicate returned false and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def dropWhile(p: Out => Boolean): Repr[Out] = via(DropWhile(p))

  /**
   * Transform this stream by applying the given partial function to each of the elements
   * on which the function is defined as they pass through this processing step.
   * Non-matching elements are filtered out.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the provided partial function is defined for the element
   *
   * '''Backpressures when''' the partial function is defined for the element and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def collect[T](pf: PartialFunction[Out, T]): Repr[T] = via(Collect(pf))

  /**
   * Transform this stream by testing the type of each of the elements
   * on which the element is an instance of the provided type as they pass through this processing step.
   *
   * Non-matching elements are filtered out.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the element is an instance of the provided type
   *
   * '''Backpressures when''' the element is an instance of the provided type and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def collectType[T](implicit tag: ClassTag[T]): Repr[T] =
    collect { case c if tag.runtimeClass.isInstance(c) => c.asInstanceOf[T] }

  /**
   * Chunk up this stream into groups of the given size, with the last group
   * possibly smaller than requested due to end-of-stream.
   *
   * `n` must be positive, otherwise IllegalArgumentException is thrown.
   *
   * '''Emits when''' the specified number of elements have been accumulated or upstream completed
   *
   * '''Backpressures when''' a group has been assembled and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def grouped(n: Int): Repr[immutable.Seq[Out]] =
    via(GroupedWeighted[Out](n, ConstantFun.oneLong).withAttributes(DefaultAttributes.grouped))

  /**
   * Chunk up this stream into groups of elements that have a cumulative weight greater than or equal to
   * the `minWeight`, with the last group possibly smaller than requested `minWeight` due to end-of-stream.
   *
   * `minWeight` must be positive, otherwise IllegalArgumentException is thrown.
   * `costFn` must return a non-negative result for all inputs, otherwise the stage will fail
   * with an IllegalArgumentException.
   *
   * '''Emits when''' the cumulative weight of elements is greater than or equal to the `minWeight` or upstream completed
   *
   * '''Backpressures when''' a buffered group weighs more than `minWeight` and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def groupedWeighted(minWeight: Long)(costFn: Out => Long): Repr[immutable.Seq[Out]] =
    via(GroupedWeighted[Out](minWeight, costFn))

  /**
   * Ensure stream boundedness by limiting the number of elements from upstream.
   * If the number of incoming elements exceeds max, it will signal
   * upstream failure `StreamLimitException` downstream.
   *
   * Due to input buffering some elements may have been
   * requested from upstream publishers that will then not be processed downstream
   * of this step.
   *
   * '''Emits when''' upstream emits and the number of emitted elements has not reached max
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes and the number of emitted elements has not reached max
   *
   * '''Errors when''' the total number of incoming element exceeds max
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.take]], [[FlowOps.takeWithin]], [[FlowOps.takeWhile]]
   */
  def limit(max: Long): Repr[Out] = limitWeighted(max)(_ => 1)

  /**
   * Ensure stream boundedness by evaluating the cost of incoming elements
   * using a cost function. Exactly how many elements will be allowed to travel downstream depends on the
   * evaluated cost of each element. If the accumulated cost exceeds max, it will signal
   * upstream failure `StreamLimitException` downstream.
   *
   * Due to input buffering some elements may have been
   * requested from upstream publishers that will then not be processed downstream
   * of this step.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' upstream emits and the accumulated cost has not reached max
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes and the number of emitted elements has not reached max
   *
   * '''Errors when''' when the accumulated cost exceeds max
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.take]], [[FlowOps.takeWithin]], [[FlowOps.takeWhile]]
   */
  def limitWeighted[T](max: Long)(costFn: Out => Long): Repr[Out] = via(LimitWeighted(max, costFn))

  /**
   * Apply a sliding window over the stream and return the windows as groups of elements, with the last group
   * possibly smaller than requested due to end-of-stream.
   *
   * `n` must be positive, otherwise IllegalArgumentException is thrown.
   * `step` must be positive, otherwise IllegalArgumentException is thrown.
   *
   * '''Emits when''' enough elements have been collected within the window or upstream completed
   *
   * '''Backpressures when''' a window has been assembled and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def sliding(n: Int, step: Int = 1): Repr[immutable.Seq[Out]] = via(Sliding(n, step))

  /**
   * Similar to `fold` but is not a terminal operation,
   * emits its current value which starts at `zero` and then
   * applies the current and next value to the given function `f`,
   * emitting the next current value.
   *
   * If the function `f` throws an exception and the supervision decision is
   * [[akka.stream.Supervision.Restart]] current value starts at `zero` again
   * the stream will continue.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * Note that the `zero` value must be immutable.
   *
   * '''Emits when''' the function scanning the element returns a new element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.scanAsync]]
   */
  def scan[T](zero: T)(f: (T, Out) => T): Repr[T] = via(Scan(zero, f))

  /**
   * Similar to `scan` but with an asynchronous function,
   * emits its current value which starts at `zero` and then
   * applies the current and next value to the given function `f`,
   * emitting a `Future` that resolves to the next current value.
   *
   * If the function `f` throws an exception and the supervision decision is
   * [[akka.stream.Supervision.Restart]] current value starts at `zero` again
   * the stream will continue.
   *
   * If the function `f` throws an exception and the supervision decision is
   * [[akka.stream.Supervision.Resume]] current value starts at the previous
   * current value, or zero when it doesn't have one, and the stream will continue.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * Note that the `zero` value must be immutable.
   *
   * '''Emits when''' the future returned by `f` completes
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes and the last future returned by `f` completes
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.scan]]
   */
  def scanAsync[T](zero: T)(f: (T, Out) => Future[T]): Repr[T] = via(ScanAsync(zero, f))

  /**
   * Similar to `scan` but only emits its result when the upstream completes,
   * after which it also completes. Applies the given function towards its current and next value,
   * yielding the next current value.
   *
   * If the function `f` throws an exception and the supervision decision is
   * [[akka.stream.Supervision.Restart]] current value starts at `zero` again
   * the stream will continue.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * Note that the `zero` value must be immutable.
   *
   * '''Emits when''' upstream completes
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.scan]]
   */
  def fold[T](zero: T)(f: (T, Out) => T): Repr[T] = via(Fold(zero, f))

  /**
   * Similar to `fold` but with an asynchronous function.
   * Applies the given function towards its current and next value,
   * yielding the next current value.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * If the function `f` returns a failure and the supervision decision is
   * [[akka.stream.Supervision.Restart]] current value starts at `zero` again
   * the stream will continue.
   *
   * Note that the `zero` value must be immutable.
   *
   * '''Emits when''' upstream completes
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.fold]]
   */
  def foldAsync[T](zero: T)(f: (T, Out) => Future[T]): Repr[T] = via(new FoldAsync(zero, f))

  /**
   * Similar to `fold` but uses first element as zero element.
   * Applies the given function towards its current and next value,
   * yielding the next current value.
   *
   * If the stream is empty (i.e. completes before signalling any elements),
   * the reduce operator will fail its downstream with a [[NoSuchElementException]],
   * which is semantically in-line with that Scala's standard library collections
   * do in such situations.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' upstream completes
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.fold]]
   */
  def reduce[T >: Out](f: (T, T) => T): Repr[T] = via(new Reduce[T](f))

  /**
   * Intersperses stream with provided element, similar to how [[scala.collection.immutable.List.mkString]]
   * injects a separator between a List's elements.
   *
   * Additionally can inject start and end marker elements to stream.
   *
   * Examples:
   *
   * {{{
   * val nums = Source(List(1,2,3)).map(_.toString)
   * nums.intersperse(",")            //   1 , 2 , 3
   * nums.intersperse("[", ",", "]")  // [ 1 , 2 , 3 ]
   * }}}
   *
   * In case you want to only prepend or only append an element (yet still use the `intercept` feature
   * to inject a separator between elements, you may want to use the following pattern instead of the 3-argument
   * version of intersperse (See [[Source.concat]] for semantics details):
   *
   * {{{
   * Source.single(">> ") ++ Source(List("1", "2", "3")).intersperse(",")
   * Source(List("1", "2", "3")).intersperse(",") ++ Source.single("END")
   * }}}
   *
   * '''Emits when''' upstream emits (or before with the `start` element if provided)
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def intersperse[T >: Out](start: T, inject: T, end: T): Repr[T] =
    via(Intersperse(Some(start), inject, Some(end)))

  /**
   * Intersperses stream with provided element, similar to how [[scala.collection.immutable.List.mkString]]
   * injects a separator between a List's elements.
   *
   * Additionally can inject start and end marker elements to stream.
   *
   * Examples:
   *
   * {{{
   * val nums = Source(List(1,2,3)).map(_.toString)
   * nums.intersperse(",")            //   1 , 2 , 3
   * nums.intersperse("[", ",", "]")  // [ 1 , 2 , 3 ]
   * }}}
   *
   * '''Emits when''' upstream emits (or before with the `start` element if provided)
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def intersperse[T >: Out](inject: T): Repr[T] =
    via(Intersperse(None, inject, None))

  /**
   * Chunk up this stream into groups of elements received within a time window,
   * or limited by the given number of elements, whatever happens first.
   * Empty groups will not be emitted if no elements are received from upstream.
   * The last group before end-of-stream will contain the buffered elements
   * since the previously emitted group.
   *
   * `n` must be positive, and `d` must be greater than 0 seconds, otherwise
   * IllegalArgumentException is thrown.
   *
   * '''Emits when''' the configured time elapses since the last group has been emitted or `n` elements is buffered
   *
   * '''Backpressures when''' downstream backpressures, and there are `n+1` buffered elements
   *
   * '''Completes when''' upstream completes (emits last group)
   *
   * '''Cancels when''' downstream completes
   */
  def groupedWithin(n: Int, d: FiniteDuration): Repr[immutable.Seq[Out]] =
    via(
      new GroupedWeightedWithin[Out](Long.MaxValue, n, ConstantFun.zeroLong, d)
        .withAttributes(DefaultAttributes.groupedWithin))

  /**
   * Chunk up this stream into groups of elements received within a time window,
   * or limited by the weight of the elements, whatever happens first.
   * Empty groups will not be emitted if no elements are received from upstream.
   * The last group before end-of-stream will contain the buffered elements
   * since the previously emitted group.
   *
   * `maxWeight` must be positive, and `d` must be greater than 0 seconds, otherwise
   * IllegalArgumentException is thrown.
   *
   * '''Emits when''' the configured time elapses since the last group has been emitted or weight limit reached
   *
   * '''Backpressures when''' downstream backpressures, and buffered group (+ pending element) weighs more than `maxWeight`
   *
   * '''Completes when''' upstream completes (emits last group)
   *
   * '''Cancels when''' downstream completes
   */
  def groupedWeightedWithin(maxWeight: Long, d: FiniteDuration)(costFn: Out => Long): Repr[immutable.Seq[Out]] =
    via(new GroupedWeightedWithin[Out](maxWeight, Int.MaxValue, costFn, d))

  /**
   * Chunk up this stream into groups of elements received within a time window,
   * or limited by the weight and number of the elements, whatever happens first.
   * Empty groups will not be emitted if no elements are received from upstream.
   * The last group before end-of-stream will contain the buffered elements
   * since the previously emitted group.
   *
   * `maxWeight` must be positive, `maxNumber` must be positive, and `d` must be greater than 0 seconds,
   * otherwise IllegalArgumentException is thrown.
   *
   * '''Emits when''' the configured time elapses since the last group has been emitted or weight limit reached
   *
   * '''Backpressures when''' downstream backpressures, and buffered group (+ pending element) weighs more than
   * `maxWeight` or has more than `maxNumber` elements
   *
   * '''Completes when''' upstream completes (emits last group)
   *
   * '''Cancels when''' downstream completes
   */
  def groupedWeightedWithin(maxWeight: Long, maxNumber: Int, d: FiniteDuration)(
      costFn: Out => Long): Repr[immutable.Seq[Out]] =
    via(new GroupedWeightedWithin[Out](maxWeight, maxNumber, costFn, d))

  /**
   * Shifts elements emission in time by a specified amount. It allows to store elements
   * in internal buffer while waiting for next element to be emitted. Depending on the defined
   * [[akka.stream.DelayOverflowStrategy]] it might drop elements or backpressure the upstream if
   * there is no space available in the buffer.
   *
   * Delay precision is 10ms to avoid unnecessary timer scheduling cycles
   *
   * Internal buffer has default capacity 16. You can set buffer size by calling `addAttributes(inputBuffer)`
   *
   * '''Emits when''' there is a pending element in the buffer and configured time for this element elapsed
   *  * EmitEarly - strategy do not wait to emit element if buffer is full
   *
   * '''Backpressures when''' depending on OverflowStrategy
   *  * Backpressure - backpressures when buffer is full
   *  * DropHead, DropTail, DropBuffer - never backpressures
   *  * Fail - fails the stream if buffer gets full
   *
   * '''Completes when''' upstream completes and buffered elements have been drained
   *
   * '''Cancels when''' downstream cancels
   *
   * @param of time to shift all messages
   * @param strategy Strategy that is used when incoming elements cannot fit inside the buffer
   */
  def delay(of: FiniteDuration, strategy: DelayOverflowStrategy = DelayOverflowStrategy.dropTail): Repr[Out] = {
    val fixedDelay = DelayStrategy.fixedDelay(of)
    via(new Delay[Out](() => fixedDelay, strategy))
  }

  /**
   * Shifts elements emission in time by an amount individually determined through delay strategy a specified amount.
   * It allows to store elements in internal buffer while waiting for next element to be emitted. Depending on the defined
   * [[akka.stream.DelayOverflowStrategy]] it might drop elements or backpressure the upstream if
   * there is no space available in the buffer.
   *
   * It determines delay for each ongoing element invoking `DelayStrategy.nextDelay(elem: T): FiniteDuration`.
   *
   * Note that elements are not re-ordered: if an element is given a delay much shorter than its predecessor,
   * it will still have to wait for the preceding element before being emitted.
   * It is also important to notice that [[scaladsl.DelayStrategy]] can be stateful.
   *
   * Delay precision is 10ms to avoid unnecessary timer scheduling cycles.
   *
   * Internal buffer has default capacity 16. You can set buffer size by calling `addAttributes(inputBuffer)`
   *
   * '''Emits when''' there is a pending element in the buffer and configured time for this element elapsed
   *  * EmitEarly - strategy do not wait to emit element if buffer is full
   *
   * '''Backpressures when''' depending on OverflowStrategy
   *  * Backpressure - backpressures when buffer is full
   *  * DropHead, DropTail, DropBuffer - never backpressures
   *  * Fail - fails the stream if buffer gets full
   *
   * '''Completes when''' upstream completes and buffered elements have been drained
   *
   * '''Cancels when''' downstream cancels
   *
   * @param delayStrategySupplier creates new [[DelayStrategy]] object for each materialization
   * @param overFlowStrategy Strategy that is used when incoming elements cannot fit inside the buffer
   */
  def delayWith(delayStrategySupplier: () => DelayStrategy[Out], overFlowStrategy: DelayOverflowStrategy): Repr[Out] =
    via(new Delay[Out](delayStrategySupplier, overFlowStrategy))

  /**
   * Discard the given number of elements at the beginning of the stream.
   * No elements will be dropped if `n` is zero or negative.
   *
   * '''Emits when''' the specified number of elements has been dropped already
   *
   * '''Backpressures when''' the specified number of elements has been dropped and downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def drop(n: Long): Repr[Out] =
    via(Drop[Out](n))

  /**
   * Discard the elements received within the given duration at beginning of the stream.
   *
   * '''Emits when''' the specified time elapsed and a new upstream element arrives
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def dropWithin(d: FiniteDuration): Repr[Out] =
    via(new DropWithin[Out](d))

  /**
   * Terminate processing (and cancel the upstream publisher) after the given
   * number of elements. Due to input buffering some elements may have been
   * requested from upstream publishers that will then not be processed downstream
   * of this step.
   *
   * The stream will be completed without producing any elements if `n` is zero
   * or negative.
   *
   * '''Emits when''' the specified number of elements to take has not yet been reached
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' the defined number of elements has been taken or upstream completes
   *
   * '''Cancels when''' the defined number of elements has been taken or downstream cancels
   *
   * See also [[FlowOps.limit]], [[FlowOps.limitWeighted]]
   */
  def take(n: Long): Repr[Out] =
    via(Take[Out](n))

  /**
   * Terminate processing (and cancel the upstream publisher) after the given
   * duration. Due to input buffering some elements may have been
   * requested from upstream publishers that will then not be processed downstream
   * of this step.
   *
   * Note that this can be combined with [[#take]] to limit the number of elements
   * within the duration.
   *
   * '''Emits when''' an upstream element arrives
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or timer fires
   *
   * '''Cancels when''' downstream cancels or timer fires
   */
  def takeWithin(d: FiniteDuration): Repr[Out] = via(new TakeWithin[Out](d))

  /**
   * Allows a faster upstream to progress independently of a slower subscriber by conflating elements into a summary
   * until the subscriber is ready to accept them. For example a conflate step might average incoming numbers if the
   * upstream publisher is faster.
   *
   * This version of conflate allows to derive a seed from the first element and change the aggregated type to be
   * different than the input type. See [[FlowOps.conflate]] for a simpler version that does not change types.
   *
   * This element only rolls up elements if the upstream is faster, but if the downstream is faster it will not
   * duplicate elements.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' downstream stops backpressuring and there is a conflated element available
   *
   * '''Backpressures when''' never
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * @param seed Provides the first state for a conflated value using the first unconsumed element as a start
   * @param aggregate Takes the currently aggregated value and the current pending element to produce a new aggregate
   *
   * See also [[FlowOps.conflate]], [[FlowOps.limit]], [[FlowOps.limitWeighted]] [[FlowOps.batch]] [[FlowOps.batchWeighted]]
   */
  def conflateWithSeed[S](seed: Out => S)(aggregate: (S, Out) => S): Repr[S] =
    via(
      Batch(1L, ConstantFun.zeroLong, seed, aggregate)
        .withAttributes(DefaultAttributes.conflate and SourceLocation.forLambda(aggregate)))

  /**
   * Allows a faster upstream to progress independently of a slower subscriber by conflating elements into a summary
   * until the subscriber is ready to accept them. For example a conflate step might average incoming numbers if the
   * upstream publisher is faster.
   *
   * This version of conflate does not change the output type of the stream. See [[FlowOps.conflateWithSeed]] for a
   * more flexible version that can take a seed function and transform elements while rolling up.
   *
   * This element only rolls up elements if the upstream is faster, but if the downstream is faster it will not
   * duplicate elements.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' downstream stops backpressuring and there is a conflated element available
   *
   * '''Backpressures when''' never
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * @param aggregate Takes the currently aggregated value and the current pending element to produce a new aggregate
   *
   * See also [[FlowOps.conflate]], [[FlowOps.limit]], [[FlowOps.limitWeighted]] [[FlowOps.batch]] [[FlowOps.batchWeighted]]
   */
  def conflate[O2 >: Out](aggregate: (O2, O2) => O2): Repr[O2] =
    conflateWithSeed[O2](ConstantFun.scalaIdentityFunction)(aggregate)

  /**
   * Allows a faster upstream to progress independently of a slower subscriber by aggregating elements into batches
   * until the subscriber is ready to accept them. For example a batch step might store received elements in
   * an array up to the allowed max limit if the upstream publisher is faster.
   *
   * This only rolls up elements if the upstream is faster, but if the downstream is faster it will not
   * duplicate elements.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' downstream stops backpressuring and there is an aggregated element available
   *
   * '''Backpressures when''' there are `max` batched elements and 1 pending element and downstream backpressures
   *
   * '''Completes when''' upstream completes and there is no batched/pending element waiting
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.conflateWithSeed]], [[FlowOps.batchWeighted]]
   *
   * @param max maximum number of elements to batch before backpressuring upstream (must be positive non-zero)
   * @param seed Provides the first state for a batched value using the first unconsumed element as a start
   * @param aggregate Takes the currently batched value and the current pending element to produce a new aggregate
   */
  def batch[S](max: Long, seed: Out => S)(aggregate: (S, Out) => S): Repr[S] =
    via(
      Batch(max, ConstantFun.oneLong, seed, aggregate)
        .withAttributes(DefaultAttributes.batch and SourceLocation.forLambda(aggregate)))

  /**
   * Allows a faster upstream to progress independently of a slower subscriber by aggregating elements into batches
   * until the subscriber is ready to accept them. For example a batch step might concatenate `ByteString`
   * elements up to the allowed max limit if the upstream publisher is faster.
   *
   * This element only rolls up elements if the upstream is faster, but if the downstream is faster it will not
   * duplicate elements.
   *
   * Batching will apply for all elements, even if a single element cost is greater than the total allowed limit.
   * In this case, previous batched elements will be emitted, then the "heavy" element will be emitted (after
   * being applied with the `seed` function) without batching further elements with it, and then the rest of the
   * incoming elements are batched.
   *
   * '''Emits when''' downstream stops backpressuring and there is a batched element available
   *
   * '''Backpressures when''' there are `max` weighted batched elements + 1 pending element and downstream backpressures
   *
   * '''Completes when''' upstream completes and there is no batched/pending element waiting
   *
   * '''Cancels when''' downstream cancels
   *
   * See also [[FlowOps.conflateWithSeed]], [[FlowOps.batch]]
   *
   * @param max maximum weight of elements to batch before backpressuring upstream (must be positive non-zero)
   * @param costFn a function to compute a single element weight
   * @param seed Provides the first state for a batched value using the first unconsumed element as a start
   * @param aggregate Takes the currently batched value and the current pending element to produce a new batch
   */
  def batchWeighted[S](max: Long, costFn: Out => Long, seed: Out => S)(aggregate: (S, Out) => S): Repr[S] =
    via(
      Batch(max, costFn, seed, aggregate).withAttributes(
        DefaultAttributes.batchWeighted and SourceLocation.forLambda(aggregate)))

  /**
   * Allows a faster downstream to progress independently of a slower upstream by extrapolating elements from an older
   * element until new element comes from the upstream. For example an expand step might repeat the last element for
   * the subscriber until it receives an update from upstream.
   *
   * This element will never "drop" upstream elements as all elements go through at least one extrapolation step.
   * This means that if the upstream is actually faster than the upstream it will be backpressured by the downstream
   * subscriber.
   *
   * Expand does not support [[akka.stream.Supervision.Restart]] and [[akka.stream.Supervision.Resume]].
   * Exceptions from the `seed` function will complete the stream with failure.
   *
   * '''Emits when''' downstream stops backpressuring
   *
   * '''Backpressures when''' downstream backpressures or iterator runs empty
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   * @param expander       Takes the current extrapolation state to produce an output element and the next extrapolation
   *                       state.
   * @see [[#extrapolate]] for a version that always preserves the original element and allows for an initial "startup"
   *                       element.
   */
  def expand[U](expander: Out => Iterator[U]): Repr[U] = via(new Expand(expander))

  /**
   * Allows a faster downstream to progress independent of a slower upstream.
   *
   * This is achieved by introducing "extrapolated" elements - based on those from upstream - whenever downstream
   * signals demand.
   *
   * Extrapolate does not support [[akka.stream.Supervision.Restart]] and [[akka.stream.Supervision.Resume]].
   * Exceptions from the `extrapolate` function will complete the stream with failure.
   *
   * '''Emits when''' downstream stops backpressuring, AND EITHER upstream emits OR initial element is present OR
   * `extrapolate` is non-empty and applicable
   *
   * '''Backpressures when''' downstream backpressures or current `extrapolate` runs empty
   *
   * '''Completes when''' upstream completes and current `extrapolate` runs empty
   *
   * '''Cancels when''' downstream cancels
   *
   * @param extrapolator takes the current upstream element and provides a sequence of "extrapolated" elements based
   *                    on the original, to be emitted in case downstream signals demand.
   * @param initial the initial element to be emitted, in case upstream is able to stall the entire stream.
   * @see [[#expand]]    for a version that can overwrite the original element.
   */
  def extrapolate[U >: Out](extrapolator: U => Iterator[U], initial: Option[U] = None): Repr[U] = {
    val expandArg = (u: U) => Iterator.single(u) ++ extrapolator(u)

    val expandStep = new Expand[U, U](expandArg)

    initial.map(e => prepend(Source.single(e)).via(expandStep)).getOrElse(via(expandStep))
  }

  /**
   * Adds a fixed size buffer in the flow that allows to store elements from a faster upstream until it becomes full.
   * Depending on the defined [[akka.stream.OverflowStrategy]] it might drop elements or backpressure the upstream if
   * there is no space available
   *
   * '''Emits when''' downstream stops backpressuring and there is a pending element in the buffer
   *
   * '''Backpressures when''' downstream backpressures or depending on OverflowStrategy:
   *  <ul>
   *    <li>Backpressure - backpressures when buffer is full</li>
   *    <li>DropHead, DropTail, DropBuffer - never backpressures</li>
   *    <li>Fail - fails the stream if buffer gets full</li>
   *  </ul>
   *
   * '''Completes when''' upstream completes and buffered elements have been drained
   *
   * '''Cancels when''' downstream cancels
   *
   * @param size The size of the buffer in element count
   * @param overflowStrategy Strategy that is used when incoming elements cannot fit inside the buffer
   */
  def buffer(size: Int, overflowStrategy: OverflowStrategy): Repr[Out] = via(fusing.Buffer(size, overflowStrategy))

  /**
   * Takes up to `n` elements from the stream (less than `n` only if the upstream completes before emitting `n` elements)
   * and returns a pair containing a strict sequence of the taken element
   * and a stream representing the remaining elements. If ''n'' is zero or negative, then this will return a pair
   * of an empty collection and a stream containing the whole upstream unchanged.
   *
   * In case of an upstream error, depending on the current state
   *  - the master stream signals the error if less than `n` elements has been seen, and therefore the substream
   *    has not yet been emitted
   *  - the tail substream signals the error after the prefix and tail has been emitted by the main stream
   *    (at that point the main stream has already completed)
   *
   * '''Emits when''' the configured number of prefix elements are available. Emits this prefix, and the rest
   * as a substream
   *
   * '''Backpressures when''' downstream backpressures or substream backpressures
   *
   * '''Completes when''' prefix elements have been consumed and substream has been consumed
   *
   * '''Cancels when''' downstream cancels or substream cancels
   */
  def prefixAndTail[U >: Out](n: Int): Repr[(immutable.Seq[Out], Source[U, NotUsed])] =
    via(new PrefixAndTail[Out](n))

  /**
   * Takes up to `n` elements from the stream (less than `n` only if the upstream completes before emitting `n` elements),
   * then apply `f` on these elements in order to obtain a flow, this flow is then materialized and the rest of the input is processed by this flow (similar to via).
   * This method returns a flow consuming the rest of the stream producing the materialized flow's output.
   *
   * '''Emits when''' the materialized flow emits.
   *  Notice the first `n` elements are buffered internally before materializing the flow and connecting it to the rest of the upstream - producing elements at its own discretion (might 'swallow' or multiply elements).
   *
   * '''Backpressures when''' the materialized flow backpressures
   *
   * '''Completes when''' the materialized flow completes.
   *  If upstream completes before producing `n` elements, `f` will be applied with the provided elements,
   *  the resulting flow will be materialized and signalled for upstream completion, it can then complete or continue to emit elements at its own discretion.
   *
   * '''Cancels when''' the materialized flow cancels.
   *  When downstream cancels before materialization of the nested flow, the operator's default behaviour is to cancel immediately,
   *  this behaviour can be controlled by setting the [[akka.stream.Attributes.NestedMaterializationCancellationPolicy]] attribute on the flow.
   *  When this attribute is configured to true, downstream cancellation is delayed until the nested flow's materialization which is then immediately cancelled (with the original cancellation cause).
   *
   *  @param n the number of elements to accumulate before materializing the downstream flow.
   *  @param f a function that produces the downstream flow based on the upstream's prefix.
   **/
  def flatMapPrefix[Out2, Mat2](n: Int)(f: immutable.Seq[Out] => Flow[Out, Out2, Mat2]): Repr[Out2] = {
    via(new FlatMapPrefix(n, f))
  }

  /**
   * This operation demultiplexes the incoming stream into separate output
   * streams, one for each element key. The key is computed for each element
   * using the given function. When a new key is encountered for the first time
   * a new substream is opened and subsequently fed with all elements belonging to
   * that key.
   *
   * WARNING: If `allowClosedSubstreamRecreation` is set to `false` (default behavior) the operator
   * keeps track of all keys of streams that have already been closed. If you expect an infinite
   * number of keys this can cause memory issues. Elements belonging to those keys are drained
   * directly and not send to the substream.
   *
   * Note: If `allowClosedSubstreamRecreation` is set to `true` substream completion and incoming
   * elements are subject to race-conditions. If elements arrive for a stream that is in the process
   * of closing these elements might get lost.
   *
   * The object returned from this method is not a normal [[Source]] or [[Flow]],
   * it is a [[SubFlow]]. This means that after this operator all transformations
   * are applied to all encountered substreams in the same fashion. Substream mode
   * is exited either by closing the substream (i.e. connecting it to a [[Sink]])
   * or by merging the substreams back together; see the `to` and `mergeBack` methods
   * on [[SubFlow]] for more information.
   *
   * It is important to note that the substreams also propagate back-pressure as
   * any other stream, which means that blocking one substream will block the `groupBy`
   * operator itself—and thereby all substreams—once all internal or
   * explicit buffers are filled.
   *
   * If the group by function `f` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Stop]] the stream and substreams will be completed
   * with failure.
   *
   * If the group by function `f` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Resume]] or [[akka.stream.Supervision.Restart]]
   * the element is dropped and the stream and substreams continue.
   *
   * Function `f`  MUST NOT return `null`. This will throw exception and trigger supervision decision mechanism.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' an element for which the grouping function returns a group that has not yet been created.
   * Emits the new group
   *
   * '''Backpressures when''' there is an element pending for a group whose substream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels and all substreams cancel
   *
   * @param maxSubstreams configures the maximum number of substreams (keys)
   *        that are supported; if more distinct keys are encountered then the stream fails
   * @param f computes the key for each element
   * @param allowClosedSubstreamRecreation enables recreation of already closed substreams if elements with their
   *        corresponding keys arrive after completion
   */
  def groupBy[K](
      maxSubstreams: Int,
      f: Out => K,
      allowClosedSubstreamRecreation: Boolean): SubFlow[Out, Mat, Repr, Closed] = {
    val merge = new SubFlowImpl.MergeBack[Out, Repr] {
      override def apply[T](flow: Flow[Out, T, NotUsed], breadth: Int): Repr[T] =
        via(new GroupBy(maxSubstreams, f, allowClosedSubstreamRecreation))
          .map(_.via(flow))
          .via(new FlattenMerge(breadth))
    }
    val finish: (Sink[Out, NotUsed]) => Closed = s =>
      via(new GroupBy(maxSubstreams, f, allowClosedSubstreamRecreation))
        .to(Sink.foreach(_.runWith(s)(GraphInterpreter.currentInterpreter.materializer)))
    new SubFlowImpl(Flow[Out], merge, finish)
  }

  /**
   * This operation demultiplexes the incoming stream into separate output
   * streams, one for each element key. The key is computed for each element
   * using the given function. When a new key is encountered for the first time
   * a new substream is opened and subsequently fed with all elements belonging to
   * that key.
   *
   * WARNING: The operator keeps track of all keys of streams that have already been closed.
   * If you expect an infinite number of keys this can cause memory issues. Elements belonging
   * to those keys are drained directly and not send to the substream.
   *
   * @see [[#groupBy]]
   */
  def groupBy[K](maxSubstreams: Int, f: Out => K): SubFlow[Out, Mat, Repr, Closed] =
    groupBy(maxSubstreams, f, allowClosedSubstreamRecreation = false)

  /**
   * This operation applies the given predicate to all incoming elements and
   * emits them to a stream of output streams, always beginning a new one with
   * the current element if the given predicate returns true for it. This means
   * that for the following series of predicate values, three substreams will
   * be produced with lengths 1, 2, and 3:
   *
   * {{{
   * false,             // element goes into first substream
   * true, false,       // elements go into second substream
   * true, false, false // elements go into third substream
   * }}}
   *
   * In case the *first* element of the stream matches the predicate, the first
   * substream emitted by splitWhen will start from that element. For example:
   *
   * {{{
   * true, false, false // first substream starts from the split-by element
   * true, false        // subsequent substreams operate the same way
   * }}}
   *
   * The object returned from this method is not a normal [[Source]] or [[Flow]],
   * it is a [[SubFlow]]. This means that after this operator all transformations
   * are applied to all encountered substreams in the same fashion. Substream mode
   * is exited either by closing the substream (i.e. connecting it to a [[Sink]])
   * or by merging the substreams back together; see the `to` and `mergeBack` methods
   * on [[SubFlow]] for more information.
   *
   * It is important to note that the substreams also propagate back-pressure as
   * any other stream, which means that blocking one substream will block the `splitWhen`
   * operator itself—and thereby all substreams—once all internal or
   * explicit buffers are filled.
   *
   * If the split predicate `p` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Stop]] the stream and substreams will be completed
   * with failure.
   *
   * If the split predicate `p` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Resume]] or [[akka.stream.Supervision.Restart]]
   * the element is dropped and the stream and substreams continue.
   *
   * '''Emits when''' an element for which the provided predicate is true, opening and emitting
   * a new substream for subsequent element
   *
   * '''Backpressures when''' there is an element pending for the next substream, but the previous
   * is not fully consumed yet, or the substream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels and substreams cancel on `SubstreamCancelStrategy.drain`, downstream
   * cancels or any substream cancels on `SubstreamCancelStrategy.propagate`
   *
   * See also [[FlowOps.splitAfter]].
   */
  def splitWhen(substreamCancelStrategy: SubstreamCancelStrategy)(
      p: Out => Boolean): SubFlow[Out, Mat, Repr, Closed] = {
    val merge = new SubFlowImpl.MergeBack[Out, Repr] {
      override def apply[T](flow: Flow[Out, T, NotUsed], breadth: Int): Repr[T] =
        via(Split.when(p, substreamCancelStrategy)).map(_.via(flow)).via(new FlattenMerge(breadth))
    }

    val finish: (Sink[Out, NotUsed]) => Closed = s =>
      via(Split.when(p, substreamCancelStrategy))
        .to(Sink.foreach(_.runWith(s)(GraphInterpreter.currentInterpreter.materializer)))

    new SubFlowImpl(Flow[Out], merge, finish)
  }

  /**
   * This operation applies the given predicate to all incoming elements and
   * emits them to a stream of output streams, always beginning a new one with
   * the current element if the given predicate returns true for it.
   *
   * @see [[#splitWhen]]
   */
  def splitWhen(p: Out => Boolean): SubFlow[Out, Mat, Repr, Closed] =
    splitWhen(SubstreamCancelStrategy.drain)(p)

  /**
   * This operation applies the given predicate to all incoming elements and
   * emits them to a stream of output streams. It *ends* the current substream when the
   * predicate is true. This means that for the following series of predicate values,
   * three substreams will be produced with lengths 2, 2, and 3:
   *
   * {{{
   * false, true,        // elements go into first substream
   * false, true,        // elements go into second substream
   * false, false, true  // elements go into third substream
   * }}}
   *
   * The object returned from this method is not a normal [[Source]] or [[Flow]],
   * it is a [[SubFlow]]. This means that after this operator all transformations
   * are applied to all encountered substreams in the same fashion. Substream mode
   * is exited either by closing the substream (i.e. connecting it to a [[Sink]])
   * or by merging the substreams back together; see the `to` and `mergeBack` methods
   * on [[SubFlow]] for more information.
   *
   * It is important to note that the substreams also propagate back-pressure as
   * any other stream, which means that blocking one substream will block the `splitAfter`
   * operator itself—and thereby all substreams—once all internal or
   * explicit buffers are filled.
   *
   * If the split predicate `p` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Stop]] the stream and substreams will be completed
   * with failure.
   *
   * If the split predicate `p` throws an exception and the supervision decision
   * is [[akka.stream.Supervision.Resume]] or [[akka.stream.Supervision.Restart]]
   * the element is dropped and the stream and substreams continue.
   *
   * '''Emits when''' an element passes through. When the provided predicate is true it emits the element
   * and opens a new substream for subsequent element
   *
   * '''Backpressures when''' there is an element pending for the next substream, but the previous
   * is not fully consumed yet, or the substream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels and substreams cancel on `SubstreamCancelStrategy.drain`, downstream
   * cancels or any substream cancels on `SubstreamCancelStrategy.propagate`
   *
   * See also [[FlowOps.splitWhen]].
   */
  def splitAfter(substreamCancelStrategy: SubstreamCancelStrategy)(
      p: Out => Boolean): SubFlow[Out, Mat, Repr, Closed] = {
    val merge = new SubFlowImpl.MergeBack[Out, Repr] {
      override def apply[T](flow: Flow[Out, T, NotUsed], breadth: Int): Repr[T] =
        via(Split.after(p, substreamCancelStrategy)).map(_.via(flow)).via(new FlattenMerge(breadth))
    }
    val finish: (Sink[Out, NotUsed]) => Closed = s =>
      via(Split.after(p, substreamCancelStrategy))
        .to(Sink.foreach(_.runWith(s)(GraphInterpreter.currentInterpreter.materializer)))
    new SubFlowImpl(Flow[Out], merge, finish)
  }

  /**
   * This operation applies the given predicate to all incoming elements and
   * emits them to a stream of output streams. It *ends* the current substream when the
   * predicate is true.
   *
   * @see [[#splitAfter]]
   */
  def splitAfter(p: Out => Boolean): SubFlow[Out, Mat, Repr, Closed] =
    splitAfter(SubstreamCancelStrategy.drain)(p)

  /**
   * Transform each input element into a `Source` of output elements that is
   * then flattened into the output stream by concatenation,
   * fully consuming one Source after the other.
   *
   * '''Emits when''' a currently consumed substream has an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes and all consumed substreams complete
   *
   * '''Cancels when''' downstream cancels
   */
  def flatMapConcat[T, M](f: Out => Graph[SourceShape[T], M]): Repr[T] = map(f).via(new FlattenMerge[T, M](1))

  /**
   * Transform each input element into a `Source` of output elements that is
   * then flattened into the output stream by merging, where at most `breadth`
   * substreams are being consumed at any given time.
   *
   * '''Emits when''' a currently consumed substream has an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes and all consumed substreams complete
   *
   * '''Cancels when''' downstream cancels
   */
  def flatMapMerge[T, M](breadth: Int, f: Out => Graph[SourceShape[T], M]): Repr[T] =
    map(f).via(new FlattenMerge[T, M](breadth))

  /**
   * If the first element has not passed through this operator before the provided timeout, the stream is failed
   * with a [[akka.stream.InitialTimeoutException]].
   *
   * '''Emits when''' upstream emits an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or fails if timeout elapses before first element arrives
   *
   * '''Cancels when''' downstream cancels
   */
  def initialTimeout(timeout: FiniteDuration): Repr[Out] = via(new Timers.Initial[Out](timeout))

  /**
   * If the completion of the stream does not happen until the provided timeout, the stream is failed
   * with a [[akka.stream.CompletionTimeoutException]].
   *
   * '''Emits when''' upstream emits an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or fails if timeout elapses before upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def completionTimeout(timeout: FiniteDuration): Repr[Out] = via(new Timers.Completion[Out](timeout))

  /**
   * If the time between two processed elements exceeds the provided timeout, the stream is failed
   * with a [[akka.stream.StreamIdleTimeoutException]]. The timeout is checked periodically,
   * so the resolution of the check is one period (equals to timeout value).
   *
   * '''Emits when''' upstream emits an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or fails if timeout elapses between two emitted elements
   *
   * '''Cancels when''' downstream cancels
   */
  def idleTimeout(timeout: FiniteDuration): Repr[Out] = via(new Timers.Idle[Out](timeout))

  /**
   * If the time between the emission of an element and the following downstream demand exceeds the provided timeout,
   * the stream is failed with a [[akka.stream.BackpressureTimeoutException]]. The timeout is checked periodically,
   * so the resolution of the check is one period (equals to timeout value).
   *
   * '''Emits when''' upstream emits an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes or fails if timeout elapses between element emission and downstream demand.
   *
   * '''Cancels when''' downstream cancels
   */
  def backpressureTimeout(timeout: FiniteDuration): Repr[Out] = via(new Timers.BackpressureTimeout[Out](timeout))

  /**
   * Injects additional elements if upstream does not emit for a configured amount of time. In other words, this
   * operator attempts to maintains a base rate of emitted elements towards the downstream.
   *
   * If the downstream backpressures then no element is injected until downstream demand arrives. Injected elements
   * do not accumulate during this period.
   *
   * Upstream elements are always preferred over injected elements.
   *
   * '''Emits when''' upstream emits an element or if the upstream was idle for the configured period
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def keepAlive[U >: Out](maxIdle: FiniteDuration, injectedElem: () => U): Repr[U] =
    via(new Timers.IdleInject[Out, U](maxIdle, injectedElem))

  /**
   * Sends elements downstream with speed limited to `elements/per`. In other words, this operator sets the maximum rate
   * for emitting messages. This operator works for streams where all elements have the same cost or length.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and
   * started.
   *
   * The burst size is calculated based on the given rate (`cost/per`) as 0.1 * rate, for example:
   * - rate < 20/second => burst size 1
   * - rate 20/second => burst size 2
   * - rate 100/second => burst size 10
   * - rate 200/second => burst size 20
   *
   * The throttle `mode` is [[akka.stream.ThrottleMode.Shaping]], which pauses before emitting messages to
   * meet the throttle rate.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def throttle(elements: Int, per: FiniteDuration): Repr[Out] =
    throttle(elements, per, maximumBurst = Throttle.AutomaticMaximumBurst, ConstantFun.oneInt, ThrottleMode.Shaping)

  /**
   * Sends elements downstream with speed limited to `elements/per`. In other words, this operator sets the maximum rate
   * for emitting messages. This operator works for streams where all elements have the same cost or length.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size or maximumBurst).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and started.
   *
   * Parameter `mode` manages behavior when upstream is faster than throttle rate:
   *  - [[akka.stream.ThrottleMode.Shaping]] makes pauses before emitting messages to meet the throttle rate
   *  - [[akka.stream.ThrottleMode.Enforcing]] fails with exception when upstream is faster than throttle rate. Enforcing
   *  cannot emit elements that cost more than the maximumBurst
   *
   * It is recommended to use non-zero burst sizes as they improve both performance and throttling precision by allowing
   * the implementation to avoid using the scheduler when input rates fall below the enforced limit and to reduce
   * most of the inaccuracy caused by the scheduler resolution (which is in the range of milliseconds).
   *
   *  WARNING: Be aware that throttle is using scheduler to slow down the stream. This scheduler has minimal time of triggering
   *  next push. Consequently it will slow down the stream as it has minimal pause for emitting. This can happen in
   *  case burst is 0 and speed is higher than 30 events per second. You need to increase the `maximumBurst`  if
   *  elements arrive with small interval (30 milliseconds or less). Use the overloaded `throttle` method without
   *  `maximumBurst` parameter to automatically calculate the `maximumBurst` based on the given rate (`cost/per`).
   *  In other words the throttler always enforces the rate limit when `maximumBurst` parameter is given, but in
   *  certain cases (mostly due to limited scheduler resolution) it enforces a tighter bound than what was prescribed.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def throttle(elements: Int, per: FiniteDuration, maximumBurst: Int, mode: ThrottleMode): Repr[Out] =
    throttle(elements, per, maximumBurst, ConstantFun.oneInt, mode)

  /**
   * Sends elements downstream with speed limited to `cost/per`. Cost is
   * calculating for each element individually by calling `calculateCost` function.
   * This operator works for streams when elements have different cost(length).
   * Streams of `ByteString` for example.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and
   * started.
   *
   * The burst size is calculated based on the given rate (`cost/per`) as 0.1 * rate, for example:
   * - rate < 20/second => burst size 1
   * - rate 20/second => burst size 2
   * - rate 100/second => burst size 10
   * - rate 200/second => burst size 20
   *
   * The throttle `mode` is [[akka.stream.ThrottleMode.Shaping]], which pauses before emitting messages to
   * meet the throttle rate.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def throttle(cost: Int, per: FiniteDuration, costCalculation: (Out) => Int): Repr[Out] =
    throttle(cost, per, Throttle.AutomaticMaximumBurst, costCalculation, ThrottleMode.Shaping)

  /**
   * Sends elements downstream with speed limited to `cost/per`. Cost is
   * calculating for each element individually by calling `calculateCost` function.
   * This operator works for streams when elements have different cost(length).
   * Streams of `ByteString` for example.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size or maximumBurst).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and started.
   *
   * Parameter `mode` manages behavior when upstream is faster than throttle rate:
   *  - [[akka.stream.ThrottleMode.Shaping]] makes pauses before emitting messages to meet the throttle rate
   *  - [[akka.stream.ThrottleMode.Enforcing]] fails with exception when upstream is faster than throttle rate. Enforcing
   *  cannot emit elements that cost more than the maximumBurst
   *
   * It is recommended to use non-zero burst sizes as they improve both performance and throttling precision by allowing
   * the implementation to avoid using the scheduler when input rates fall below the enforced limit and to reduce
   * most of the inaccuracy caused by the scheduler resolution (which is in the range of milliseconds).
   *
   *  WARNING: Be aware that throttle is using scheduler to slow down the stream. This scheduler has minimal time of triggering
   *  next push. Consequently it will slow down the stream as it has minimal pause for emitting. This can happen in
   *  case burst is 0 and speed is higher than 30 events per second. You need to increase the `maximumBurst`  if
   *  elements arrive with small interval (30 milliseconds or less). Use the overloaded `throttle` method without
   *  `maximumBurst` parameter to automatically calculate the `maximumBurst` based on the given rate (`cost/per`).
   *  In other words the throttler always enforces the rate limit when `maximumBurst` parameter is given, but in
   *  certain cases (mostly due to limited scheduler resolution) it enforces a tighter bound than what was prescribed.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def throttle(
      cost: Int,
      per: FiniteDuration,
      maximumBurst: Int,
      costCalculation: (Out) => Int,
      mode: ThrottleMode): Repr[Out] =
    via(new Throttle(() => new ThrottleControl(cost, per, maximumBurst, mode, shared = false), costCalculation, mode))

  /**
   * The `ThrottleControl` can be updated to change the throttle rate from the outside of the stream
   * and share a total rate limit between several streams.
   * Sends elements downstream with speed limited to `elements/per` as defined in the `ThrottleControl`.
   * In other words, this operator sets the maximum rate for emitting messages. This operator works for streams
   * where all elements have the same cost or length.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and
   * started.
   *
   * The burst size is calculated based on the given rate (`cost/per`) as 0.1 * rate, for example:
   * - rate < 20/second => burst size 1
   * - rate 20/second => burst size 2
   * - rate 100/second => burst size 10
   * - rate 200/second => burst size 20
   *
   * The throttle `mode` is [[akka.stream.ThrottleMode.Shaping]], which pauses before emitting messages to
   * meet the throttle rate.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def throttle(control: ThrottleControl): Repr[Out] =
    throttle(control, ConstantFun.oneInt)

  /**
   * The `ThrottleControl` can be updated to change the throttle rate from the outside of the stream
   * and share a total rate limit between several streams.
   * Sends elements downstream with speed limited to `cost/per` as defined in the `ThrottleControl`. Cost is
   * calculating for each element individually by calling `calculateCost` function.
   * This operator works for streams when elements have different cost(length).
   * Streams of `ByteString` for example.
   *
   * Throttle implements the token bucket model. There is a bucket with a given token capacity (burst size or maximumBurst).
   * Tokens drop into the bucket at a given rate and can be kept for later use up to bucket capacity
   * to allow some burstiness. Whenever the stream wants to send an element, it takes as many
   * tokens from the bucket as the element costs. If there isn't any, throttle waits until the
   * bucket accumulates enough tokens. Elements that cost more than the allowed burst will be delayed proportionally
   * to their cost minus available tokens, meeting the target rate. Bucket is full when the stream is first materialized and started.
   *
   * Parameter `mode` manages behavior when upstream is faster than throttle rate:
   *  - [[akka.stream.ThrottleMode.Shaping]] makes pauses before emitting messages to meet the throttle rate
   *  - [[akka.stream.ThrottleMode.Enforcing]] fails with exception when upstream is faster than throttle rate. Enforcing
   *  cannot emit elements that cost more than the maximumBurst
   *
   * It is recommended to use non-zero burst sizes as they improve both performance and throttling precision by allowing
   * the implementation to avoid using the scheduler when input rates fall below the enforced limit and to reduce
   * most of the inaccuracy caused by the scheduler resolution (which is in the range of milliseconds).
   *
   *  WARNING: Be aware that throttle is using scheduler to slow down the stream. This scheduler has minimal time of triggering
   *  next push. Consequently it will slow down the stream as it has minimal pause for emitting. This can happen in
   *  case burst is 0 and speed is higher than 30 events per second. You need to increase the `maximumBurst`  if
   *  elements arrive with small interval (30 milliseconds or less). Use the overloaded `throttle` method without
   *  `maximumBurst` parameter to automatically calculate the `maximumBurst` based on the given rate (`cost/per`).
   *  In other words the throttler always enforces the rate limit when `maximumBurst` parameter is given, but in
   *  certain cases (mostly due to limited scheduler resolution) it enforces a tighter bound than what was prescribed.
   *
   * '''Emits when''' upstream emits an element and configured time per each element elapsed
   *
   * '''Backpressures when''' downstream backpressures or the incoming rate is higher than the speed limit
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   *
   */
  def throttle(control: ThrottleControl, costCalculation: (Out) => Int): Repr[Out] =
    via(new Throttle(() => control, costCalculation, control.mode))

  /**
   * Detaches upstream demand from downstream demand without detaching the
   * stream rates; in other words acts like a buffer of size 1.
   *
   * '''Emits when''' upstream emits an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def detach: Repr[Out] = via(GraphStages.detacher)

  /**
   * Delays the initial element by the specified duration.
   *
   * '''Emits when''' upstream emits an element if the initial delay is already elapsed
   *
   * '''Backpressures when''' downstream backpressures or initial delay is not yet elapsed
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def initialDelay(delay: FiniteDuration): Repr[Out] = via(new Timers.DelayInitial[Out](delay))

  /**
   * Logs elements flowing through the stream as well as completion and erroring.
   *
   * By default element and completion signals are logged on debug level, and errors are logged on Error level.
   * This can be adjusted according to your needs by providing a custom [[Attributes.LogLevels]] attribute on the given Flow:
   *
   * Uses implicit [[LoggingAdapter]] if available, otherwise uses an internally created one,
   * which uses `akka.event.Logging(materializer.system, materializer)` as its source (use this class to configure slf4j loggers).
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the mapping function returns an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def log(name: String, extract: Out => Any = ConstantFun.scalaIdentityFunction)(
      implicit log: LoggingAdapter = null): Repr[Out] =
    via(Log(name, extract.asInstanceOf[Any => Any], Option(log)))

  /**
   * Logs elements flowing through the stream as well as completion and erroring.
   *
   * By default element and completion signals are logged on debug level, and errors are logged on Error level.
   * This can be adjusted according to your needs by providing a custom [[Attributes.LogLevels]] attribute on the given Flow:
   *
   * Uses implicit [[MarkerLoggingAdapter]] if available, otherwise uses an internally created one,
   * which uses `akka.event.Logging.withMarker(materializer.system, materializer)` as its source (use this class to configure slf4j loggers).
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the mapping function returns an element
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def logWithMarker(name: String, marker: Out => LogMarker, extract: Out => Any = ConstantFun.scalaIdentityFunction)(
      implicit log: MarkerLoggingAdapter = null): Repr[Out] =
    via(LogWithMarker(name, marker, extract.asInstanceOf[Any => Any], Option(log)))

  /**
   * Combine the elements of current flow and the given [[Source]] into a stream of tuples.
   *
   * '''Emits when''' all of the inputs have an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' any upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zip[U](that: Graph[SourceShape[U], _]): Repr[(Out, U)] = via(zipGraph(that))

  /**
   * Combine the elements of current flow and the given [[Source]] into a stream of tuples.
   *
   * '''Emits when''' at first emits when both inputs emit, and then as long as any input emits (coupled to the default value of the completed input).
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' all upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zipAll[U, A >: Out](that: Graph[SourceShape[U], _], thisElem: A, thatElem: U): Repr[(A, U)] = {
    via(zipAllFlow(that, thisElem, thatElem))
  }

  protected def zipAllFlow[U, A >: Out, Mat2](
      that: Graph[SourceShape[U], Mat2],
      thisElem: A,
      thatElem: U): Flow[Out @uncheckedVariance, (A, U), Mat2] = {
    case object passedEnd
    val passedEndSrc = Source.repeat(passedEnd)
    val left: Flow[Out, Any, NotUsed] = Flow[A].concat(passedEndSrc)
    val right: Source[Any, Mat2] = Source.fromGraph(that).concat(passedEndSrc)
    val zipFlow: Flow[Out, (A, U), Mat2] = left
      .zipMat(right)(Keep.right)
      .takeWhile {
        case (`passedEnd`, `passedEnd`) => false
        case _                          => true
      }
      .map {
        case (`passedEnd`, r: U @unchecked) => (thisElem, r)
        case (l: A @unchecked, `passedEnd`) => (l, thatElem)
        case t: (A, U) @unchecked           => t
      }
    zipFlow
  }

  protected def zipGraph[U, M](that: Graph[SourceShape[U], M]): Graph[FlowShape[Out @uncheckedVariance, (Out, U)], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val zip = b.add(Zip[Out, U]())
      r ~> zip.in1
      FlowShape(zip.in0, zip.out)
    }

  /**
   * Combine the elements of 2 streams into a stream of tuples, picking always the latest element of each.
   *
   * A `ZipLatest` has a `left` and a `right` input port and one `out` port.
   *
   * No element is emitted until at least one element from each Source becomes available.
   *
   * '''Emits when''' all of the inputs have at least an element available, and then each time an element becomes
   *  available on either of the inputs
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' any upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zipLatest[U](that: Graph[SourceShape[U], _]): Repr[(Out, U)] = via(zipLatestGraph(that))

  protected def zipLatestGraph[U, M](
      that: Graph[SourceShape[U], M]): Graph[FlowShape[Out @uncheckedVariance, (Out, U)], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val zip = b.add(ZipLatest[Out, U]())
      r ~> zip.in1
      FlowShape(zip.in0, zip.out)
    }

  /**
   * Put together the elements of current flow and the given [[Source]]
   * into a stream of combined elements using a combiner function.
   *
   * '''Emits when''' all of the inputs have an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' any upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zipWith[Out2, Out3](that: Graph[SourceShape[Out2], _])(combine: (Out, Out2) => Out3): Repr[Out3] =
    via(zipWithGraph(that)(combine))

  protected def zipWithGraph[Out2, Out3, M](that: Graph[SourceShape[Out2], M])(
      combine: (Out, Out2) => Out3): Graph[FlowShape[Out @uncheckedVariance, Out3], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val zip = b.add(ZipWith[Out, Out2, Out3](combine))
      r ~> zip.in1
      FlowShape(zip.in0, zip.out)
    }

  /**
   * Combine the elements of multiple streams into a stream of combined elements using a combiner function,
   * picking always the latest of the elements of each source.
   *
   * No element is emitted until at least one element from each Source becomes available. Whenever a new
   * element appears, the zipping function is invoked with a tuple containing the new element
   * and the other last seen elements.
   *
   *   '''Emits when''' all of the inputs have at least an element available, and then each time an element becomes
   *   available on either of the inputs
   *
   *   '''Backpressures when''' downstream backpressures
   *
   *   '''Completes when''' any of the upstreams completes
   *
   *   '''Cancels when''' downstream cancels
   */
  def zipLatestWith[Out2, Out3](that: Graph[SourceShape[Out2], _])(combine: (Out, Out2) => Out3): Repr[Out3] =
    zipLatestWith(that, eagerComplete = true)(combine)

  /**
   * Combine the elements of multiple streams into a stream of combined elements using a combiner function,
   * picking always the latest of the elements of each source.
   *
   * No element is emitted until at least one element from each Source becomes available. Whenever a new
   * element appears, the zipping function is invoked with a tuple containing the new element
   * and the other last seen elements.
   *
   *   '''Emits when''' all of the inputs have at least an element available, and then each time an element becomes
   *   available on either of the inputs
   *
   *   '''Backpressures when''' downstream backpressures
   *
   *   '''Completes when''' any upstream completes if `eagerComplete` is enabled or wait for all upstreams to complete
   *
   *   '''Cancels when''' downstream cancels
   */
  def zipLatestWith[Out2, Out3](that: Graph[SourceShape[Out2], _], eagerComplete: Boolean)(
      combine: (Out, Out2) => Out3): Repr[Out3] =
    via(zipLatestWithGraph(that, eagerComplete)(combine))

  protected def zipLatestWithGraph[Out2, Out3, M](that: Graph[SourceShape[Out2], M])(
      combine: (Out, Out2) => Out3): Graph[FlowShape[Out @uncheckedVariance, Out3], M] =
    zipLatestWithGraph(that, eagerComplete = true)(combine)

  protected def zipLatestWithGraph[Out2, Out3, M](that: Graph[SourceShape[Out2], M], eagerComplete: Boolean)(
      combine: (Out, Out2) => Out3): Graph[FlowShape[Out @uncheckedVariance, Out3], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val zip = b.add(ZipLatestWith[Out, Out2, Out3](combine, eagerComplete))
      r ~> zip.in1
      FlowShape(zip.in0, zip.out)
    }

  /**
   * Combine the elements of current flow into a stream of tuples consisting
   * of all elements paired with their index. Indices start at 0.
   *
   * '''Emits when''' upstream emits an element and is paired with their index
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zipWithIndex: Repr[(Out, Long)] =
    via(
      new StatefulMap[Long, Out, (Out, Long)](
        () => 0L,
        (index, out) => (index + 1L, (out, index)),
        ConstantFun.scalaAnyToNone).withAttributes(DefaultAttributes.zipWithIndex))

  /**
   * Interleave is a deterministic merge of the given [[Source]] with elements of this [[Flow]].
   * It first emits `segmentSize` number of elements from this flow to downstream, then - same amount for `that`
   * source, then repeat process.
   *
   * Example:
   * {{{
   * Source(List(1, 2, 3)).interleave(List(4, 5, 6, 7), 2) // 1, 2, 4, 5, 3, 6, 7
   * }}}
   *
   * After one of upstreams is complete then all the rest elements will be emitted from the second one
   *
   * If it gets error from one of upstreams - stream completes with failure.
   *
   * '''Emits when''' element is available from the currently consumed upstream
   *
   * '''Backpressures when''' downstream backpressures. Signal to current
   * upstream, switch to next upstream when received `segmentSize` elements
   *
   * '''Completes when''' the [[Flow]] and given [[Source]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def interleave[U >: Out](that: Graph[SourceShape[U], _], segmentSize: Int): Repr[U] =
    interleave(that, segmentSize, eagerClose = false)

  /**
   * Interleave is a deterministic merge of the given [[Source]] with elements of this [[Flow]].
   * It first emits `segmentSize` number of elements from this flow to downstream, then - same amount for `that`
   * source, then repeat process.
   *
   * If eagerClose is false and one of the upstreams complete the elements from the other upstream will continue passing
   * through the interleave operator. If eagerClose is true and one of the upstream complete interleave will cancel the
   * other upstream and complete itself.
   *
   * If it gets error from one of upstreams - stream completes with failure.
   *
   * '''Emits when''' element is available from the currently consumed upstream
   *
   * '''Backpressures when''' downstream backpressures. Signal to current
   * upstream, switch to next upstream when received `segmentSize` elements
   *
   * '''Completes when''' the [[Flow]] and given [[Source]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def interleave[U >: Out](that: Graph[SourceShape[U], _], segmentSize: Int, eagerClose: Boolean): Repr[U] =
    via(interleaveGraph(that, segmentSize, eagerClose))

  protected def interleaveGraph[U >: Out, M](
      that: Graph[SourceShape[U], M],
      segmentSize: Int,
      eagerClose: Boolean = false): Graph[FlowShape[Out @uncheckedVariance, U], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val interleave = b.add(Interleave[U](2, segmentSize, eagerClose))
      r ~> interleave.in(1)
      FlowShape(interleave.in(0), interleave.out)
    }

  /**
   * Interleave is a deterministic merge of the given [[Source]]s with elements of this [[Flow]].
   * It first emits `segmentSize` number of elements from this flow to downstream, then - same amount for `that`
   * source, then repeat process.
   *
   * If eagerClose is false and one of the upstreams complete the elements from the other upstream will continue passing
   * through the interleave operator. If eagerClose is true and one of the upstream complete interleave will cancel the
   * other upstream and complete itself.
   *
   * If it gets error from one of upstreams - stream completes with failure.
   *
   * '''Emits when''' element is available from the currently consumed upstream
   *
   * '''Backpressures when''' downstream backpressures. Signal to current
   * upstream, switch to next upstream when received `segmentSize` elements
   *
   * '''Completes when''' the [[Flow]] and given [[Source]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def interleaveAll[U >: Out](
      those: immutable.Seq[Graph[SourceShape[U], _]],
      segmentSize: Int,
      eagerClose: Boolean): Repr[U] = those match {
    case those if those.isEmpty => this.asInstanceOf[Repr[U]]
    case _ =>
      via(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val interleave = b.add(Interleave[U](those.size + 1, segmentSize, eagerClose))
        for ((that, idx) <- those.zipWithIndex)
          that ~> interleave.in(idx + 1)
        FlowShape(interleave.in(0), interleave.out)
      })
  }

  /**
   * Merge the given [[Source]] to this [[Flow]], taking elements as they arrive from input streams,
   * picking randomly when several elements ready.
   *
   * '''Emits when''' one of the inputs has an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' all upstreams complete (eagerComplete=false) or one upstream completes (eagerComplete=true), default value is `false`
   *
   * '''Cancels when''' downstream cancels
   */
  def merge[U >: Out, M](that: Graph[SourceShape[U], M], eagerComplete: Boolean = false): Repr[U] =
    via(mergeGraph(that, eagerComplete))

  protected def mergeGraph[U >: Out, M](
      that: Graph[SourceShape[U], M],
      eagerComplete: Boolean): Graph[FlowShape[Out @uncheckedVariance, U], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(Merge[U](2, eagerComplete))
      r ~> merge.in(1)
      FlowShape(merge.in(0), merge.out)
    }

  /**
   * Merge the given [[Source]]s to this [[Flow]], taking elements as they arrive from input streams,
   * picking randomly when several elements ready.
   *
   * '''Emits when''' one of the inputs has an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' all upstreams complete (eagerComplete=false) or one upstream completes (eagerComplete=true), default value is `false`
   *
   * '''Cancels when''' downstream cancels
   */
  def mergeAll[U >: Out](those: immutable.Seq[Graph[SourceShape[U], _]], eagerComplete: Boolean): Repr[U] =
    those match {
      case those if those.isEmpty => this.asInstanceOf[Repr[U]]
      case _ =>
        via(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val merge = b.add(Merge[U](those.size + 1, eagerComplete))
          for ((that, idx) <- those.zipWithIndex)
            that ~> merge.in(idx + 1)
          FlowShape(merge.in(0), merge.out)
        })
    }

  /**
   * MergeLatest joins elements from N input streams into stream of lists of size N.
   * i-th element in list is the latest emitted element from i-th input stream.
   * MergeLatest emits list for each element emitted from some input stream,
   * but only after each input stream emitted at least one element.
   *
   * '''Emits when''' an element is available from some input and each input emits at least one element from stream start
   *
   * '''Completes when''' all upstreams complete (eagerClose=false) or one upstream completes (eagerClose=true)
   */
  def mergeLatest[U >: Out, M](that: Graph[SourceShape[U], M], eagerComplete: Boolean = false): Repr[immutable.Seq[U]] =
    via(mergeLatestGraph(that, eagerComplete))

  protected def mergeLatestGraph[U >: Out, M](
      that: Graph[SourceShape[U], M],
      eagerComplete: Boolean): Graph[FlowShape[Out @uncheckedVariance, immutable.Seq[U]], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(MergeLatest[U](2, eagerComplete))
      r ~> merge.in(1)
      FlowShape(merge.in(0), merge.out)
    }

  /**
   * Merge two sources. Prefer one source if both sources have elements ready.
   *
   * '''emits''' when one of the inputs has an element available. If multiple have elements available, prefer the 'right' one when 'preferred' is 'true', or the 'left' one when 'preferred' is 'false'.
   *
   * '''backpressures''' when downstream backpressures
   *
   * '''completes''' when all upstreams complete (This behavior is changeable to completing when any upstream completes by setting `eagerComplete=true`.)
   */
  @nowarn
  def mergePreferred[U >: Out, M](
      that: Graph[SourceShape[U], M],
      @deprecatedName(Symbol("priority"))
      preferred: Boolean,
      eagerComplete: Boolean = false): Repr[U] =
    via(mergePreferredGraph(that, preferred, eagerComplete))

  @nowarn
  protected def mergePreferredGraph[U >: Out, M](
      that: Graph[SourceShape[U], M],
      @deprecatedName(Symbol("priority"))
      preferred: Boolean,
      eagerComplete: Boolean): Graph[FlowShape[Out @uncheckedVariance, U], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(MergePreferred[U](1, eagerComplete))
      r ~> merge.in(if (preferred) 0 else 1)
      FlowShape(merge.in(if (preferred) 1 else 0), merge.out)
    }

  /**
   * Merge two sources. Prefer the sources depending on the 'priority' parameters.
   *
   * '''emits''' when one of the inputs has an element available, preferring inputs based on the 'priority' parameters if both have elements available
   *
   * '''backpressures''' when downstream backpressures
   *
   * '''completes''' when both upstreams complete (This behavior is changeable to completing when any upstream completes by setting `eagerComplete=true`.)
   */
  def mergePrioritized[U >: Out, M](
      that: Graph[SourceShape[U], M],
      leftPriority: Int,
      rightPriority: Int,
      eagerComplete: Boolean = false): Repr[U] =
    via(mergePrioritizedGraph(that, leftPriority, rightPriority, eagerComplete))

  protected def mergePrioritizedGraph[U >: Out, M](
      that: Graph[SourceShape[U], M],
      leftPriority: Int,
      rightPriority: Int,
      eagerComplete: Boolean): Graph[FlowShape[Out @uncheckedVariance, U], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(MergePrioritized[U](Seq(leftPriority, rightPriority), eagerComplete))
      r ~> merge.in(1)
      FlowShape(merge.in(0), merge.out)
    }

  /**
   * Merge the given [[Source]] to this [[Flow]], taking elements as they arrive from input streams,
   * picking always the smallest of the available elements (waiting for one element from each side
   * to be available). This means that possible contiguity of the input streams is not exploited to avoid
   * waiting for elements, this merge will block when one of the inputs does not have more elements (and
   * does not complete).
   *
   * '''Emits when''' all of the inputs have an element available
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' all upstreams complete
   *
   * '''Cancels when''' downstream cancels
   */
  def mergeSorted[U >: Out, M](that: Graph[SourceShape[U], M])(implicit ord: Ordering[U]): Repr[U] =
    via(mergeSortedGraph(that))

  protected def mergeSortedGraph[U >: Out, M](that: Graph[SourceShape[U], M])(
      implicit ord: Ordering[U]): Graph[FlowShape[Out @uncheckedVariance, U], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(new MergeSorted[U])
      r ~> merge.in1
      FlowShape(merge.in0, merge.out)
    }

  /**
   * Concatenate the given [[Source]] to this [[Flow]], meaning that once this
   * Flow’s input is exhausted and all result elements have been generated,
   * the Source’s elements will be produced.
   *
   * Note that the [[Source]] is materialized together with this Flow and is "detached" meaning it will
   * in effect behave as a one element buffer in front of both the sources, that eagerly demands an element on start
   * (so it can not be combined with `Source.lazy` to defer materialization of `that`).
   *
   * The second source is then kept from producing elements by asserting back-pressure until its time comes.
   *
   * When needing a concat operator that is not detached use [[#concatLazy]]
   *
   * If this [[Flow]] gets upstream error - no elements from the given [[Source]] will be pulled.
   *
   * '''Emits when''' element is available from current stream or from the given [[Source]] when current is completed
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' given [[Source]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def concat[U >: Out, Mat2](that: Graph[SourceShape[U], Mat2]): Repr[U] =
    internalConcat(that, detached = true)

  protected def concatGraph[U >: Out, Mat2](
      that: Graph[SourceShape[U], Mat2],
      detached: Boolean): Graph[FlowShape[Out @uncheckedVariance, U], Mat2] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(akka.stream.scaladsl.Concat[U](2, detached))
      r ~> merge.in(1)
      FlowShape(merge.in(0), merge.out)
    }

  /**
   * Concatenate the given [[Source]] to this [[Flow]], meaning that once this
   * Flow’s input is exhausted and all result elements have been generated,
   * the Source’s elements will be produced.
   *
   * Note that the [[Source]] is materialized together with this Flow. If `lazy` materialization is what is needed
   * the operator can be combined with for example `Source.lazySource` to defer materialization of `that` until the
   * time when this source completes.
   *
   * The second source is then kept from producing elements by asserting back-pressure until its time comes.
   *
   * For a concat operator that is detached, use [[#concat]]
   *
   * If this [[Flow]] gets upstream error - no elements from the given [[Source]] will be pulled.
   *
   * '''Emits when''' element is available from current stream or from the given [[Source]] when current is completed
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' given [[Source]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def concatLazy[U >: Out, Mat2](that: Graph[SourceShape[U], Mat2]): Repr[U] =
    internalConcat(that, detached = false)

  /**
   * Concatenate the given [[Source]]s to this [[Flow]], meaning that once this
   * Flow’s input is exhausted and all result elements have been generated,
   * the [[Source]]s' elements will be produced.
   *
   * Note that the [[Source]]s are materialized together with this Flow. If `lazy` materialization is what is needed
   * the operator can be combined with for example `Source.lazySource` to defer materialization of `that` until the
   * time when this source completes.
   *
   * The second source is then kept from producing elements by asserting back-pressure until its time comes.
   *
   * For a concat operator that is detached, use [[#concat]]
   *
   * If this [[Flow]] gets upstream error - no elements from the given [[Source]]s will be pulled.
   *
   * '''Emits when''' element is available from current stream or from the given [[Source]]s when current is completed
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' given all those [[Source]]s completes
   *
   * '''Cancels when''' downstream cancels
   */
  def concatAllLazy[U >: Out](those: Graph[SourceShape[U], _]*): Repr[U] =
    internalConcatAll(those.toArray, detached = false)

  private def internalConcat[U >: Out, Mat2](that: Graph[SourceShape[U], Mat2], detached: Boolean): Repr[U] =
    that match {
      case source if TraversalBuilder.isEmptySource(source) => this.asInstanceOf[Repr[U]]
      case other =>
        TraversalBuilder.getSingleSource[Any](other) match {
          case OptionVal.Some(singleSource) =>
            via(new SingleConcat(singleSource.elem.asInstanceOf[U]))
          case _ => via(concatGraph(other, detached))
        }
    }

  private def internalConcatAll[U >: Out](those: Array[Graph[SourceShape[U], _]], detached: Boolean): Repr[U] =
    those match {
      case those if those.isEmpty     => this.asInstanceOf[Repr[U]]
      case those if those.length == 1 => internalConcat(those.head, detached)
      case _ =>
        via(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val concat = b.add(Concat[U](those.length + 1, detached))
          for ((that, idx) <- those.zipWithIndex)
            that ~> concat.in(idx + 1)
          FlowShape(concat.in(0), concat.out)
        })
    }

  /**
   * Prepend the given [[Source]] to this [[Flow]], meaning that before elements
   * are generated from this Flow, the Source's elements will be produced until it
   * is exhausted, at which point Flow elements will start being produced.
   *
   * Note that the [[Source]] is materialized together with this Flow and is "detached" meaning
   * in effect behave as a one element buffer in front of both the sources, that eagerly demands an element on start
   * (so it can not be combined with `Source.lazy` to defer materialization of `that`).
   *
   * This flow will then be kept from producing elements by asserting back-pressure until its time comes.
   *
   * When needing a prepend operator that is not detached use [[#prependLazy]]
   *
   *
   * '''Emits when''' element is available from the given [[Source]] or from current stream when the [[Source]] is completed
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' this [[Flow]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def prepend[U >: Out, Mat2](that: Graph[SourceShape[U], Mat2]): Repr[U] =
    via(prependGraph(that, detached = true))

  protected def prependGraph[U >: Out, Mat2](
      that: Graph[SourceShape[U], Mat2],
      detached: Boolean): Graph[FlowShape[Out @uncheckedVariance, U], Mat2] =
    GraphDSL.createGraph(that) { implicit b => r =>
      val merge = b.add(Concat[U](2, detached))
      r ~> merge.in(0)
      FlowShape(merge.in(1), merge.out)
    }

  /**
   * Prepend the given [[Source]] to this [[Flow]], meaning that before elements
   * are generated from this Flow, the Source's elements will be produced until it
   * is exhausted, at which point Flow elements will start being produced.
   *
   * Note that the [[Source]] is materialized together with this Flow and will then be kept from producing elements
   * by asserting back-pressure until its time comes.
   *
   * When needing a prepend operator that is also detached use [[#prepend]]
   *
   * If the given [[Source]] gets upstream error - no elements from this [[Flow]] will be pulled.
   *
   * '''Emits when''' element is available from the given [[Source]] or from current stream when the [[Source]] is completed
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' this [[Flow]] completes
   *
   * '''Cancels when''' downstream cancels
   */
  def prependLazy[U >: Out, Mat2](that: Graph[SourceShape[U], Mat2]): Repr[U] =
    via(prependGraph(that, detached = false))

  /**
   * Provides a secondary source that will be consumed if this stream completes without any
   * elements passing by. As soon as the first element comes through this stream, the alternative
   * will be cancelled.
   *
   * Note that this Flow will be materialized together with the [[Source]] and just kept
   * from producing elements by asserting back-pressure until its time comes or it gets
   * cancelled.
   *
   * On errors the operator is failed regardless of source of the error.
   *
   * '''Emits when''' element is available from first stream or first stream closed without emitting any elements and an element
   *                  is available from the second stream
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' the primary stream completes after emitting at least one element, when the primary stream completes
   *                      without emitting and the secondary stream already has completed or when the secondary stream completes
   *
   * '''Cancels when''' downstream cancels and additionally the alternative is cancelled as soon as an element passes
   *                    by from this stream.
   */
  def orElse[U >: Out, Mat2](secondary: Graph[SourceShape[U], Mat2]): Repr[U] =
    via(orElseGraph(secondary))

  protected def orElseGraph[U >: Out, Mat2](
      secondary: Graph[SourceShape[U], Mat2]): Graph[FlowShape[Out @uncheckedVariance, U], Mat2] =
    GraphDSL.createGraph(secondary) { implicit b => secondary =>
      val orElse = b.add(OrElse[U]())

      secondary ~> orElse.in(1)

      FlowShape(orElse.in(0), orElse.out)
    }

  /**
   * Concatenates this [[Flow]] with the given [[Source]] so the first element
   * emitted by that source is emitted after the last element of this
   * flow.
   *
   * This is a shorthand for [[concat]]
   */
  def ++[U >: Out, M](that: Graph[SourceShape[U], M]): Repr[U] = concat(that)

  /**
   * Connect this [[Flow]] to a [[Sink]], concatenating the processing steps of both.
   * {{{
   *     +------------------------------+
   *     | Resulting Sink[In, Mat]      |
   *     |                              |
   *     |  +------+          +------+  |
   *     |  |      |          |      |  |
   * In ~~> | flow | ~~Out~~> | sink |  |
   *     |  |   Mat|          |     M|  |
   *     |  +------+          +------+  |
   *     +------------------------------+
   * }}}
   *
   * The materialized value of the combined [[Sink]] will be the materialized
   * value of the current flow (ignoring the given Sink’s value), use
   * [[Flow#toMat[Mat2* toMat]] if a different strategy is needed.
   *
   * See also [[FlowOpsMat.toMat]] when access to materialized values of the parameter is needed.
   */
  def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Closed

  /**
   * Attaches the given [[Sink]] to this [[Source]], meaning that elements that pass
   * through will also be sent to the [[Sink]].
   *
   * It is similar to [[#wireTap]] but will backpressure instead of dropping elements when the given [[Sink]] is not ready.
   *
   * '''Emits when''' element is available and demand exists both from the Sink and the downstream.
   *
   * '''Backpressures when''' downstream or Sink backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream or Sink cancels
   */
  def alsoTo(that: Graph[SinkShape[Out], _]): Repr[Out] = via(alsoToGraph(that))

  protected def alsoToGraph[M](that: Graph[SinkShape[Out], M]): Graph[FlowShape[Out @uncheckedVariance, Out], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      import GraphDSL.Implicits._
      val bcast = b.add(Broadcast[Out](2, eagerCancel = true))
      bcast.out(1) ~> r
      FlowShape(bcast.in, bcast.out(0))
    }

  /**
   * Attaches the given [[Sink]]s to this [[Source]], meaning that elements that pass
   * through will also be sent to the [[Sink]].
   *
   * It is similar to [[#wireTap]] but will backpressure instead of dropping elements when the given [[Sink]]s is not ready.
   *
   * '''Emits when''' element is available and demand exists both from the Sinks and the downstream.
   *
   * '''Backpressures when''' downstream or any of the [[Sink]]s backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream or any of the [[Sink]]s cancels
   */
  def alsoToAll(those: Graph[SinkShape[Out], _]*): Repr[Out] = those match {
    case those if those.isEmpty => this.asInstanceOf[Repr[Out]]
    case _ =>
      via(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val bcast = b.add(Broadcast[Out](those.size + 1, eagerCancel = true))
        for ((that, idx) <- those.zipWithIndex)
          bcast.out(idx + 1) ~> that
        FlowShape(bcast.in, bcast.out(0))
      })
  }

  /**
   * Attaches the given [[Sink]] to this [[Flow]], meaning that elements will be sent to the [[Sink]]
   * instead of being passed through if the predicate `when` returns `true`.
   *
   * '''Emits when''' emits when an element is available from the input and the chosen output has demand
   *
   * '''Backpressures when''' the currently chosen output back-pressures
   *
   * '''Completes when''' upstream completes and no output is pending
   *
   * '''Cancels when''' any of the downstreams cancel
   */
  def divertTo(that: Graph[SinkShape[Out], _], when: Out => Boolean): Repr[Out] = via(divertToGraph(that, when))

  protected def divertToGraph[M](
      that: Graph[SinkShape[Out], M],
      when: Out => Boolean): Graph[FlowShape[Out @uncheckedVariance, Out], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      import GraphDSL.Implicits._
      val partition = b.add(new Partition[Out](2, out => if (when(out)) 1 else 0, eagerCancel = true))
      partition.out(1) ~> r
      FlowShape(partition.in, partition.out(0))
    }

  /**
   * Attaches the given [[Sink]] to this [[Flow]] as a wire tap, meaning that elements that pass
   * through will also be sent to the wire-tap Sink, without the latter affecting the mainline flow.
   * If the wire-tap Sink backpressures, elements that would've been sent to it will be dropped instead.
   *
   * It is similar to [[#alsoTo]] which does backpressure instead of dropping elements.
   *
   * '''Emits when''' element is available and demand exists from the downstream; the element will
   * also be sent to the wire-tap Sink if there is demand.
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def wireTap(that: Graph[SinkShape[Out], _]): Repr[Out] = via(wireTapGraph(that))

  protected def wireTapGraph[M](that: Graph[SinkShape[Out], M]): Graph[FlowShape[Out @uncheckedVariance, Out], M] =
    GraphDSL.createGraph(that) { implicit b => r =>
      import GraphDSL.Implicits._
      val bcast = b.add(WireTap[Out]())
      bcast.out1 ~> r
      FlowShape(bcast.in, bcast.out0)
    }

  def withAttributes(attr: Attributes): Repr[Out]

  def addAttributes(attr: Attributes): Repr[Out]

  def named(name: String): Repr[Out]

  /**
   * Put an asynchronous boundary around this `Flow`.
   *
   * If this is a `SubFlow` (created e.g. by `groupBy`), this creates an
   * asynchronous boundary around each materialized sub-flow, not the
   * super-flow. That way, the super-flow will communicate with sub-flows
   * asynchronously.
   */
  def async: Repr[Out]

  /**
   * Aggregate input elements into an arbitrary data structure that can be completed and emitted downstream
   * when custom condition is met which can be triggered by aggregate or timer.
   * It can be thought of a more general [[groupedWeightedWithin]].
   *
   * '''Emits when''' the aggregation function decides the aggregate is complete or the timer function returns true
   *
   * '''Backpressures when''' downstream backpressures and the aggregate is complete
   *
   * '''Completes when''' upstream completes and the last aggregate has been emitted downstream
   *
   * '''Cancels when''' downstream cancels
   *
   * @param allocate    allocate the initial data structure for aggregated elements
   * @param aggregate   update the aggregated elements, return true if ready to emit after update.
   * @param harvest     this is invoked before emit within the current stage/operator
   * @param emitOnTimer decide whether the current aggregated elements can be emitted, the custom function is invoked on every interval
   */
  @ApiMayChange
  def aggregateWithBoundary[Agg, Emit](allocate: () => Agg)(
      aggregate: (Agg, Out) => (Agg, Boolean),
      harvest: Agg => Emit,
      emitOnTimer: Option[(Agg => Boolean, FiniteDuration)]): Repr[Emit] =
    via(AggregateWithBoundary(allocate, aggregate, harvest, emitOnTimer))

}

/**
 * INTERNAL API: this trait will be changed in binary-incompatible ways for classes that are derived from it!
 * Do not implement this interface outside the Akka code base!
 *
 * Binary compatibility is only maintained for callers of this trait’s interface.
 */
trait FlowOpsMat[+Out, +Mat] extends FlowOps[Out, Mat] {

  type Repr[+O] <: ReprMat[O, Mat] {
    type Repr[+OO] = FlowOpsMat.this.Repr[OO]
    type ReprMat[+OO, +MM] = FlowOpsMat.this.ReprMat[OO, MM]
    type Closed = FlowOpsMat.this.Closed
    type ClosedMat[+M] = FlowOpsMat.this.ClosedMat[M]
  }
  type ReprMat[+O, +M] <: FlowOpsMat[O, M] {
    type Repr[+OO] = FlowOpsMat.this.ReprMat[OO, M @uncheckedVariance]
    type ReprMat[+OO, +MM] = FlowOpsMat.this.ReprMat[OO, MM]
    type Closed = FlowOpsMat.this.ClosedMat[M @uncheckedVariance]
    type ClosedMat[+MM] = FlowOpsMat.this.ClosedMat[MM]
  }
  type ClosedMat[+M] <: Graph[_, M]

  /**
   * Transform this [[Flow]] by appending the given processing steps.
   * {{{
   *     +---------------------------------+
   *     | Resulting Flow[In, T, M2]       |
   *     |                                 |
   *     |  +------+            +------+   |
   *     |  |      |            |      |   |
   * In ~~> | this |  ~~Out~~>  | flow |  ~~> T
   *     |  |   Mat|            |     M|   |
   *     |  +------+            +------+   |
   *     +---------------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * flow into the materialized value of the resulting Flow.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def viaMat[T, Mat2, Mat3](flow: Graph[FlowShape[Out, T], Mat2])(combine: (Mat, Mat2) => Mat3): ReprMat[T, Mat3]

  /**
   * Connect this [[Flow]] to a [[Sink]], concatenating the processing steps of both.
   * {{{
   *     +----------------------------+
   *     | Resulting Sink             |
   *     |                            |
   *     |  +------+        +------+  |
   *     |  |      |        |      |  |
   * In ~~> | flow | ~Out~> | sink |  |
   *     |  |      |        |      |  |
   *     |  +------+        +------+  |
   *     +----------------------------+
   * }}}
   * The `combine` function is used to compose the materialized values of this flow and that
   * Sink into the materialized value of the resulting Sink.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def toMat[Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => Mat3): ClosedMat[Mat3]

  /**
   * mat version of [[#flatMapPrefix]], this method gives access to a future materialized value of the downstream flow.
   * see [[#flatMapPrefix]] for details.
   */
  def flatMapPrefixMat[Out2, Mat2, Mat3](n: Int)(f: immutable.Seq[Out] => Flow[Out, Out2, Mat2])(
      matF: (Mat, Future[Mat2]) => Mat3): ReprMat[Out2, Mat3] = {
    viaMat(new FlatMapPrefix(n, f))(matF)
  }

  /**
   * Combine the elements of current flow and the given [[Source]] into a stream of tuples.
   *
   * @see [[#zip]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def zipMat[U, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[(Out, U), Mat3] =
    viaMat(zipGraph(that))(matF)

  /**
   * Combine the elements of current flow and the given [[Source]] into a stream of tuples.
   *
   * @see [[#zipAll]]
   *
   * '''Emits when''' at first emits when both inputs emit, and then as long as any input emits (coupled to the default value of the completed input).
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' all upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def zipAllMat[U, Mat2, Mat3, A >: Out](that: Graph[SourceShape[U], Mat2], thisElem: A, thatElem: U)(
      matF: (Mat, Mat2) => Mat3): ReprMat[(A, U), Mat3] = {
    viaMat(zipAllFlow(that, thisElem, thatElem))(matF)
  }

  /**
   * Put together the elements of current flow and the given [[Source]]
   * into a stream of combined elements using a combiner function.
   *
   * @see [[#zipWith]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def zipWithMat[Out2, Out3, Mat2, Mat3](that: Graph[SourceShape[Out2], Mat2])(combine: (Out, Out2) => Out3)(
      matF: (Mat, Mat2) => Mat3): ReprMat[Out3, Mat3] =
    viaMat(zipWithGraph(that)(combine))(matF)

  /**
   * Combine the elements of current flow and the given [[Source]] into a stream of tuples,
   * picking always the latest of the elements of each source.
   *
   * @see [[#zipLatest]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def zipLatestMat[U, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(
      matF: (Mat, Mat2) => Mat3): ReprMat[(Out, U), Mat3] =
    viaMat(zipLatestGraph(that))(matF)

  /**
   * Put together the elements of current flow and the given [[Source]]
   * into a stream of combined elements using a combiner function, picking always the latest of the elements of each source.
   *
   * @see [[#zipLatestWith]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def zipLatestWithMat[Out2, Out3, Mat2, Mat3](that: Graph[SourceShape[Out2], Mat2])(combine: (Out, Out2) => Out3)(
      matF: (Mat, Mat2) => Mat3): ReprMat[Out3, Mat3] =
    zipLatestWithMat(that, eagerComplete = true)(combine)(matF)

  /**
   * Put together the elements of current flow and the given [[Source]]
   * into a stream of combined elements using a combiner function, picking always the latest of the elements of each source.
   *
   * @see [[#zipLatestWith]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def zipLatestWithMat[Out2, Out3, Mat2, Mat3](that: Graph[SourceShape[Out2], Mat2], eagerComplete: Boolean)(
      combine: (Out, Out2) => Out3)(matF: (Mat, Mat2) => Mat3): ReprMat[Out3, Mat3] =
    viaMat(zipLatestWithGraph(that, eagerComplete)(combine))(matF)

  /**
   * Merge the given [[Source]] to this [[Flow]], taking elements as they arrive from input streams,
   * picking randomly when several elements ready.
   *
   * @see [[#merge]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def mergeMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2], eagerComplete: Boolean = false)(
      matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(mergeGraph(that, eagerComplete))(matF)

  /**
   * Interleave is a deterministic merge of the given [[Source]] with elements of this [[Flow]].
   * It first emits `segmentSize` number of elements from this flow to downstream, then - same amount for `that` source,
   * then repeat process.
   *
   * After one of upstreams is complete then all the rest elements will be emitted from the second one
   *
   * If it gets error from one of upstreams - stream completes with failure.
   *
   * @see [[#interleave]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  @nowarn("msg=deprecated")
  def interleaveMat[U >: Out, Mat2, Mat3](
      that: Graph[SourceShape[U], Mat2],
      @deprecatedName(Symbol("request"))
      segmentSize: Int)(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    interleaveMat(that, segmentSize, eagerClose = false)(matF)

  /**
   * Interleave is a deterministic merge of the given [[Source]] with elements of this [[Flow]].
   * It first emits `segmentSize` number of elements from this flow to downstream, then - same amount for `that` source,
   * then repeat process.
   *
   * If eagerClose is false and one of the upstreams complete the elements from the other upstream will continue passing
   * through the interleave operator. If eagerClose is true and one of the upstream complete interleave will cancel the
   * other upstream and complete itself.
   *
   * If it gets error from one of upstreams - stream completes with failure.
   *
   * @see [[#interleave]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  @nowarn("msg=deprecated")
  def interleaveMat[U >: Out, Mat2, Mat3](
      that: Graph[SourceShape[U], Mat2],
      @deprecatedName(Symbol("request"))
      segmentSize: Int,
      eagerClose: Boolean)(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(interleaveGraph(that, segmentSize, eagerClose))(matF)

  /**
   * MergeLatest joins elements from N input streams into stream of lists of size N.
   * i-th element in list is the latest emitted element from i-th input stream.
   * MergeLatest emits list for each element emitted from some input stream,
   * but only after each input stream emitted at least one element.
   *
   * @see [[#mergeLatest]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def mergeLatestMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2], eagerClose: Boolean)(
      matF: (Mat, Mat2) => Mat3): ReprMat[immutable.Seq[U], Mat3] =
    viaMat(mergeLatestGraph(that, eagerClose))(matF)

  /**
   * Merge two sources. Prefer one source if both sources have elements ready.
   *
   * @see [[#mergePreferred]]
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def mergePreferredMat[U >: Out, Mat2, Mat3](
      that: Graph[SourceShape[U], Mat2],
      preferred: Boolean,
      eagerClose: Boolean)(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(mergePreferredGraph(that, preferred, eagerClose))(matF)

  /**
   * Merge two sources. Prefer the sources depending on the 'priority' parameters.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def mergePrioritizedMat[U >: Out, Mat2, Mat3](
      that: Graph[SourceShape[U], Mat2],
      leftPriority: Int,
      rightPriority: Int,
      eagerClose: Boolean)(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(mergePrioritizedGraph(that, leftPriority, rightPriority, eagerClose))(matF)

  /**
   * Merge the given [[Source]] to this [[Flow]], taking elements as they arrive from input streams,
   * picking always the smallest of the available elements (waiting for one element from each side
   * to be available). This means that possible contiguity of the input streams is not exploited to avoid
   * waiting for elements, this merge will block when one of the inputs does not have more elements (and
   * does not complete).
   *
   * @see [[#mergeSorted]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def mergeSortedMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(matF: (Mat, Mat2) => Mat3)(
      implicit ord: Ordering[U]): ReprMat[U, Mat3] =
    viaMat(mergeSortedGraph(that))(matF)

  /**
   * Concatenate the given [[Source]] to this [[Flow]], meaning that once this
   * Flow’s input is exhausted and all result elements have been generated,
   * the Source’s elements will be produced.
   *
   * Note that the [[Source]] is materialized together with this Flow and is "detached" meaning it will
   * in effect behave as a one element buffer in front of both the sources, that eagerly demands an element on start
   * (so it can not be combined with `Source.lazy` to defer materialization of `that`).
   *
   * The second source is then kept from producing elements by asserting back-pressure until its time comes.
   *
   * When needing a concat operator that is not detached use [[#concatLazyMat]]
   *
   * @see [[#concat]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def concatMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(concatGraph(that, detached = true))(matF)

  /**
   * Concatenate the given [[Source]] to this [[Flow]], meaning that once this
   * Flow’s input is exhausted and all result elements have been generated,
   * the Source’s elements will be produced.
   *
   * Note that the [[Source]] is materialized together with this Flow, if `lazy` materialization is what is needed
   * the operator can be combined with `Source.lazy` to defer materialization of `that`.
   *
   * The second source is then kept from producing elements by asserting back-pressure until its time comes.
   *
   * For a concat operator that is detached, use [[#concatMat]]
   *
   * @see [[#concatLazy]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def concatLazyMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(
      matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(concatGraph(that, detached = false))(matF)

  /**
   * Prepend the given [[Source]] to this [[Flow]], meaning that before elements
   * are generated from this Flow, the Source's elements will be produced until it
   * is exhausted, at which point Flow elements will start being produced.
   *
   * Note that this Flow will be materialized together with the [[Source]] and just kept
   * from producing elements by asserting back-pressure until its time comes.
   *
   * If the given [[Source]] gets upstream error - no elements from this [[Flow]] will be pulled.
   *
   * When needing a concat operator that is not detached use [[#prependLazyMat]]
   *
   * @see [[#prepend]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def prependMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(prependGraph(that, detached = true))(matF)

  /**
   * Prepend the given [[Source]] to this [[Flow]], meaning that before elements
   * are generated from this Flow, the Source's elements will be produced until it
   * is exhausted, at which point Flow elements will start being produced.
   *
   * Note that the [[Source]] is materialized together with this Flow and is "detached" meaning
   * in effect behave as a one element buffer in front of both the sources, that eagerly demands an element on start
   * (so it can not be combined with `Source.lazy` to defer materialization of `that`).
   *
   * This flow will then be kept from producing elements by asserting back-pressure until its time comes.
   *
   * When needing a prepend operator that is not detached use [[#prependLazyMat]]
   *
   * @see [[#prependLazy]].
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def prependLazyMat[U >: Out, Mat2, Mat3](that: Graph[SourceShape[U], Mat2])(
      matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(prependGraph(that, detached = true))(matF)

  /**
   * Provides a secondary source that will be consumed if this stream completes without any
   * elements passing by. As soon as the first element comes through this stream, the alternative
   * will be cancelled.
   *
   * Note that this Flow will be materialized together with the [[Source]] and just kept
   * from producing elements by asserting back-pressure until its time comes or it gets
   * cancelled.
   *
   * On errors the operator is failed regardless of source of the error.
   *
   * '''Emits when''' element is available from first stream or first stream closed without emitting any elements and an element
   *                  is available from the second stream
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' the primary stream completes after emitting at least one element, when the primary stream completes
   *                      without emitting and the secondary stream already has completed or when the secondary stream completes
   *
   * '''Cancels when''' downstream cancels and additionally the alternative is cancelled as soon as an element passes
   *                    by from this stream.
   */
  def orElseMat[U >: Out, Mat2, Mat3](secondary: Graph[SourceShape[U], Mat2])(
      matF: (Mat, Mat2) => Mat3): ReprMat[U, Mat3] =
    viaMat(orElseGraph(secondary))(matF)

  /**
   * Attaches the given [[Sink]] to this [[Flow]], meaning that elements that pass
   * through will also be sent to the [[Sink]].
   *
   * @see [[#alsoTo]]
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def alsoToMat[Mat2, Mat3](that: Graph[SinkShape[Out], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[Out, Mat3] =
    viaMat(alsoToGraph(that))(matF)

  /**
   * Attaches the given [[Sink]] to this [[Flow]], meaning that elements will be sent to the [[Sink]]
   * instead of being passed through if the predicate `when` returns `true`.
   *
   * @see [[#divertTo]]
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def divertToMat[Mat2, Mat3](that: Graph[SinkShape[Out], Mat2], when: Out => Boolean)(
      matF: (Mat, Mat2) => Mat3): ReprMat[Out, Mat3] =
    viaMat(divertToGraph(that, when))(matF)

  /**
   * Attaches the given [[Sink]] to this [[Flow]] as a wire tap, meaning that elements that pass
   * through will also be sent to the wire-tap Sink, without the latter affecting the mainline flow.
   * If the wire-tap Sink backpressures, elements that would've been sent to it will be dropped instead.
   *
   * It is similar to [[#alsoToMat]] which does backpressure instead of dropping elements.
   *
   * @see [[#wireTap]]
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def wireTapMat[Mat2, Mat3](that: Graph[SinkShape[Out], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[Out, Mat3] =
    viaMat(wireTapGraph(that))(matF)

  /**
   * Materializes to `Future[Done]` that completes on getting termination message.
   * The Future completes with success when received complete message from upstream or cancel
   * from downstream. It fails with the propagated error when received error message from
   * upstream or downstream.
   *
   * It is recommended to use the internally optimized `Keep.left` and `Keep.right` combiners
   * where appropriate instead of manually writing functions that pass through one of the values.
   */
  def watchTermination[Mat2]()(matF: (Mat, Future[Done]) => Mat2): ReprMat[Out, Mat2] =
    viaMat(GraphStages.terminationWatcher)(matF)

  /**
   * Transform the materialized value of this graph, leaving all other properties as they were.
   */
  def mapMaterializedValue[Mat2](f: Mat => Mat2): ReprMat[Out, Mat2]

  /**
   * Materializes to `FlowMonitor[Out]` that allows monitoring of the current flow. All events are propagated
   * by the monitor unchanged. Note that the monitor inserts a memory barrier every time it processes an
   * event, and may therefor affect performance.
   *
   * The `combine` function is used to combine the `FlowMonitor` with this flow's materialized value.
   */
  def monitorMat[Mat2](combine: (Mat, FlowMonitor[Out]) => Mat2): ReprMat[Out, Mat2] =
    viaMat(GraphStages.monitor)(combine)

  /**
   * Materializes to `(Mat, FlowMonitor[Out])`, which is unlike most other operators (!),
   * in which usually the default materialized value keeping semantics is to keep the left value
   * (by passing `Keep.left()` to a `*Mat` version of a method). This operator is an exception from
   * that rule and keeps both values since dropping its sole purpose is to introduce that materialized value.
   *
   * The `FlowMonitor[Out]` allows monitoring of the current flow. All events are propagated
   * by the monitor unchanged. Note that the monitor inserts a memory barrier every time it processes an
   * event, and may therefor affect performance.
   */
  def monitor: ReprMat[Out, (Mat, FlowMonitor[Out])] =
    monitorMat(Keep.both)
}
