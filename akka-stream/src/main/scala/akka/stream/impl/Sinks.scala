/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.function.BinaryOperator

import scala.collection.Factory
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

import akka.NotUsed
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.ActorAttributes.StreamSubscriptionTimeout
import akka.stream.Attributes.InputBuffer
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.QueueSink.Output
import akka.stream.impl.QueueSink.Pull
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.AtomicModule
import akka.stream.scaladsl.{ Keep, Sink, SinkQueueWithCancel, Source }
import akka.stream.stage._

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] abstract class SinkModule[-In, Mat](val shape: SinkShape[In])
    extends AtomicModule[SinkShape[In], Mat] {

  /**
   * Create the Subscriber or VirtualPublisher that consumes the incoming
   * stream, plus the materialized value. Since Subscriber and VirtualPublisher
   * do not share a common supertype apart from AnyRef this is what the type
   * union devolves into; unfortunately we do not have union types at our
   * disposal at this point.
   */
  def create(context: MaterializationContext): (AnyRef, Mat)

  def attributes: Attributes

  override def traversalBuilder: TraversalBuilder =
    LinearTraversalBuilder.fromModule(this, attributes).makeIsland(SinkModuleIslandTag)

  protected def amendShape(attr: Attributes): SinkShape[In] = {
    val thisN = traversalBuilder.attributes.nameOrDefault(null)
    val thatN = attr.nameOrDefault(null)

    if ((thatN eq null) || thisN == thatN) shape
    else shape.copy(in = Inlet(thatN + ".in"))
  }

  protected def label: String = Logging.simpleName(this)
  final override def toString: String = f"$label [${System.identityHashCode(this)}%08x]"

}

/**
 * INTERNAL API
 * Holds the downstream-most [[org.reactivestreams.Publisher]] interface of the materialized flow.
 * The stream will not have any subscribers attached at this point, which means that after prefetching
 * elements to fill the internal buffers it will assert back-pressure until
 * a subscriber connects and creates demand for elements to be emitted.
 */
@InternalApi private[akka] class PublisherSink[In](val attributes: Attributes, shape: SinkShape[In])
    extends SinkModule[In, Publisher[In]](shape) {

  /*
   * This method is the reason why SinkModule.create may return something that is
   * not a Subscriber: a VirtualPublisher is used in order to avoid the immediate
   * subscription a VirtualProcessor would perform (and it also saves overhead).
   */
  override def create(context: MaterializationContext): (AnyRef, Publisher[In]) = {

    val proc = new VirtualPublisher[In]
    val StreamSubscriptionTimeout(timeout, mode) =
      context.effectiveAttributes.mandatoryAttribute[StreamSubscriptionTimeout]
    if (mode != StreamSubscriptionTimeoutTerminationMode.noop) {
      context.materializer.scheduleOnce(timeout, () => proc.onSubscriptionTimeout(context.materializer, mode))
    }
    (proc, proc)
  }

  override def withAttributes(attr: Attributes): SinkModule[In, Publisher[In]] =
    new PublisherSink[In](attr, amendShape(attr))
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class FanoutPublisherSink[In](val attributes: Attributes, shape: SinkShape[In])
    extends SinkModule[In, Publisher[In]](shape) {

  override def create(context: MaterializationContext): (Subscriber[In], Publisher[In]) = {
    val impl = context.materializer.actorOf(context, FanoutProcessorImpl.props(context.effectiveAttributes))
    val fanoutProcessor = new ActorProcessor[In, In](impl)
    // Resolve cyclic dependency with actor. This MUST be the first message no matter what.
    impl ! ExposedPublisher(fanoutProcessor.asInstanceOf[ActorPublisher[Any]])
    (fanoutProcessor, fanoutProcessor)
  }

  override def withAttributes(attr: Attributes): SinkModule[In, Publisher[In]] =
    new FanoutPublisherSink[In](attr, amendShape(attr))
}

/**
 * INTERNAL API
 * Attaches a subscriber to this stream.
 */
@InternalApi private[akka] final class SubscriberSink[In](
    subscriber: Subscriber[In],
    val attributes: Attributes,
    shape: SinkShape[In])
    extends SinkModule[In, NotUsed](shape) {

  override def create(context: MaterializationContext) = (subscriber, NotUsed)

  override def withAttributes(attr: Attributes): SinkModule[In, NotUsed] =
    new SubscriberSink[In](subscriber, attr, amendShape(attr))
}

/**
 * INTERNAL API
 * A sink that immediately cancels its upstream upon materialization.
 */
@InternalApi private[akka] final class CancelSink(val attributes: Attributes, shape: SinkShape[Any])
    extends SinkModule[Any, NotUsed](shape) {
  override def create(context: MaterializationContext): (Subscriber[Any], NotUsed) =
    (new CancellingSubscriber[Any], NotUsed)
  override def withAttributes(attr: Attributes): SinkModule[Any, NotUsed] = new CancelSink(attr, amendShape(attr))
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class TakeLastStage[T](n: Int)
    extends GraphStageWithMaterializedValue[SinkShape[T], Future[immutable.Seq[T]]] {
  if (n <= 0)
    throw new IllegalArgumentException("requirement failed: n must be greater than 0")

  val in: Inlet[T] = Inlet("takeLast.in")

  override val shape: SinkShape[T] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val p: Promise[immutable.Seq[T]] = Promise()
    (new GraphStageLogic(shape) with InHandler {
      private[this] val buffer = mutable.Queue.empty[T]
      private[this] var count = 0

      override def preStart(): Unit = pull(in)

      override def onPush(): Unit = {
        buffer.enqueue(grab(in))
        if (count < n)
          count += 1
        else
          buffer.dequeue()
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val elements = buffer.toList
        buffer.clear()
        p.trySuccess(elements)
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        p.tryFailure(ex)
        failStage(ex)
      }

      setHandler(in, this)
    }, p.future)
  }

  override def toString: String = "TakeLastStage"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class HeadOptionStage[T]
    extends GraphStageWithMaterializedValue[SinkShape[T], Future[Option[T]]] {

  val in: Inlet[T] = Inlet("headOption.in")

  override val shape: SinkShape[T] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val p: Promise[Option[T]] = Promise()
    (new GraphStageLogic(shape) with InHandler {
      override def preStart(): Unit = pull(in)

      def onPush(): Unit = {
        p.trySuccess(Option(grab(in)))
        completeStage()
      }

      override def onUpstreamFinish(): Unit = {
        p.trySuccess(None)
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        p.tryFailure(ex)
        failStage(ex)
      }

      override def postStop(): Unit = {
        if (!p.isCompleted) p.failure(new AbruptStageTerminationException(this))
      }

      setHandler(in, this)
    }, p.future)
  }

  override def toString: String = "HeadOptionStage"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class SeqStage[T, That](implicit cbf: Factory[T, That with immutable.Iterable[_]])
    extends GraphStageWithMaterializedValue[SinkShape[T], Future[That]] {
  val in = Inlet[T]("seq.in")

  override def toString: String = "SeqStage"

  override val shape: SinkShape[T] = SinkShape.of(in)

  override protected def initialAttributes: Attributes = DefaultAttributes.seqSink

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic with InHandler, Future[That]) = {
    val p: Promise[That] = Promise()
    val logic = new GraphStageLogic(shape) with InHandler {
      val buf = cbf.newBuilder

      override def preStart(): Unit = pull(in)

      def onPush(): Unit = {
        buf += grab(in)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val result = buf.result()
        p.trySuccess(result)
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        p.tryFailure(ex)
        failStage(ex)
      }

      override def postStop(): Unit = {
        if (!p.isCompleted) p.failure(new AbruptStageTerminationException(this))
      }

      setHandler(in, this)
    }

    (logic, p.future)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object QueueSink {
  sealed trait Output[+T]
  final case class Pull[T](promise: Promise[Option[T]]) extends Output[T]
  case object Cancel extends Output[Nothing]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class QueueSink[T](maxConcurrentPulls: Int)
    extends GraphStageWithMaterializedValue[SinkShape[T], SinkQueueWithCancel[T]] {

  require(maxConcurrentPulls > 0, "Max concurrent pulls must be greater than 0")

  val in = Inlet[T]("queueSink.in")
  override def initialAttributes = DefaultAttributes.queueSink
  override val shape: SinkShape[T] = SinkShape.of(in)

  override def toString: String = "QueueSink"

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, SinkQueueWithCancel[T]) = {
    val stageLogic = new GraphStageLogic(shape) with InHandler with SinkQueueWithCancel[T] {

      val maxBuffer = inheritedAttributes.get[InputBuffer](InputBuffer(16, 16)).max
      require(maxBuffer > 0, "Buffer size must be greater than 0")

      // Allocates one additional element to hold stream closed/failure indicators
      val buffer: Buffer[Try[Option[T]]] = Buffer(maxBuffer + 1, inheritedAttributes)
      val currentRequests: Buffer[Promise[Option[T]]] = Buffer(maxConcurrentPulls, inheritedAttributes)

      override def preStart(): Unit = {
        setKeepGoing(true)
        pull(in)
      }

      private val callback = getAsyncCallback[Output[T]] {
        case QueueSink.Pull(pullPromise: Promise[Option[T]] @unchecked) =>
          if (currentRequests.isFull)
            pullPromise.failure(
              new IllegalStateException(s"Too many concurrent pulls. Specified maximum is $maxConcurrentPulls. " +
              "You have to wait for one previous future to be resolved to send another request"))
          else if (buffer.isEmpty) currentRequests.enqueue(pullPromise)
          else {
            if (buffer.used == maxBuffer) tryPull(in)
            sendDownstream(pullPromise)
          }
        case QueueSink.Cancel => completeStage()
      }

      def sendDownstream(promise: Promise[Option[T]]): Unit = {
        val e = buffer.dequeue()
        promise.complete(e)
        e match {
          case Success(_: Some[_]) => //do nothing
          case Success(None)       => completeStage()
          case Failure(t)          => failStage(t)
        }
      }

      def onPush(): Unit = {
        buffer.enqueue(Success(Some(grab(in))))
        if (currentRequests.nonEmpty) currentRequests.dequeue().complete(buffer.dequeue())
        if (buffer.used < maxBuffer) pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        buffer.enqueue(Success(None))
        while (currentRequests.nonEmpty && buffer.nonEmpty) currentRequests.dequeue().complete(buffer.dequeue())
        while (currentRequests.nonEmpty) currentRequests.dequeue().complete(Success(None))
        if (buffer.isEmpty) completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        buffer.enqueue(Failure(ex))
        while (currentRequests.nonEmpty && buffer.nonEmpty) currentRequests.dequeue().complete(buffer.dequeue())
        while (currentRequests.nonEmpty) currentRequests.dequeue().complete(Failure(ex))
        if (buffer.isEmpty) failStage(ex)
      }

      override def postStop(): Unit =
        while (currentRequests.nonEmpty) currentRequests.dequeue().failure(new AbruptStageTerminationException(this))

      setHandler(in, this)

      // SinkQueueWithCancel impl
      override def pull(): Future[Option[T]] = {
        val p = Promise[Option[T]]()
        callback
          .invokeWithFeedback(Pull(p))
          .failed
          .foreach {
            case NonFatal(e) => p.tryFailure(e)
            case _           => ()
          }(ExecutionContext.parasitic)
        p.future
      }
      override def cancel(): Unit = {
        callback.invoke(QueueSink.Cancel)
      }
    }

    (stageLogic, stageLogic)
  }
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express collection as a fold using mutable data without
 * accidentally sharing state between materializations
 */
@InternalApi private[akka] trait CollectorState[T, R] {
  def accumulated(): Any
  def update(elem: T): CollectorState[T, R]
  def finish(): R
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express collection as a fold using mutable data
 */
@InternalApi private[akka] final class FirstCollectorState[T, R](
    collectorFactory: () => java.util.stream.Collector[T, Any, R])
    extends CollectorState[T, R] {

  override def update(elem: T): CollectorState[T, R] = {
    // on first update, return a new mutable collector to ensure not
    // sharing collector between streams
    val collector = collectorFactory()
    val accumulator = collector.accumulator()
    val accumulated = collector.supplier().get()
    accumulator.accept(accumulated, elem)
    new MutableCollectorState(collector, accumulator, accumulated)
  }

  override def accumulated(): Any = {
    // only called if it is asked about accumulated before accepting a first element
    val collector = collectorFactory()
    collector.supplier().get()
  }

  override def finish(): R = {
    // only called if completed without elements
    val collector = collectorFactory()
    collector.finisher().apply(collector.supplier().get())
  }
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express collection as a fold using mutable data
 */
@InternalApi private[akka] final class MutableCollectorState[T, R](
    collector: java.util.stream.Collector[T, Any, R],
    accumulator: java.util.function.BiConsumer[Any, T],
    _accumulated: Any)
    extends CollectorState[T, R] {

  override def accumulated(): Any = _accumulated

  override def update(elem: T): CollectorState[T, R] = {
    accumulator.accept(_accumulated, elem)
    this
  }

  override def finish(): R = {
    // only called if completed without elements
    collector.finisher().apply(_accumulated)
  }
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express reduce as a fold for parallel collector without
 * accidentally sharing state between materializations
 */
@InternalApi private[akka] trait ReducerState[T, R] {
  def update(batch: Any): ReducerState[T, R]
  def finish(): R
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express reduce as a fold for parallel collector
 */
@InternalApi private[akka] final class FirstReducerState[T, R](
    collectorFactory: () => java.util.stream.Collector[T, Any, R])
    extends ReducerState[T, R] {

  def update(batch: Any): ReducerState[T, R] = {
    // on first update, return a new mutable collector to ensure not
    // sharing collector between streams
    val collector = collectorFactory()
    val combiner = collector.combiner()
    new MutableReducerState(collector, combiner, batch)
  }

  def finish(): R = {
    // only called if completed without elements
    val collector = collectorFactory()
    collector.finisher().apply(null)
  }
}

/**
 * INTERNAL API
 *
 * Helper class to be able to express reduce as a fold for parallel collector
 */
@InternalApi private[akka] final class MutableReducerState[T, R](
    val collector: java.util.stream.Collector[T, Any, R],
    val combiner: BinaryOperator[Any],
    var reduced: Any)
    extends ReducerState[T, R] {

  def update(batch: Any): ReducerState[T, R] = {
    reduced = combiner(reduced, batch)
    this
  }

  def finish(): R = collector.finisher().apply(reduced)
}

/**
 * INTERNAL API
 */
@InternalApi final private[stream] class LazySink[T, M](sinkFactory: T => Future[Sink[T, M]])
    extends GraphStageWithMaterializedValue[SinkShape[T], Future[M]] {
  val in = Inlet[T]("lazySink.in")
  override def initialAttributes = DefaultAttributes.lazySink and SourceLocation.forLambda(sinkFactory)
  override val shape: SinkShape[T] = SinkShape.of(in)

  override def toString: String = "LazySink"

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[M]) = {

    val promise = Promise[M]()
    val stageLogic = new GraphStageLogic(shape) with InHandler {
      var switching = false
      override def preStart(): Unit = pull(in)

      override def onPush(): Unit = {
        val element = grab(in)
        switching = true
        val cb: AsyncCallback[Try[Sink[T, M]]] =
          getAsyncCallback {
            case Success(sink) =>
              // check if the stage is still in need for the lazy sink
              // (there could have been an onUpstreamFailure in the meantime that has completed the promise)
              if (!promise.isCompleted) {
                try {
                  val mat = switchTo(sink, element)
                  promise.success(mat)
                  setKeepGoing(true)
                } catch {
                  case NonFatal(e) =>
                    promise.failure(e)
                    failStage(e)
                }
              }
            case Failure(e) =>
              promise.failure(e)
              failStage(e)
          }
        try {
          sinkFactory(element).onComplete(cb.invoke)(ExecutionContext.parasitic)
        } catch {
          case NonFatal(e) =>
            promise.failure(e)
            failStage(e)
        }
      }

      override def onUpstreamFinish(): Unit = {
        // ignore onUpstreamFinish while the stage is switching but setKeepGoing
        //
        if (switching) {
          // there is a cached element -> the stage must not be shut down automatically because isClosed(in) is satisfied
          setKeepGoing(true)
        } else {
          promise.failure(new NeverMaterializedException)
          super.onUpstreamFinish()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        promise.failure(ex)
        super.onUpstreamFailure(ex)
      }

      setHandler(in, this)

      private def switchTo(sink: Sink[T, M], firstElement: T): M = {

        var firstElementPushed = false

        val subOutlet = new SubSourceOutlet[T]("LazySink")

        val matVal = interpreter.subFusingMaterializer
          .materialize(Source.fromGraph(subOutlet.source).toMat(sink)(Keep.right), inheritedAttributes)

        def maybeCompleteStage(): Unit = {
          if (isClosed(in) && subOutlet.isClosed) {
            completeStage()
          }
        }

        // The stage must not be shut down automatically; it is completed when maybeCompleteStage decides
        setKeepGoing(true)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              subOutlet.push(grab(in))
            }
            override def onUpstreamFinish(): Unit = {
              if (firstElementPushed) {
                subOutlet.complete()
                maybeCompleteStage()
              }
            }
            override def onUpstreamFailure(ex: Throwable): Unit = {
              // propagate exception irrespective if the cached element has been pushed or not
              subOutlet.fail(ex)
              // #25410 if we fail the stage here directly, the SubSource may not have been started yet,
              // which can happen if upstream fails immediately after emitting a first value.
              // The SubSource won't be started until the stream shuts down, which means downstream won't see the failure,
              // scheduling it lets the interpreter first start the substream
              getAsyncCallback[Throwable](failStage).invoke(ex)
            }
          })

        subOutlet.setHandler(new OutHandler {
          override def onPull(): Unit = {
            if (firstElementPushed) {
              pull(in)
            } else {
              // the demand can be satisfied right away by the cached element
              firstElementPushed = true
              subOutlet.push(firstElement)
              // in.onUpstreamFinished was not propagated if it arrived before the cached element was pushed
              // -> check if the completion must be propagated now
              if (isClosed(in)) {
                subOutlet.complete()
                maybeCompleteStage()
              }
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            if (!isClosed(in)) cancel(in, cause)
            maybeCompleteStage()
          }
        })

        matVal
      }

    }
    (stageLogic, promise.future)
  }
}
