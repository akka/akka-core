/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

import akka.Done
import akka.actor.Cancellable
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.FlowMonitorState._
import akka.stream.Shape
import akka.stream.impl.ContextPropagation
import akka.stream.impl.LinearTraversalBuilder
import akka.stream.impl.ReactiveStreamsCompliance
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl._
import akka.stream.stage._

/**
 * INTERNAL API
 */
// TODO: Fix variance issues
@InternalApi private[akka] final case class GraphStageModule[+S <: Shape @uncheckedVariance, +M](
    shape: S,
    attributes: Attributes,
    stage: GraphStageWithMaterializedValue[S, M])
    extends AtomicModule[S, M] {

  override def withAttributes(attributes: Attributes): AtomicModule[S, M] =
    if (attributes ne this.attributes) new GraphStageModule(shape, attributes, stage)
    else this

  override private[stream] def traversalBuilder = LinearTraversalBuilder.fromModule(this, attributes)

  override def toString: String = f"GraphStage($stage) [${System.identityHashCode(this)}%08x]"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object GraphStages {

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] abstract class SimpleLinearGraphStage[T] extends GraphStage[FlowShape[T, T]] {
    val in = Inlet[T](Logging.simpleName(this) + ".in")
    val out = Outlet[T](Logging.simpleName(this) + ".out")
    override val shape = FlowShape(in, out)
  }

  private object Identity extends SimpleLinearGraphStage[Any] {
    override def initialAttributes = DefaultAttributes.identityOp

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        def onPush(): Unit = push(out, grab(in))
        def onPull(): Unit = pull(in)

        setHandler(in, this)
        setHandler(out, this)
      }

    override def toString = "Identity"
  }

  def identity[T] = Identity.asInstanceOf[SimpleLinearGraphStage[T]]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] object Detacher extends SimpleLinearGraphStage[Any] {
    override def initialAttributes = DefaultAttributes.detacher

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private val contextPropagation = ContextPropagation()

        def onPush(): Unit = {
          if (isAvailable(out)) {
            push(out, grab(in))
            tryPull(in)
          } else {
            contextPropagation.suspendContext()
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (!isAvailable(in)) completeStage()
        }

        def onPull(): Unit = {
          if (isAvailable(in)) {
            contextPropagation.resumeContext()
            push(out, grab(in))
            if (isClosed(in)) completeStage()
            else pull(in)
          }
        }

        setHandlers(in, out, this)

        override def preStart(): Unit = tryPull(in)
      }

    override def toString = "Detacher"
  }

  def detacher[T]: GraphStage[FlowShape[T, T]] = Detacher.asInstanceOf[SimpleLinearGraphStage[T]]

  private object TerminationWatcher extends GraphStageWithMaterializedValue[FlowShape[Any, Any], Future[Done]] {
    val in = Inlet[Any]("terminationWatcher.in")
    val out = Outlet[Any]("terminationWatcher.out")
    override val shape = FlowShape(in, out)
    override def initialAttributes: Attributes = DefaultAttributes.terminationWatcher

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
      val finishPromise = Promise[Done]()

      (new GraphStageLogic(shape) with InHandler with OutHandler {
        def onPush(): Unit = push(out, grab(in))

        override def onUpstreamFinish(): Unit = {
          finishPromise.success(Done)
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          finishPromise.failure(ex)
          failStage(ex)
        }

        def onPull(): Unit = pull(in)

        override def onDownstreamFinish(cause: Throwable): Unit = {
          cause match {
            case _: SubscriptionWithCancelException.NonFailureCancellation =>
              finishPromise.success(Done)
            case ex =>
              finishPromise.failure(ex)
          }
          cancelStage(cause)
        }

        override def postStop(): Unit = {
          if (!finishPromise.isCompleted) finishPromise.failure(new AbruptStageTerminationException(this))
        }

        setHandlers(in, out, this)
      }, finishPromise.future)
    }

    override def toString = "TerminationWatcher"
  }

  def terminationWatcher[T]: GraphStageWithMaterializedValue[FlowShape[T, T], Future[Done]] =
    TerminationWatcher.asInstanceOf[GraphStageWithMaterializedValue[FlowShape[T, T], Future[Done]]]

  private class FlowMonitorImpl[T] extends AtomicReference[Any](Initialized) with FlowMonitor[T] {
    override def state = get match {
      case s: StreamState[_] => s.asInstanceOf[StreamState[T]]
      case msg               => Received(msg.asInstanceOf[T])
    }
  }

  private class MonitorFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], FlowMonitor[T]] {
    val in = Inlet[T]("FlowMonitor.in")
    val out = Outlet[T]("FlowMonitor.out")
    val shape = FlowShape.of(in, out)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, FlowMonitor[T]) = {
      val monitor: FlowMonitorImpl[T] = new FlowMonitorImpl[T]

      val logic: GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

        def onPush(): Unit = {
          val msg = grab(in)
          push(out, msg)
          monitor.set(if (msg.isInstanceOf[StreamState[_]]) Received(msg) else msg)
        }

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          monitor.set(Finished)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          monitor.set(Failed(ex))
        }

        def onPull(): Unit = pull(in)

        override def onDownstreamFinish(cause: Throwable): Unit = {
          super.onDownstreamFinish(cause)
          monitor.set(Finished)
        }

        override def postStop(): Unit = {
          monitor.state match {
            case Finished | _: Failed =>
            case _                    => monitor.set(Failed(new AbruptStageTerminationException(this)))
          }
        }

        setHandler(in, this)
        setHandler(out, this)

        override def toString = "MonitorFlowLogic"
      }

      (logic, monitor)
    }

    override def toString = "MonitorFlow"
  }

  def monitor[T]: GraphStageWithMaterializedValue[FlowShape[T, T], FlowMonitor[T]] =
    new MonitorFlow[T]

  final class TickSource[T](val initialDelay: FiniteDuration, val interval: FiniteDuration, val tick: T)
      extends GraphStageWithMaterializedValue[SourceShape[T], Cancellable] {
    override val shape = SourceShape(Outlet[T]("TickSource.out"))
    val out = shape.out
    override def initialAttributes: Attributes = DefaultAttributes.tickSource
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Cancellable) = {

      val logic = new TimerGraphStageLogic(shape) with Cancellable {
        val cancelled = new AtomicBoolean(false)
        val cancelCallback: AtomicReference[Option[AsyncCallback[Unit]]] = new AtomicReference(None)

        override def preStart() = {
          cancelCallback.set(Some(getAsyncCallback[Unit](_ => completeStage())))
          if (cancelled.get)
            completeStage()
          else
            scheduleWithFixedDelay("TickTimer", initialDelay, interval)
        }

        setHandler(out, eagerTerminateOutput)

        override protected def onTimer(timerKey: Any) =
          if (isAvailable(out) && !isCancelled) push(out, tick)

        override def cancel() = {
          val success = !cancelled.getAndSet(true)
          if (success) cancelCallback.get.foreach(_.invoke(()))
          success
        }

        override def isCancelled = cancelled.get

        override def toString: String = "TickSourceLogic"
      }

      (logic, logic)
    }

    override def toString: String = s"TickSource($initialDelay, $interval, $tick)"
  }

  final class SingleSource[T](val elem: T) extends GraphStage[SourceShape[T]] {
    override def initialAttributes: Attributes = DefaultAttributes.singleSource
    ReactiveStreamsCompliance.requireNonNullElement(elem)
    val out = Outlet[T]("single.out")
    val shape = SourceShape(out)
    def createLogic(attr: Attributes) =
      new GraphStageLogic(shape) with OutHandler {
        def onPull(): Unit = {
          push(out, elem)
          completeStage()
        }
        setHandler(out, this)
      }

    override def toString: String = "SingleSource"
  }

  final class IterableSource[T](val elements: immutable.Iterable[T]) extends GraphStage[SourceShape[T]] {
    ReactiveStreamsCompliance.requireNonNullElement(elements)
    override protected def initialAttributes: Attributes = DefaultAttributes.iterableSource
    private val out = Outlet[T]("IterableSource.out")
    override val shape: SourceShape[T] = SourceShape(out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with OutHandler {
        private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
        private var currentIterator: Iterator[T] = _

        override def onPull(): Unit =
          try {
            if (currentIterator eq null) {
              currentIterator = elements.iterator
            }
            tryPushNextOrComplete()
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop   => failStage(ex)
                case Supervision.Resume => tryPushNextOrComplete()
                case Supervision.Restart =>
                  currentIterator = elements.iterator
                  tryPushNextOrComplete()
              }
          }

        private def tryPushNextOrComplete(): Unit =
          if (currentIterator.hasNext) {
            if (isAvailable(out)) {
              push(out, currentIterator.next())
              if (!currentIterator.hasNext) {
                completeStage()
              }
            }
          } else {
            completeStage()
          }

        setHandler(out, this)
      }

    override def toString: String = "IterableSource"
  }

  final class FutureFlattenSource[T, M](futureSource: Future[Graph[SourceShape[T], M]])
      extends GraphStageWithMaterializedValue[SourceShape[T], Future[M]] {
    ReactiveStreamsCompliance.requireNonNullElement(futureSource)

    val out: Outlet[T] = Outlet("FutureFlattenSource.out")
    override val shape = SourceShape(out)

    override def initialAttributes = DefaultAttributes.futureFlattenSource

    override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[M]) = {
      val materialized = Promise[M]()

      val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
        private val sinkIn = new SubSinkInlet[T]("FutureFlattenSource.in")

        override def preStart(): Unit =
          futureSource.value match {
            case Some(it) =>
              // this optimisation avoids going through any execution context, in similar vein to FastFuture
              onFutureSourceCompleted(it)
            case _ =>
              val cb = getAsyncCallback[Try[Graph[SourceShape[T], M]]](onFutureSourceCompleted).invoke _
              futureSource.onComplete(cb)(ExecutionContext.parasitic) // could be optimised FastFuture-like
          }

        // initial handler (until future completes)
        setHandler(
          out,
          new OutHandler {
            def onPull(): Unit = {}

            override def onDownstreamFinish(cause: Throwable): Unit = {
              if (!materialized.isCompleted) {
                // we used to try to materialize the "inner" source here just to get
                // the materialized value, but that is not safe and may cause the graph shell
                // to leak/stay alive after the stage completes

                materialized.tryFailure(
                  new StreamDetachedException("Stream cancelled before Source Future completed").initCause(cause))
              }

              super.onDownstreamFinish(cause)
            }
          })

        def onPush(): Unit =
          push(out, sinkIn.grab())

        def onPull(): Unit =
          sinkIn.pull()

        override def onUpstreamFinish(): Unit =
          completeStage()

        override def onDownstreamFinish(cause: Throwable): Unit = {
          sinkIn.cancel(cause)
          super.onDownstreamFinish(cause)
        }

        def onFutureSourceCompleted(result: Try[Graph[SourceShape[T], M]]): Unit = {
          result
            .map { graph =>
              val runnable = Source.fromGraph(graph).toMat(sinkIn.sink)(Keep.left)
              val matVal = interpreter.subFusingMaterializer.materialize(runnable, defaultAttributes = attr)
              materialized.success(matVal)

              setHandler(out, this)
              sinkIn.setHandler(this)

              if (isAvailable(out)) {
                sinkIn.pull()
              }

            }
            .recover {
              case t =>
                sinkIn.cancel()
                materialized.failure(t)
                failStage(t)
            }
        }
      }

      (logic, materialized.future)
    }

    override def toString: String = "FutureFlattenSource"
  }

  final class FutureSource[T](val future: Future[T]) extends GraphStage[SourceShape[T]] {
    ReactiveStreamsCompliance.requireNonNullElement(future)
    val shape = SourceShape(Outlet[T]("FutureSource.out"))
    val out = shape.out
    override def initialAttributes: Attributes = DefaultAttributes.futureSource
    override def createLogic(attr: Attributes) =
      new GraphStageLogic(shape) with OutHandler {
        def onPull(): Unit = {
          future.value match {
            case Some(completed) =>
              // optimization if the future is already completed
              onFutureCompleted(completed)
            case None =>
              val cb = getAsyncCallback[Try[T]](onFutureCompleted).invoke _
              future.onComplete(cb)(ExecutionContext.parasitic)
          }

          def onFutureCompleted(result: Try[T]): Unit = {
            result match {
              case scala.util.Success(null) => completeStage()
              case scala.util.Success(v)    => emit(out, v, () => completeStage())
              case scala.util.Failure(t)    => failStage(t)
            }
          }

          setHandler(out, eagerTerminateOutput) // After first pull we won't produce anything more
        }

        setHandler(out, this)
      }

    override def toString: String = "FutureSource"
  }

  /**
   * INTERNAL API
   * Discards all received elements.
   */
  @InternalApi private[akka] object IgnoreSink extends GraphStageWithMaterializedValue[SinkShape[Any], Future[Done]] {

    val in = Inlet[Any]("Ignore.in")
    val shape = SinkShape(in)

    override def initialAttributes = DefaultAttributes.ignoreSink

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
      val promise = Promise[Done]()
      val logic = new GraphStageLogic(shape) with InHandler {

        override def preStart(): Unit = pull(in)
        override def onPush(): Unit = pull(in)

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          promise.trySuccess(Done)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          promise.tryFailure(ex)
        }

        override def postStop(): Unit = {
          if (!promise.isCompleted) promise.tryFailure(new AbruptStageTerminationException(this))
        }

        setHandler(in, this)

      }

      (logic, promise.future)
    }
  }

  @InternalApi
  private[akka] object NeverSource extends GraphStage[SourceShape[Nothing]] {
    private val out = Outlet[Nothing]("NeverSource.out")
    val shape: SourceShape[Nothing] = SourceShape(out)
    override def initialAttributes: Attributes = DefaultAttributes.neverSource
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic with OutHandler =
      new GraphStageLogic(shape) with OutHandler {
        override def onPull(): Unit = ()
        setHandler(out, this)
      }
  }

  @InternalApi
  private[akka] object NeverSink extends GraphStageWithMaterializedValue[SinkShape[Any], Future[Done]] {
    private val in = Inlet[Any]("NeverSink.in")
    val shape: SinkShape[Any] = SinkShape(in)

    override def initialAttributes: Attributes = DefaultAttributes.neverSink

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
      val promise = Promise[Done]()
      val logic = new GraphStageLogic(shape) with InHandler {

        override def onPush(): Unit =
          promise.tryFailure(new IllegalStateException("NeverSink should not receive any push."))

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          promise.trySuccess(Done)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          promise.tryFailure(ex)
        }

        override def postStop(): Unit = {
          if (!promise.isCompleted) promise.tryFailure(new AbruptStageTerminationException(this))
        }

        setHandler(in, this)
      }

      (logic, promise.future)
    }
  }

  /**
   * INTERNAL API.
   *
   * Fusing graphs that have cycles involving FanIn operators might lead to deadlocks if
   * demand is not carefully managed.
   *
   * This means that FanIn operators need to early pull every relevant input on startup.
   * This can either be implemented inside the operator itself, or this method can be used,
   * which adds a detacher operator to every input.
   */
  @InternalApi private[stream] def withDetachedInputs[T](stage: GraphStage[UniformFanInShape[T, T]]) =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val concat = builder.add(stage)
      val ds = concat.inlets.map { inlet =>
        val detacher = builder.add(GraphStages.detacher[T])
        detacher ~> inlet
        detacher.in
      }
      UniformFanInShape(concat.out, ds: _*)
    }

}
