/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.VectorBuilder
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.util.{ Failure, Success, Try }
import scala.util.control.{ NoStackTrace, NonFatal }
import scala.util.control.Exception.Catcher

import akka.actor.{ ActorRef, Terminated }
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.event._
import akka.event.Logging.LogLevel
import akka.stream.{ Supervision, _ }
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes
import akka.stream.Attributes.{ InputBuffer, LogLevels }
import akka.stream.Attributes.SourceLocation
import akka.stream.OverflowStrategies._
import akka.stream.Supervision.Decider
import akka.stream.impl.{ ContextPropagation, ReactiveStreamsCompliance, Buffer => BufferImpl }
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.TraversalBuilder
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.{ DelayStrategy, Source }
import akka.stream.stage._
import akka.util.OptionVal

// This file is perhaps getting long (Github Issue #31619), please add new operators in other files

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Map[In, Out](f: In => Out) extends GraphStage[FlowShape[In, Out]] {
  val in = Inlet[In]("Map.in")
  val out = Outlet[Out]("Map.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.map and SourceLocation.forLambda(f)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private def decider =
        inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      override def onPush(): Unit = {
        try {
          push(out, f(grab(in)))
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _                => pull(in)
            }
        }
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Filter[T](p: T => Boolean) extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.filter and SourceLocation.forLambda(p)

  override def toString: String = "Filter"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {
      def decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var buffer: OptionVal[T] = OptionVal.none
      private val contextPropagation = ContextPropagation()

      override def preStart(): Unit = pull(in)
      override def onPush(): Unit =
        try {
          val elem = grab(in)
          if (p(elem))
            if (isAvailable(out)) {
              push(out, elem)
              pull(in)
            } else {
              buffer = OptionVal.Some(elem)
              contextPropagation.suspendContext()
            } else pull(in)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _                => pull(in)
            }
        }

      override def onPull(): Unit =
        buffer match {
          case OptionVal.Some(value) =>
            contextPropagation.resumeContext()
            push(out, value)
            buffer = OptionVal.none
            if (!isClosed(in)) pull(in)
            else completeStage()
          case _ => // already pulled
        }

      override def onUpstreamFinish(): Unit =
        if (buffer.isEmpty) super.onUpstreamFinish()
      // else onPull will complete

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class TakeWhile[T](p: T => Boolean, inclusive: Boolean = false)
    extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.takeWhile and SourceLocation.forLambda(p)

  override def toString: String = "TakeWhile"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {
      override def toString = "TakeWhileLogic"

      def decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          if (p(elem)) {
            push(out, elem)
          } else {
            if (inclusive) push(out, elem)
            completeStage()
          }
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _                => pull(in)
            }
        }
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class DropWhile[T](p: T => Boolean) extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.dropWhile and SourceLocation.forLambda(p)

  def createLogic(inheritedAttributes: Attributes) =
    new SupervisedGraphStageLogic(inheritedAttributes, shape) with InHandler with OutHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        withSupervision(() => p(elem)) match {
          case OptionVal.Some(flag) =>
            if (flag) pull(in)
            else {
              push(out, elem)
              setHandler(in, rest)
            }
          case _ => // do nothing
        }
      }

      def rest = new InHandler {
        def onPush() = push(out, grab(in))
      }

      override def onResume(t: Throwable): Unit = if (!hasBeenPulled(in)) pull(in)

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

  override def toString = "DropWhile"
}

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] abstract class SupervisedGraphStageLogic(inheritedAttributes: Attributes, shape: Shape)
    extends GraphStageLogic(shape) {
  private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

  def withSupervision[T](f: () => T): OptionVal[T] =
    try {
      OptionVal.Some(f())
    } catch {
      case NonFatal(ex) =>
        decider(ex) match {
          case Supervision.Stop    => onStop(ex)
          case Supervision.Resume  => onResume(ex)
          case Supervision.Restart => onRestart(ex)
        }
        OptionVal.none[T]
    }

  def onResume(t: Throwable): Unit

  def onStop(t: Throwable): Unit = failStage(t)

  def onRestart(t: Throwable): Unit = onResume(t)
}

private[stream] object Collect {
  // Cached function that can be used with PartialFunction.applyOrElse to ensure that A) the guard is only applied once,
  // and the caller can check the returned value with Collect.notApplied to query whether the PF was applied or not.
  // Prior art: https://github.com/scala/scala/blob/v2.11.4/src/library/scala/collection/immutable/List.scala#L458
  final val NotApplied: Any => Any = _ => Collect.NotApplied
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Collect[In, Out](pf: PartialFunction[In, Out])
    extends GraphStage[FlowShape[In, Out]] {
  val in = Inlet[In]("Collect.in")
  val out = Outlet[Out]("Collect.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.collect and SourceLocation.forLambda(pf)

  def createLogic(inheritedAttributes: Attributes) =
    new SupervisedGraphStageLogic(inheritedAttributes, shape) with InHandler with OutHandler {

      import Collect.NotApplied

      val wrappedPf = () => pf.applyOrElse(grab(in), NotApplied)

      override def onPush(): Unit = withSupervision(wrappedPf) match {
        case OptionVal.Some(result) =>
          result match {
            case NotApplied             => pull(in)
            case result: Out @unchecked => push(out, result)
            case _                      => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
          }
        case _ => //do nothing
      }

      override def onResume(t: Throwable): Unit = if (!hasBeenPulled(in)) pull(in)

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

  override def toString = "Collect"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Recover[T](pf: PartialFunction[Throwable, T])
    extends SimpleLinearGraphStage[T] {
  override protected def initialAttributes: Attributes = DefaultAttributes.recover and SourceLocation.forLambda(pf)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      import Collect.NotApplied

      var recovered: OptionVal[T] = OptionVal.none

      override def onPush(): Unit = push(out, grab(in))

      override def onPull(): Unit = recovered match {
        case OptionVal.Some(elem) =>
          push(out, elem)
          completeStage()
        case _ => pull(in)
      }

      override def onUpstreamFailure(ex: Throwable): Unit =
        try pf.applyOrElse(ex, NotApplied) match {
          case NotApplied => failStage(ex)
          case result: T @unchecked =>
            ReactiveStreamsCompliance.requireNonNullElement(result)
            if (isAvailable(out)) {
              push(out, result)
              completeStage()
            } else {
              recovered = OptionVal.Some(result)
            }
          case _ => throw new IllegalStateException() // won't happen, compiler exhaustiveness check pleaser
        } catch {
          case NonFatal(ex) => failStage(ex)
        }

      setHandlers(in, out, this)
    }
}

/**
 * Maps error with the provided function if it is defined for an error or, otherwise, passes it on unchanged.
 *
 * While similar to [[Recover]] this operator can be used to transform an error signal to a different one *without* logging
 * it as an error in the process. So in that sense it is NOT exactly equivalent to `recover(t => throw t2)` since recover
 * would log the `t2` error.
 */
@InternalApi private[akka] final case class MapError[T](f: PartialFunction[Throwable, Throwable])
    extends SimpleLinearGraphStage[T] {

  override protected def initialAttributes: Attributes = DefaultAttributes.mapError

  override def createLogic(attr: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def onPush(): Unit = push(out, grab(in))
      import Collect.NotApplied

      override def onUpstreamFailure(ex: Throwable): Unit = f.applyOrElse(ex, NotApplied) match {
        case NotApplied   => super.onUpstreamFailure(ex)
        case t: Throwable => super.onUpstreamFailure(t)
        case _            => throw new IllegalStateException() // won't happen, compiler exhaustiveness check pleaser
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Take[T](count: Long) extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.take

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var left: Long = count

      override def onPush(): Unit = {
        if (left > 0) {
          push(out, grab(in))
          left -= 1
        }
        if (left <= 0) completeStage()
      }

      override def onPull(): Unit = {
        if (left > 0) pull(in)
        else completeStage()
      }

      setHandlers(in, out, this)
    }

  override def toString: String = "Take"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Drop[T](count: Long) extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.drop

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var left: Long = count

      override def onPush(): Unit = {
        if (left > 0) {
          left -= 1
          pull(in)
        } else push(out, grab(in))
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

  override def toString: String = "Drop"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Scan[In, Out](zero: Out, f: (Out, In) => Out)
    extends GraphStage[FlowShape[In, Out]] {
  override val shape = FlowShape[In, Out](Inlet("Scan.in"), Outlet("Scan.out"))

  override def initialAttributes: Attributes = DefaultAttributes.scan and SourceLocation.forLambda(f)

  override def toString: String = "Scan"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler { self =>

      private var aggregator = zero
      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      import Supervision.{ Restart, Resume, Stop }
      import shape.{ in, out }

      // Initial behavior makes sure that the zero gets flushed if upstream is empty
      setHandlers(
        in,
        out,
        new InHandler with OutHandler {
          override def onPush(): Unit = ()
          override def onUpstreamFinish(): Unit =
            setHandler(out, new OutHandler {
              override def onPull(): Unit = {
                push(out, aggregator)
                completeStage()
              }
            })
          override def onPull(): Unit = {
            push(out, aggregator)
            setHandlers(in, out, self)
          }
        })

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = {
        try {
          aggregator = f(aggregator, grab(in))
          push(out, aggregator)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Resume => if (!hasBeenPulled(in)) pull(in)
              case Stop   => failStage(ex)
              case Restart =>
                aggregator = zero
                push(out, aggregator)
            }
        }
      }
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ScanAsync[In, Out](zero: Out, f: (Out, In) => Future[Out])
    extends GraphStage[FlowShape[In, Out]] {

  val in = Inlet[In]("ScanAsync.in")
  val out = Outlet[Out]("ScanAsync.out")
  override val shape: FlowShape[In, Out] = FlowShape[In, Out](in, out)

  override val initialAttributes: Attributes = Attributes.name("scanAsync") and SourceLocation.forLambda(f)

  override val toString: String = "ScanAsync"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler { self =>

      private var current: Out = zero
      private var elementHandled: Boolean = false

      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private val ZeroHandler: OutHandler with InHandler = new OutHandler with InHandler {
        override def onPush(): Unit =
          throw new IllegalStateException("No push should happen before zero value has been consumed")

        override def onPull(): Unit = {
          elementHandled = true
          push(out, current)
          setHandlers(in, out, self)
        }

        override def onUpstreamFinish(): Unit =
          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              push(out, current)
              completeStage()
            }
          })
      }

      private def onRestart(): Unit = {
        current = zero
        elementHandled = false
      }

      private def safePull(): Unit = {
        if (isClosed(in)) {
          completeStage()
        } else if (isAvailable(out)) {
          if (!hasBeenPulled(in)) {
            tryPull(in)
          }
        }
      }

      private def pushAndPullOrFinish(update: Out): Unit = {
        push(out, update)
        safePull()
      }

      private def doSupervision(t: Throwable): Unit = {
        decider(t) match {
          case Supervision.Stop   => failStage(t)
          case Supervision.Resume => safePull()
          case Supervision.Restart =>
            onRestart()
            safePull()
        }
        elementHandled = true
      }

      private val futureCB = getAsyncCallback[Try[Out]] {
        case Success(next) =>
          if (next != null) {
            current = next
            pushAndPullOrFinish(next)
            elementHandled = true
          } else {
            doSupervision(ReactiveStreamsCompliance.elementMustNotBeNullException)
          }
        case Failure(t) => doSupervision(t)
      }.invoke _

      setHandlers(in, out, ZeroHandler)

      def onPull(): Unit = safePull()

      def onPush(): Unit = {
        try {
          elementHandled = false

          val eventualCurrent = f(current, grab(in))

          eventualCurrent.value match {
            case Some(result) => futureCB(result)
            case _            => eventualCurrent.onComplete(futureCB)(ExecutionContext.parasitic)
          }
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop    => failStage(ex)
              case Supervision.Restart => onRestart()
              case Supervision.Resume  => ()
            }
            tryPull(in)
            elementHandled = true
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (elementHandled) {
          completeStage()
        }
      }

      override val toString: String = s"ScanAsync.Logic(completed=$elementHandled)"
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Fold[In, Out](zero: Out, f: (Out, In) => Out)
    extends GraphStage[FlowShape[In, Out]] {

  val in = Inlet[In]("Fold.in")
  val out = Outlet[Out]("Fold.out")
  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def toString: String = "Fold"

  override val initialAttributes = DefaultAttributes.fold and SourceLocation.forLambda(f)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var aggregator: Out = zero

      private def decider =
        inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      override def onPush(): Unit = {
        val elem = grab(in)
        try {
          aggregator = f(aggregator, elem)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop    => failStage(ex)
              case Supervision.Restart => aggregator = zero
              case _                   => ()
            }
        } finally {
          if (!isClosed(in)) pull(in)
        }
      }

      override def onPull(): Unit = {
        if (isClosed(in)) {
          push(out, aggregator)
          completeStage()
        } else {
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) {
          push(out, aggregator)
          completeStage()
        }
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class FoldAsync[In, Out](zero: Out, f: (Out, In) => Future[Out])
    extends GraphStage[FlowShape[In, Out]] {

  val in = Inlet[In]("FoldAsync.in")
  val out = Outlet[Out]("FoldAsync.out")
  val shape = FlowShape.of(in, out)

  override def toString: String = "FoldAsync"

  override val initialAttributes = DefaultAttributes.foldAsync and SourceLocation.forLambda(f)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var aggregator: Out = zero
      private var aggregating: Future[Out] = Future.successful(aggregator)

      private def onRestart(@nowarn("msg=never used") t: Throwable): Unit = {
        aggregator = zero
      }

      private val futureCB = getAsyncCallback[Try[Out]] {
        case Success(update) if update != null =>
          aggregator = update

          if (isClosed(in)) {
            push(out, update)
            completeStage()
          } else if (isAvailable(out) && !hasBeenPulled(in)) tryPull(in)

        case other =>
          val ex = other match {
            case Failure(t) => t
            case Success(null) =>
              ReactiveStreamsCompliance.elementMustNotBeNullException
            case Success(_) =>
              throw new IllegalStateException() // won't happen, compiler exhaustiveness check pleaser
          }
          val supervision = decider(ex)

          if (supervision == Supervision.Stop) failStage(ex)
          else {
            if (supervision == Supervision.Restart) onRestart(ex)

            if (isClosed(in)) {
              push(out, aggregator)
              completeStage()
            } else if (isAvailable(out) && !hasBeenPulled(in)) tryPull(in)
          }
      }.invoke _

      def onPush(): Unit = {
        try {
          aggregating = f(aggregator, grab(in))
          handleAggregatingValue()
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case supervision => {
                supervision match {
                  case Supervision.Restart => onRestart(ex)
                  case _                   => () // just ignore on Resume
                }

                tryPull(in)
              }
            }
        }
      }

      override def onUpstreamFinish(): Unit = {
        handleAggregatingValue()
      }

      def onPull(): Unit = if (!hasBeenPulled(in)) tryPull(in)

      private def handleAggregatingValue(): Unit = {
        aggregating.value match {
          case Some(result) => futureCB(result) // already completed
          case _            => aggregating.onComplete(futureCB)(ExecutionContext.parasitic)
        }
      }

      setHandlers(in, out, this)

      override def toString =
        s"FoldAsync.Logic(completed=${aggregating.isCompleted})"
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Intersperse[T](start: Option[T], inject: T, end: Option[T])
    extends SimpleLinearGraphStage[T] {
  ReactiveStreamsCompliance.requireNonNullElement(inject)
  if (start.isDefined) ReactiveStreamsCompliance.requireNonNullElement(start.get)
  if (end.isDefined) ReactiveStreamsCompliance.requireNonNullElement(end.get)

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
    val startInHandler = new InHandler {
      override def onPush(): Unit = {
        // if else (to avoid using Iterator[T].flatten in hot code)
        if (start.isDefined) emitMultiple(out, Iterator(start.get, grab(in)))
        else emit(out, grab(in))
        setHandler(in, restInHandler) // switch handler
      }

      override def onUpstreamFinish(): Unit = {
        emitMultiple(out, Iterator(start, end).flatten)
        completeStage()
      }
    }

    val restInHandler = new InHandler {
      override def onPush(): Unit = emitMultiple(out, Iterator(inject, grab(in)))

      override def onUpstreamFinish(): Unit = {
        if (end.isDefined) emit(out, end.get)
        completeStage()
      }
    }

    def onPull(): Unit = pull(in)

    setHandler(in, startInHandler)
    setHandler(out, this)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class GroupedWeighted[T](minWeight: Long, costFn: T => Long)
    extends GraphStage[FlowShape[T, immutable.Seq[T]]] {
  require(minWeight > 0, "minWeight must be greater than 0")

  val in = Inlet[T]("GroupedWeighted.in")
  val out = Outlet[immutable.Seq[T]]("GroupedWeighted.out")
  override val shape: FlowShape[T, immutable.Seq[T]] = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.groupedWeighted

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val buf = Vector.newBuilder[T]
      var left: Long = minWeight

      override def onPush(): Unit = {
        val elem = grab(in)
        val cost = costFn(elem)
        if (cost < 0L)
          failStage(new IllegalArgumentException(s"Negative weight [$cost] for element [$elem] is not allowed"))
        else {
          buf += elem
          left -= cost
          if (left <= 0) {
            val elements = buf.result()
            buf.clear()
            left = minWeight
            push(out, elements)
          } else {
            pull(in)
          }
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        // Since the upstream has finished we have to push any buffered elements downstream.
        val elements = buf.result()
        if (elements.nonEmpty) {
          buf.clear()
          left = minWeight
          push(out, elements)
        }
        completeStage()
      }

      setHandlers(in, out, this)
    }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class LimitWeighted[T](val n: Long, val costFn: T => Long)
    extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.limitWeighted and SourceLocation.forLambda(costFn)

  def createLogic(inheritedAttributes: Attributes) =
    new SupervisedGraphStageLogic(inheritedAttributes, shape) with InHandler with OutHandler {
      private var left = n

      override def onPush(): Unit = {
        val elem = grab(in)
        withSupervision(() => costFn(elem)) match {
          case OptionVal.Some(weight) =>
            left -= weight
            if (left >= 0) push(out, elem) else failStage(new StreamLimitReachedException(n))
          case _ => //do nothing
        }
      }

      override def onResume(t: Throwable): Unit = if (!hasBeenPulled(in)) pull(in)

      override def onRestart(t: Throwable): Unit = {
        left = n
        if (!hasBeenPulled(in)) pull(in)
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

  override def toString = "LimitWeighted"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Sliding[T](val n: Int, val step: Int)
    extends GraphStage[FlowShape[T, immutable.Seq[T]]] {
  require(n > 0, "n must be greater than 0")
  require(step > 0, "step must be greater than 0")

  val in = Inlet[T]("Sliding.in")
  val out = Outlet[immutable.Seq[T]]("Sliding.out")
  override val shape: FlowShape[T, immutable.Seq[T]] = FlowShape(in, out)

  override protected val initialAttributes: Attributes = DefaultAttributes.sliding

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var buf = Vector.empty[T]

      override def onPush(): Unit = {
        buf :+= grab(in)
        if (buf.size < n) {
          pull(in)
        } else if (buf.size == n) {
          push(out, buf)
        } else if (step <= n) {
          buf = buf.drop(step)
          if (buf.size == n) {
            push(out, buf)
          } else pull(in)
        } else {
          if (buf.size == step) {
            buf = buf.drop(step)
          }
          pull(in)
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {

        // We can finish current stage directly if:
        //  1. the buf is empty or
        //  2. when the step size is greater than the sliding size (step > n) and current stage is in between
        //     two sliding (ie. buf.size >= n && buf.size < step).
        //
        // Otherwise it means there is still a not finished sliding so we have to push them before finish current stage.
        if (buf.size < n && buf.size > 0) {
          push(out, buf)
        }
        completeStage()
      }

      this.setHandlers(in, out, this)
    }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Buffer[T](size: Int, overflowStrategy: OverflowStrategy)
    extends SimpleLinearGraphStage[T] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
      override protected def logSource: Class[_] = classOf[Buffer[_]]

      private val buffer: BufferImpl[T] = BufferImpl(size, inheritedAttributes)

      private val name = inheritedAttributes.nameOrDefault(getClass.toString)
      val enqueueAction: T => Unit =
        overflowStrategy match {
          case s: DropHead =>
            elem =>
              if (buffer.isFull) {
                log.log(
                  s.logLevel,
                  "Dropping the head element because buffer is full and overflowStrategy is: [DropHead] in stream [{}]",
                  name)
                buffer.dropHead()
              }
              buffer.enqueue(elem)
              pull(in)
          case s: DropTail =>
            elem =>
              if (buffer.isFull) {
                log.log(
                  s.logLevel,
                  "Dropping the tail element because buffer is full and overflowStrategy is: [DropTail] in stream [{}]",
                  name)
                buffer.dropTail()
              }
              buffer.enqueue(elem)
              pull(in)
          case s: DropBuffer =>
            elem =>
              if (buffer.isFull) {
                log.log(
                  s.logLevel,
                  "Dropping all the buffered elements because buffer is full and overflowStrategy is: [DropBuffer] in stream [{}]",
                  name)
                buffer.clear()
              }
              buffer.enqueue(elem)
              pull(in)
          case s: DropNew =>
            elem =>
              if (!buffer.isFull) buffer.enqueue(elem)
              else
                log.log(
                  s.logLevel,
                  "Dropping the new element because buffer is full and overflowStrategy is: [DropNew] in stream [{}]",
                  name)
              pull(in)
          case s: Backpressure =>
            elem =>
              buffer.enqueue(elem)
              if (!buffer.isFull) pull(in)
              else
                log.log(
                  s.logLevel,
                  "Backpressuring because buffer is full and overflowStrategy is: [Backpressure] in stream [{}]",
                  name)
          case s: Fail =>
            elem =>
              if (buffer.isFull) {
                log.log(
                  s.logLevel,
                  "Failing because buffer is full and overflowStrategy is: [Fail] in stream [{}]",
                  name)
                failStage(BufferOverflowException(s"Buffer overflow (max capacity was: $size)!"))
              } else {
                buffer.enqueue(elem)
                pull(in)
              }
        }

      override def preStart(): Unit = {
        pull(in)
      }

      override def onPush(): Unit = {
        val elem = grab(in)
        // If out is available, then it has been pulled but no dequeued element has been delivered.
        // It means the buffer at this moment is definitely empty,
        // so we just push the current element to out, then pull.
        if (isAvailable(out)) {
          push(out, elem)
          pull(in)
        } else {
          enqueueAction(elem)
        }
      }

      override def onPull(): Unit = {
        if (buffer.nonEmpty) push(out, buffer.dequeue())
        if (isClosed(in)) {
          if (buffer.isEmpty) completeStage()
        } else if (!hasBeenPulled(in)) {
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (buffer.isEmpty) completeStage()
      }

      setHandlers(in, out, this)
    }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Batch[In, Out](
    val max: Long,
    val costFn: In => Long,
    val seed: In => Out,
    val aggregate: (Out, In) => Out)
    extends GraphStage[FlowShape[In, Out]] {

  val in = Inlet[In]("Batch.in")
  val out = Outlet[Out]("Batch.out")

  override val shape: FlowShape[In, Out] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var agg: Out = null.asInstanceOf[Out]
      private var left: Long = max
      private var pending: In = null.asInstanceOf[In]
      private val contextPropagation = ContextPropagation()

      private def flush(): Unit = {
        if (agg != null) {
          push(out, agg)
          left = max
        }
        if (pending != null) {
          try {
            agg = seed(pending)
            left -= costFn(pending)
            pending = null.asInstanceOf[In]
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop    => failStage(ex)
                case Supervision.Restart => restartState()
                case Supervision.Resume =>
                  pending = null.asInstanceOf[In]
              }
          }
        } else {
          agg = null.asInstanceOf[Out]
        }
      }

      override def preStart() = pull(in)

      def onPush(): Unit = {
        val elem = grab(in)
        val cost = costFn(elem)
        contextPropagation.suspendContext()

        if (agg == null) {
          try {
            agg = seed(elem)
            left -= cost
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart =>
                  restartState()
                case Supervision.Resume =>
              }
          }
        } else if (left < cost) {
          pending = elem
        } else {
          try {
            agg = aggregate(agg, elem)
            left -= cost
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart =>
                  restartState()
                case Supervision.Resume =>
              }
          }
        }

        if (isAvailable(out)) flush()
        if (pending == null) pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (agg == null) completeStage()
      }

      def onPull(): Unit = {
        if (agg == null) {
          if (isClosed(in)) completeStage()
          else if (!hasBeenPulled(in)) pull(in)
        } else if (isClosed(in)) {
          contextPropagation.resumeContext()
          push(out, agg)
          if (pending == null) completeStage()
          else {
            try {
              agg = seed(pending)
            } catch {
              case NonFatal(ex) =>
                decider(ex) match {
                  case Supervision.Stop   => failStage(ex)
                  case Supervision.Resume =>
                  case Supervision.Restart =>
                    restartState()
                    if (!hasBeenPulled(in)) pull(in)
                }
            }
            pending = null.asInstanceOf[In]
          }
        } else {
          contextPropagation.resumeContext()
          flush()
          if (!hasBeenPulled(in)) pull(in)
        }

      }

      private def restartState(): Unit = {
        agg = null.asInstanceOf[Out]
        left = max
        pending = null.asInstanceOf[In]
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Expand[In, Out](val extrapolate: In => Iterator[Out])
    extends GraphStage[FlowShape[In, Out]] {
  private val in = Inlet[In]("expand.in")
  private val out = Outlet[Out]("expand.out")

  override def initialAttributes = DefaultAttributes.expand and SourceLocation.forLambda(extrapolate)

  override val shape = FlowShape(in, out)

  override def createLogic(attr: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
    private var iterator: Iterator[Out] = Iterator.empty
    private var expanded = false
    private val contextPropagation = ContextPropagation()

    override def preStart(): Unit = pull(in)

    def onPush(): Unit = {
      iterator = extrapolate(grab(in))
      if (iterator.hasNext) {
        contextPropagation.suspendContext()
        if (isAvailable(out)) {
          expanded = true
          pull(in)
          push(out, iterator.next())
        } else expanded = false
      } else pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      if (iterator.hasNext && !expanded) () // need to wait
      else completeStage()
    }

    def onPull(): Unit = {
      if (iterator.hasNext) {
        contextPropagation.resumeContext()
        if (!expanded) {
          expanded = true
          if (isClosed(in)) {
            push(out, iterator.next())
            completeStage()
          } else {
            // expand needs to pull first to be “fair” when upstream is not actually slow
            pull(in)
            push(out, iterator.next())
          }
        } else push(out, iterator.next())
      }
    }

    setHandler(in, this)
    setHandler(out, this)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object MapAsync {

  final class Holder[T](var elem: Try[T], val cb: AsyncCallback[Holder[T]]) extends (Try[T] => Unit) {

    // To support both fail-fast when the supervision directive is Stop
    // and not calling the decider multiple times (#23888) we need to cache the decider result and re-use that
    private var cachedSupervisionDirective: OptionVal[Supervision.Directive] = OptionVal.None

    def supervisionDirectiveFor(decider: Supervision.Decider, ex: Throwable): Supervision.Directive = {
      cachedSupervisionDirective match {
        case OptionVal.Some(d) => d
        case _ =>
          val d = decider(ex)
          cachedSupervisionDirective = OptionVal.Some(d)
          d
      }
    }

    def setElem(t: Try[T]): Unit = {
      elem = t
    }

    override def apply(t: Try[T]): Unit = {
      setElem(t)
      cb.invoke(this)
    }
  }

  val NotYetThere = Failure(new Exception with NoStackTrace)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class MapAsync[In, Out](parallelism: Int, f: In => Future[Out])
    extends GraphStage[FlowShape[In, Out]] {
  require(parallelism >= 1, "parallelism should >= 1")

  import MapAsync._

  private val in = Inlet[In]("MapAsync.in")
  private val out = Outlet[Out]("MapAsync.out")

  override def initialAttributes = DefaultAttributes.mapAsync and SourceLocation.forLambda(f)

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      var buffer: BufferImpl[Holder[Out]] = _

      private val futureCB = getAsyncCallback[Holder[Out]](holder =>
        holder.elem match {
          case Success(_) => pushNextIfPossible()
          case Failure(ex) =>
            holder.supervisionDirectiveFor(decider, ex) match {
              // fail fast as if supervision says so
              case Supervision.Stop => failStage(ex)
              case _                => pushNextIfPossible()
            }
        })

      override def preStart(): Unit = buffer = BufferImpl(parallelism, inheritedAttributes)

      override def onPull(): Unit = pushNextIfPossible()

      override def onPush(): Unit = {
        try {
          val future = f(grab(in))
          val holder = new Holder[Out](NotYetThere, futureCB)
          buffer.enqueue(holder)

          future.value match {
            case None    => future.onComplete(holder)(ExecutionContext.parasitic)
            case Some(v) =>
              // #20217 the future is already here, optimization: avoid scheduling it on the dispatcher and
              // run the logic directly on this thread
              holder.setElem(v)
              v match {
                // this optimization also requires us to stop the stage to fail fast if the decider says so:
                case Failure(ex) if holder.supervisionDirectiveFor(decider, ex) == Supervision.Stop => failStage(ex)
                case _                                                                              => pushNextIfPossible()
              }
          }

        } catch {
          // this logic must only be executed if f throws, not if the future is failed
          case NonFatal(ex) => if (decider(ex) == Supervision.Stop) failStage(ex)
        }

        pullIfNeeded()
      }

      override def onUpstreamFinish(): Unit = if (buffer.isEmpty) completeStage()

      @tailrec
      private def pushNextIfPossible(): Unit =
        if (buffer.isEmpty) pullIfNeeded()
        else if (buffer.peek().elem eq NotYetThere) pullIfNeeded() // ahead of line blocking to keep order
        else if (isAvailable(out)) {
          val holder = buffer.dequeue()
          holder.elem match {
            case Success(elem) =>
              if (elem != null) {
                push(out, elem)
                pullIfNeeded()
              } else {
                // elem is null
                pullIfNeeded()
                pushNextIfPossible()
              }

            case Failure(NonFatal(ex)) =>
              holder.supervisionDirectiveFor(decider, ex) match {
                // this could happen if we are looping in pushNextIfPossible and end up on a failed future before the
                // onComplete callback has run
                case Supervision.Stop => failStage(ex)
                case _                =>
                  // try next element
                  pushNextIfPossible()
              }
            case Failure(ex) =>
              // fatal exception in buffer, not sure that it can actually happen, but for good measure
              throw ex
          }
        }

      private def pullIfNeeded(): Unit = {
        if (isClosed(in) && buffer.isEmpty) completeStage()
        else if (buffer.used < parallelism && !hasBeenPulled(in)) tryPull(in)
        // else already pulled and waiting for next element
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class MapAsyncUnordered[In, Out](parallelism: Int, f: In => Future[Out])
    extends GraphStage[FlowShape[In, Out]] {

  private val in = Inlet[In]("MapAsyncUnordered.in")
  private val out = Outlet[Out]("MapAsyncUnordered.out")

  override def initialAttributes = DefaultAttributes.mapAsyncUnordered and SourceLocation.forLambda(f)

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def toString = s"MapAsyncUnordered.Logic(inFlight=$inFlight, buffer=$buffer)"

      lazy val decider =
        inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var inFlight = 0
      private var buffer: BufferImpl[Out] = _
      private val invokeFutureCB: Try[Out] => Unit = getAsyncCallback(futureCompleted).invoke

      private[this] def todo: Int = inFlight + buffer.used

      override def preStart(): Unit = buffer = BufferImpl(parallelism, inheritedAttributes)

      def futureCompleted(result: Try[Out]): Unit = {
        def isCompleted = isClosed(in) && todo == 0
        inFlight -= 1
        result match {
          case Success(elem) if elem != null =>
            if (isAvailable(out)) {
              if (!hasBeenPulled(in)) tryPull(in)
              push(out, elem)
              if (isCompleted) completeStage()
            } else buffer.enqueue(elem)
          case Success(_) =>
            if (isCompleted) completeStage()
            else if (!hasBeenPulled(in)) tryPull(in)
          case Failure(ex) =>
            if (decider(ex) == Supervision.Stop) failStage(ex)
            else if (isCompleted) completeStage()
            else if (!hasBeenPulled(in)) tryPull(in)
        }
      }

      override def onPush(): Unit = {
        try {
          val future = f(grab(in))
          inFlight += 1
          future.value match {
            case None    => future.onComplete(invokeFutureCB)(ExecutionContext.parasitic)
            case Some(v) => futureCompleted(v)
          }
        } catch {
          case NonFatal(ex) => if (decider(ex) == Supervision.Stop) failStage(ex)
        }
        if (todo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (todo == 0) completeStage()
      }

      override def onPull(): Unit = {
        if (!buffer.isEmpty) push(out, buffer.dequeue())

        val leftTodo = todo
        if (isClosed(in) && leftTodo == 0) completeStage()
        else if (leftTodo < parallelism && !hasBeenPulled(in)) tryPull(in)
      }

      setHandlers(in, out, this)
    }
}

@InternalApi private[akka] final case class Watch[T](targetRef: ActorRef) extends SimpleLinearGraphStage[T] {

  override def initialAttributes = DefaultAttributes.watch

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
      override protected def logSource: Class[_] = classOf[Watch[_]]

      override def preStart(): Unit = {
        val self = getStageActor {
          case (_, Terminated(`targetRef`)) =>
            failStage(new WatchedActorTerminatedException("Watch", targetRef))
          case (_, _) => // keep the compiler happy (stage actor receive is total)
        }
        self.watch(targetRef)
      }

      override def onPull(): Unit =
        pull(in)

      override def onPush(): Unit =
        push(out, grab(in))

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Log[T](name: String, extract: T => Any, logAdapter: Option[LoggingAdapter])
    extends SimpleLinearGraphStage[T] {

  override def toString = "Log"

  // TODO more optimisations can be done here - prepare logOnPush function etc
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {

      import Log._

      private var logLevels: LogLevels = _
      private var log: LoggingAdapter = _

      def decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      override def preStart(): Unit = {
        logLevels = inheritedAttributes.get[LogLevels](DefaultLogLevels)
        log = logAdapter match {
          case Some(l) => l
          case _ =>
            Logging(materializer.system, materializer)(fromMaterializer)
        }
      }

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          if (isEnabled(logLevels.onElement))
            log.log(logLevels.onElement, "[{}] Element: {}", name, extract(elem))

          push(out, elem)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _                => pull(in)
            }
        }
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFailure(cause: Throwable): Unit = {
        if (isEnabled(logLevels.onFailure))
          logLevels.onFailure match {
            case Logging.ErrorLevel => log.error(cause, "[{}] Upstream failed.", name)
            case level =>
              log.log(
                level,
                "[{}] Upstream failed, cause: {}: {}",
                name,
                Logging.simpleName(cause.getClass),
                cause.getMessage)
          }

        super.onUpstreamFailure(cause)
      }

      override def onUpstreamFinish(): Unit = {
        if (isEnabled(logLevels.onFinish))
          log.log(logLevels.onFinish, "[{}] Upstream finished.", name)

        super.onUpstreamFinish()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        if (isEnabled(logLevels.onFinish))
          log.log(
            logLevels.onFinish,
            "[{}] Downstream finished, cause: {}: {}",
            name,
            Logging.simpleName(cause.getClass),
            cause.getMessage)

        super.onDownstreamFinish(cause: Throwable)
      }

      private def isEnabled(l: LogLevel): Boolean = l.asInt != OffInt

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Log {

  /**
   * Must be located here to be visible for implicit resolution, when [[Materializer]] is passed to [[Logging]]
   * More specific LogSource than `fromString`, which would add the ActorSystem name in addition to the supervision to the log source.
   */
  final val fromMaterializer = new LogSource[Materializer] {

    // do not expose private context classes (of OneBoundedInterpreter)
    override def getClazz(t: Materializer): Class[_] = classOf[Materializer]

    override def genString(t: Materializer): String = {
      try s"$DefaultLoggerName(${t.supervisor.path})"
      catch {
        case _: Exception => LogSource.fromString.genString(DefaultLoggerName)
      }
    }

  }

  private final val DefaultLoggerName = "akka.stream.Log"
  private final val OffInt = LogLevels.Off.asInt
  private final val DefaultLogLevels =
    LogLevels(onElement = Logging.DebugLevel, onFinish = Logging.DebugLevel, onFailure = Logging.ErrorLevel)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class LogWithMarker[T](
    name: String,
    marker: T => LogMarker,
    extract: T => Any,
    logAdapter: Option[MarkerLoggingAdapter])
    extends SimpleLinearGraphStage[T] {

  override def toString = "LogWithMarker"

  // TODO more optimisations can be done here - prepare logOnPush function etc
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {

      import LogWithMarker._

      private var logLevels: LogLevels = _
      private var log: MarkerLoggingAdapter = _

      def decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      override def preStart(): Unit = {
        logLevels = inheritedAttributes.get[LogLevels](DefaultLogLevels)
        log = logAdapter match {
          case Some(l) => l
          case _ =>
            Logging.withMarker(materializer.system, materializer)(fromMaterializer)
        }
      }

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          if (isEnabled(logLevels.onElement))
            log.log(marker(elem), logLevels.onElement, log.format("[{}] Element: {}", name, extract(elem)))

          push(out, elem)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _                => pull(in)
            }
        }
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFailure(cause: Throwable): Unit = {
        if (isEnabled(logLevels.onFailure))
          logLevels.onFailure match {
            case Logging.ErrorLevel => log.error(cause, "[{}] Upstream failed.", name)
            case level =>
              log.log(
                level,
                "[{}] Upstream failed, cause: {}: {}",
                name,
                Logging.simpleName(cause.getClass),
                cause.getMessage)
          }

        super.onUpstreamFailure(cause)
      }

      override def onUpstreamFinish(): Unit = {
        if (isEnabled(logLevels.onFinish))
          log.log(logLevels.onFinish, "[{}] Upstream finished.", name)

        super.onUpstreamFinish()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        if (isEnabled(logLevels.onFinish))
          log.log(
            logLevels.onFinish,
            "[{}] Downstream finished, cause: {}: {}",
            name,
            Logging.simpleName(cause.getClass),
            cause.getMessage)

        super.onDownstreamFinish(cause: Throwable)
      }

      private def isEnabled(l: LogLevel): Boolean = l.asInt != OffInt

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LogWithMarker {

  /**
   * Must be located here to be visible for implicit resolution, when [[Materializer]] is passed to [[Logging]]
   * More specific LogSource than `fromString`, which would add the ActorSystem name in addition to the supervision to the log source.
   */
  final val fromMaterializer = new LogSource[Materializer] {

    // do not expose private context classes (of OneBoundedInterpreter)
    override def getClazz(t: Materializer): Class[_] = classOf[Materializer]

    override def genString(t: Materializer): String = {
      try s"$DefaultLoggerName(${t.supervisor.path})"
      catch {
        case _: Exception => LogSource.fromString.genString(DefaultLoggerName)
      }
    }

  }

  private final val DefaultLoggerName = "akka.stream.LogWithMarker"
  private final val OffInt = LogLevels.Off.asInt
  private final val DefaultLogLevels =
    LogLevels(onElement = Logging.DebugLevel, onFinish = Logging.DebugLevel, onFailure = Logging.ErrorLevel)
}

@InternalApi private[akka] object GroupedWeightedWithin {
  val groupedWeightedWithinTimer = "GroupedWeightedWithinTimer"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class GroupedWeightedWithin[T](
    val maxWeight: Long,
    val maxNumber: Int,
    val costFn: T => Long,
    val interval: FiniteDuration)
    extends GraphStage[FlowShape[T, immutable.Seq[T]]] {
  require(maxWeight > 0, "maxWeight must be greater than 0")
  require(maxNumber > 0, "maxNumber must be greater than 0")
  require(interval > Duration.Zero)

  val in = Inlet[T]("in")
  val out = Outlet[immutable.Seq[T]]("out")

  override def initialAttributes = DefaultAttributes.groupedWeightedWithin and SourceLocation.forLambda(costFn)

  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private val buf: VectorBuilder[T] = new VectorBuilder
      private var pending: T = null.asInstanceOf[T]
      private var pendingWeight: Long = 0L
      // True if:
      // - buf is nonEmpty
      //       AND
      // - (timer fired
      //        OR
      //    totalWeight >= maxWeight
      //        OR
      //    pending != null
      //        OR
      //    upstream completed)
      private var pushEagerly = false
      private var groupEmitted = true
      private var finished = false
      private var totalWeight = 0L
      private var totalNumber = 0
      private var hasElements = false
      private val contextPropagation = ContextPropagation()

      override def preStart() = {
        scheduleWithFixedDelay(GroupedWeightedWithin.groupedWeightedWithinTimer, interval, interval)
        pull(in)
      }

      private def nextElement(elem: T): Unit = {
        groupEmitted = false
        val cost = costFn(elem)
        if (cost < 0L)
          failStage(new IllegalArgumentException(s"Negative weight [$cost] for element [$elem] is not allowed"))
        else {
          hasElements = true
          // if there is place (both weight and number) for `elem` in the current group
          if (totalWeight + cost <= maxWeight && totalNumber + 1 <= maxNumber) {
            buf += elem
            totalWeight += cost
            totalNumber += 1;

            // if potentially there is a place (both weight and number) for one more element in the current group
            if (totalWeight < maxWeight && totalNumber < maxNumber) pull(in)
            else {
              if (!isAvailable(out)) {
                // we should emit group when downstream becomes available
                pushEagerly = true
                // we want to pull anyway, since we allow for zero weight elements
                // but since `emitGroup()` will pull internally (by calling `startNewGroup()`)
                // we also have to pull if downstream hasn't yet requested an element.
                pull(in)
              } else {
                scheduleWithFixedDelay(GroupedWeightedWithin.groupedWeightedWithinTimer, interval, interval)
                emitGroup()
              }
            }
          } else {
            // if there is a single heavy element that weighs more than the limit
            if (totalWeight == 0L && totalNumber == 0) {
              buf += elem
              totalWeight += cost
              totalNumber += 1;
              pushEagerly = true
            } else {
              pending = elem
              pendingWeight = cost
            }
            scheduleWithFixedDelay(GroupedWeightedWithin.groupedWeightedWithinTimer, interval, interval)
            tryCloseGroup()
          }
        }
      }

      private def tryCloseGroup(): Unit = {
        if (isAvailable(out)) emitGroup()
        else if (pending != null || finished) pushEagerly = true
      }

      private def emitGroup(): Unit = {
        groupEmitted = true
        contextPropagation.resumeContext()
        push(out, buf.result())
        buf.clear()
        if (!finished) startNewGroup()
        else if (pending != null) emit(out, Vector(pending), () => completeStage())
        else completeStage()
      }

      private def startNewGroup(): Unit = {
        if (pending != null) {
          totalWeight = pendingWeight
          totalNumber = 1
          pendingWeight = 0L
          buf += pending
          pending = null.asInstanceOf[T]
          groupEmitted = false
        } else {
          totalWeight = 0L
          totalNumber = 0
          hasElements = false
        }
        pushEagerly = false
        if (isAvailable(in)) nextElement(grab(in))
        else if (!hasBeenPulled(in)) pull(in)
      }

      override def onPush(): Unit = {
        contextPropagation.suspendContext()
        if (pending == null) nextElement(grab(in)) // otherwise keep the element for next round
      }

      override def onPull(): Unit = if (pushEagerly) emitGroup()

      override def onUpstreamFinish(): Unit = {
        finished = true
        if (groupEmitted) completeStage()
        else tryCloseGroup()
      }

      override protected def onTimer(timerKey: Any) = if (hasElements) {
        if (isAvailable(out)) emitGroup()
        else pushEagerly = true
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi object Delay {
  private val TimerName = "DelayedTimer"
  private val DelayPrecisionMS = 10
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Delay[T](
    delayStrategySupplier: () => DelayStrategy[T],
    overflowStrategy: DelayOverflowStrategy)
    extends SimpleLinearGraphStage[T] {

  override def initialAttributes: Attributes = DefaultAttributes.delay

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      import Delay._

      private[this] val size = inheritedAttributes.mandatoryAttribute[InputBuffer].max

      private[this] val delayStrategy = delayStrategySupplier()

      // buffer has pairs of timestamp of expected push and element
      private[this] val buffer = BufferImpl[(Long, T)](size, inheritedAttributes)

      private[this] val onPushWhenBufferFull: () => Unit = overflowStrategy match {
        case EmitEarly =>
          () => {
            if (isAvailable(out)) {
              if (isTimerActive(TimerName)) {
                cancelTimer(TimerName)
              }

              push(out, buffer.dequeue()._2)
              grabAndPull()
              completeIfReady()
            } else {
              throw new IllegalStateException(
                "Was configured to emitEarly and got element when out is not ready and buffer is full, should not be possible.")
            }
          }
        case _: DropHead =>
          () => {
            buffer.dropHead()
            grabAndPull()
          }
        case _: DropTail =>
          () => {
            buffer.dropTail()
            grabAndPull()
          }
        case _: DropNew =>
          () => {
            grab(in)
            if (shouldPull) pull(in)
          }
        case _: DropBuffer =>
          () => {
            buffer.clear()
            grabAndPull()
          }
        case _: Fail =>
          () => {
            failStage(new BufferOverflowException(s"Buffer overflow for delay operator (max capacity was: $size)!"))
          }
        case _: Backpressure =>
          () => {
            throw new IllegalStateException("Delay buffer must never overflow in Backpressure mode")
          }
      }

      def onPush(): Unit = {
        if (buffer.isFull)
          onPushWhenBufferFull()
        else {
          grabAndPull()
          if (!isTimerActive(TimerName)) {
            val waitTime = nextElementWaitTime()
            if (waitTime <= DelayPrecisionMS && isAvailable(out)) {
              push(out, buffer.dequeue()._2)
              completeIfReady()
            } else
              scheduleOnce(TimerName, waitTime.millis)
          }
        }
      }

      private def shouldPull: Boolean =
        buffer.used < size || !overflowStrategy.isBackpressure ||
        // we can only emit early if output is ready
        (overflowStrategy == EmitEarly && isAvailable(out))

      private def grabAndPull(): Unit = {
        val element = grab(in)
        buffer.enqueue((System.nanoTime() + delayStrategy.nextDelay(element).toNanos, element))
        if (shouldPull) pull(in)
      }

      override def onUpstreamFinish(): Unit =
        completeIfReady()

      def onPull(): Unit = {
        if (!isTimerActive(TimerName) && !buffer.isEmpty) {
          val waitTime = nextElementWaitTime()
          if (waitTime <= DelayPrecisionMS)
            push(out, buffer.dequeue()._2)
          else
            scheduleOnce(TimerName, waitTime.millis)
        }

        if (!isClosed(in) && !hasBeenPulled(in) && shouldPull)
          pull(in)

        completeIfReady()
      }

      setHandler(in, this)
      setHandler(out, this)

      def completeIfReady(): Unit = if (isClosed(in) && buffer.isEmpty) completeStage()

      private def nextElementWaitTime(): Long = {
        NANOSECONDS.toMillis(buffer.peek()._1 - System.nanoTime())
      }

      final override protected def onTimer(key: Any): Unit = {
        if (isAvailable(out))
          push(out, buffer.dequeue()._2)

        completeIfReady()
      }
    }

  override def toString = "Delay"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object TakeWithin {
  val takeWithinTimer = "TakeWithinTimer"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class TakeWithin[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      def onPush(): Unit = push(out, grab(in))

      def onPull(): Unit = pull(in)

      setHandlers(in, out, this)

      final override protected def onTimer(key: Any): Unit = completeStage()

      override def preStart(): Unit = scheduleOnce(TakeWithin.takeWithinTimer, timeout)
    }

  override def toString = "TakeWithin"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class DropWithin[T](val timeout: FiniteDuration) extends SimpleLinearGraphStage[T] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val startNanoTime = System.nanoTime()
      private val timeoutInNano = timeout.toNanos

      def onPush(): Unit = {
        if (System.nanoTime() - startNanoTime <= timeoutInNano) {
          pull(in)
        } else {
          push(out, grab(in))
          // change the in handler to avoid System.nanoTime call after timeout
          setHandler(in, new InHandler {
            def onPush() = push(out, grab(in))
          })
        }
      }

      def onPull(): Unit = pull(in)

      setHandlers(in, out, this)

    }

  override def toString = "DropWithin"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Reduce[T](val f: (T, T) => T) extends SimpleLinearGraphStage[T] {
  override def initialAttributes: Attributes = DefaultAttributes.reduce and SourceLocation.forLambda(f)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler { self =>
      override def toString = s"Reduce.Logic(aggregator=$aggregator)"

      private var aggregator: T = _
      private val empty: T = aggregator

      private def decider =
        inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      def setInitialInHandler(): Unit = {
        // Initial input handler
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            aggregator = grab(in)
            pull(in)
            setHandler(in, self)
          }

          override def onUpstreamFinish(): Unit =
            failStage(new NoSuchElementException("reduce over empty stream"))
        })
      }

      override def onPush(): Unit = {
        val elem = grab(in)
        try {
          aggregator = f(aggregator, elem)
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case Supervision.Restart =>
                aggregator = empty
                setInitialInHandler()
              case _ => ()

            }
        } finally {
          if (!isClosed(in)) pull(in)
        }
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        push(out, aggregator)
        completeStage()
      }

      setInitialInHandler()
      setHandler(out, self)
    }

  override def toString = "Reduce"
}

/**
 * INTERNAL API
 */
@InternalApi private[stream] object RecoverWith

@InternalApi private[akka] final class RecoverWith[T, M](
    val maximumRetries: Int,
    val pf: PartialFunction[Throwable, Graph[SourceShape[T], M]])
    extends SimpleLinearGraphStage[T] {

  override def initialAttributes: Attributes = DefaultAttributes.recoverWith

  override def createLogic(attr: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      var attempt = 0
      override def onPush(): Unit = push(out, grab(in))
      override def onUpstreamFailure(ex: Throwable): Unit = onFailure(ex)
      override def onPull(): Unit = pull(in)
      def onFailure(ex: Throwable): Unit = {
        import Collect.NotApplied
        if (maximumRetries < 0 || attempt < maximumRetries) {
          pf.applyOrElse(ex, NotApplied) match {
            case NotApplied => failStage(ex)
            case source: Graph[SourceShape[T] @unchecked, M @unchecked] if TraversalBuilder.isEmptySource(source) =>
              completeStage()
            case other: Graph[SourceShape[T] @unchecked, M @unchecked] =>
              switchTo(other)
              attempt += 1
            case _ => throw new IllegalStateException() // won't happen, compiler exhaustiveness check pleaser
          }
        } else
          failStage(ex)
      }

      def switchTo(source: Graph[SourceShape[T], M]): Unit = {
        val sinkIn = new SubSinkInlet[T]("RecoverWithSink")

        sinkIn.setHandler(new InHandler {
          override def onPush(): Unit = push(out, sinkIn.grab())

          override def onUpstreamFinish(): Unit = completeStage()

          override def onUpstreamFailure(ex: Throwable): Unit = onFailure(ex)
        })

        val outHandler = new OutHandler {
          override def onPull(): Unit = sinkIn.pull()

          override def onDownstreamFinish(cause: Throwable): Unit = sinkIn.cancel(cause)
        }

        Source.fromGraph(source).runWith(sinkIn.sink)(interpreter.subFusingMaterializer)
        setHandler(out, outHandler)
        if (isAvailable(out)) sinkIn.pull()
      }

      setHandlers(in, out, this)
    }

  override def toString: String = "RecoverWith"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object StatefulMap {
  private final class NullStateException(msg: String) extends NullPointerException(msg)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class StatefulMap[S, In, Out](create: () => S, f: (S, In) => (S, Out), onComplete: S => Option[Out])
    extends GraphStage[FlowShape[In, Out]] {
  import StatefulMap.NullStateException

  require(create != null, "create function should not be null")
  require(f != null, "f function should not be null")
  require(onComplete != null, "onComplete function should not be null")

  private val in = Inlet[In]("StatefulMap.in")
  private val out = Outlet[Out]("StatefulMap.out")
  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      lazy val decider: Decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var state: OptionVal[S] = OptionVal.none

      override def preStart(): Unit = {
        createNewState()
      }

      override def onPush(): Unit = {
        try {
          val elem = grab(in)
          val (newState, newElem) = f(state.get, elem)
          state = OptionVal.Some(newState)
          throwIfNoState()
          push(out, newElem)
        } catch {
          case ex: NullStateException => throw ex // don't cover with supervision
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop    => closeStateAndFail(ex)
              case Supervision.Resume  => pull(in)
              case Supervision.Restart => restartState()
            }
        }
      }

      override def onUpstreamFinish(): Unit = {
        completeStateIfNeeded() match {
          case Some(elem) => emit(out, elem, () => completeStage())
          case None       => completeStage()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = closeStateAndFail(ex)

      override def onDownstreamFinish(cause: Throwable): Unit = {
        completeStateIfNeeded()
        super.onDownstreamFinish(cause)
      }

      private def createNewState(): Unit = {
        state = OptionVal.Some(create())
        throwIfNoState()
      }

      private def restartState(): Unit = {
        completeStateIfNeeded() match {
          case Some(elem) =>
            push(out, elem)
            createNewState()
          case None =>
            createNewState()
            // should always happen here but for good measure
            if (!hasBeenPulled(in)) pull(in)
        }
      }

      private def closeStateAndFail(ex: Throwable): Unit = {
        completeStateIfNeeded() match {
          case Some(elem) => emit(out, elem, () => failStage(ex))
          case None       => failStage(ex)
        }
      }

      private def completeStateIfNeeded(): Option[Out] = {
        state match {
          case OptionVal.Some(s) =>
            state = OptionVal.none[S]
            onComplete(s)
          case _ => None
        }
      }

      override def onPull(): Unit = pull(in)

      override def postStop(): Unit = {
        completeStateIfNeeded()
      }

      private def throwIfNoState(): Unit = {
        if (state.isEmpty) // Note: no state == null because optionval
          throw new NullStateException(
            "State returned by stateFulMap create lambda or mapping function was null, which is not allowed. " +
            "Use Option or Optional to represent presence of state if needed.")
      }

      setHandlers(in, out, this)
    }

  override def toString = "StatefulMap"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class StatefulMapConcat[In, Out](val f: () => In => IterableOnce[Out])
    extends GraphStage[FlowShape[In, Out]] {
  val in = Inlet[In]("StatefulMapConcat.in")
  val out = Outlet[Out]("StatefulMapConcat.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = DefaultAttributes.statefulMapConcat and SourceLocation.forLambda(f)

  def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
    lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    var currentIterator: Iterator[Out] = _
    var plainFun = f()
    val contextPropagation = ContextPropagation()

    def hasNext = if (currentIterator != null) currentIterator.hasNext else false

    setHandlers(in, out, this)

    def pushPull(shouldResumeContext: Boolean): Unit =
      if (hasNext) {
        if (shouldResumeContext) contextPropagation.resumeContext()
        push(out, currentIterator.next())
        if (hasNext) {
          // suspend context for the next element
          contextPropagation.suspendContext()
        } else if (isClosed(in)) completeStage()
      } else if (!isClosed(in))
        pull(in)
      else completeStage()

    def onFinish(): Unit = if (!hasNext) completeStage()

    override def onPush(): Unit =
      try {
        currentIterator = plainFun(grab(in)).iterator
        pushPull(shouldResumeContext = false)
      } catch handleException

    override def onUpstreamFinish(): Unit = onFinish()

    override def onPull(): Unit =
      try pushPull(shouldResumeContext = true)
      catch handleException

    private def handleException: Catcher[Unit] = {
      case NonFatal(ex) =>
        decider(ex) match {
          case Supervision.Stop => failStage(ex)
          case Supervision.Resume =>
            if (isClosed(in)) completeStage()
            else if (!hasBeenPulled(in)) pull(in)
          case Supervision.Restart =>
            if (isClosed(in)) completeStage()
            else {
              restartState()
              if (!hasBeenPulled(in)) pull(in)
            }
        }
    }

    private def restartState(): Unit = {
      plainFun = f()
      currentIterator = null
    }
  }

  override def toString = "StatefulMapConcat"

}

// This file is perhaps getting long (Github Issue #31619), please add new operators in other files
