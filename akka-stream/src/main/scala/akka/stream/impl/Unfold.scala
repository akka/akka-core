/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

import akka.annotation.InternalApi
import akka.japi.{ function, Pair }
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Unfold[S, E](s: S, f: S => Option[(S, E)]) extends GraphStage[SourceShape[E]] {
  val out: Outlet[E] = Outlet("Unfold.out")
  override val shape: SourceShape[E] = SourceShape(out)
  override def initialAttributes: Attributes = DefaultAttributes.unfold
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private[this] var state = s

      def onPull(): Unit = f(state) match {
        case Some((newState, v)) => {
          push(out, v)
          state = newState
        }
        case None => complete(out)
      }

      setHandler(out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldAsync[S, E](s: S, f: S => Future[Option[(S, E)]])
    extends GraphStage[SourceShape[E]] {
  val out: Outlet[E] = Outlet("UnfoldAsync.out")
  override val shape: SourceShape[E] = SourceShape(out)
  override def initialAttributes: Attributes = DefaultAttributes.unfoldAsync
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private[this] var state = s
      private[this] var asyncHandler: Try[Option[(S, E)]] => Unit = _

      override def preStart(): Unit = {
        asyncHandler = getAsyncCallback[Try[Option[(S, E)]]](handle).invoke
      }

      private def handle(result: Try[Option[(S, E)]]): Unit = result match {
        case Success(Some((newS, elem))) =>
          push(out, elem)
          state = newS
        case Success(None) => complete(out)
        case Failure(ex)   => fail(out, ex)
      }

      def onPull(): Unit = {
        val future = f(state)
        future.value match {
          case Some(value) => handle(value)
          case None =>
            future.onComplete(asyncHandler)(ExecutionContext.parasitic)
        }
      }

      setHandler(out, this)
    }
}

/**
 * [[UnfoldAsync]] optimized specifically for Java API and `CompletionStage`
 *
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldAsyncJava[S, E](
    s: S,
    f: function.Function[S, CompletionStage[Optional[Pair[S, E]]]])
    extends GraphStage[SourceShape[E]] {
  val out: Outlet[E] = Outlet("UnfoldAsync.out")
  override val shape: SourceShape[E] = SourceShape(out)
  override def initialAttributes: Attributes = DefaultAttributes.unfoldAsync
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private[this] var state = s
      private[this] var asyncHandler: Try[Optional[Pair[S, E]]] => Unit = _

      override def preStart(): Unit = {
        asyncHandler = getAsyncCallback[Try[Optional[Pair[S, E]]]](handle).invoke
      }

      private def handle(result: Try[Optional[Pair[S, E]]]): Unit = result match {
        case Success(maybeValue) => handle(maybeValue)
        case Failure(ex)         => fail(out, ex)
      }

      private def handle(maybeValue: Optional[Pair[S, E]]): Unit = {
        if (maybeValue.isPresent) {
          val pair = maybeValue.get()
          push(out, pair.second)
          state = pair.first
        } else {
          complete(out)
        }
      }

      def onPull(): Unit = {
        val future = f.apply(state).toCompletableFuture
        if (future.isDone && !future.isCompletedExceptionally) {
          handle(future.getNow(null))
        } else {
          future.handle((r, ex) => {
            if (ex != null) {
              asyncHandler(Failure(ex))
            } else {
              asyncHandler(Success(r))
            }
            null
          })
        }
      }
      setHandler(out, this)
    }
}
