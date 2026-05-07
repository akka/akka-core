/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.BiConsumer

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.actor._
import akka.dispatch.Futures

trait FutureTimeoutSupport {

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with the success or failure of the provided value
   * after the specified duration.
   */
  def after[T](duration: FiniteDuration)(value: => Future[T])(
      implicit system: ClassicActorSystemProvider): Future[T] = {
    after(duration, using = system.classicSystem.scheduler)(value)(system.classicSystem.dispatcher)
  }

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with the success or failure of the provided value
   * after the specified duration.
   */
  def afterCompletionStage[T](duration: FiniteDuration)(value: => CompletionStage[T])(
      implicit system: ClassicActorSystemProvider): CompletionStage[T] =
    afterCompletionStage(duration, system.classicSystem.scheduler)(value)(system.classicSystem.dispatcher)

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with the success or failure of the provided value
   * after the specified duration.
   */
  def after[T](duration: FiniteDuration, using: Scheduler)(value: => Future[T])(
      implicit ec: ExecutionContext): Future[T] =
    if (duration.isFinite && duration.length < 1) {
      try value
      catch { case NonFatal(t) => Future.failed(t) }
    } else {
      val p = Promise[T]()
      using.scheduleOnce(duration) {
        p.completeWith {
          try value
          catch { case NonFatal(t) => Future.failed(t) }
        }
      }
      p.future
    }

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with the success or failure of the provided value
   * after the specified duration.
   */
  def afterCompletionStage[T](duration: FiniteDuration, using: Scheduler)(value: => CompletionStage[T])(
      implicit ec: ExecutionContext): CompletionStage[T] =
    if (duration.isFinite && duration.length < 1) {
      try value
      catch { case NonFatal(t) => Futures.failedCompletionStage(t) }
    } else {
      val p = new CompletableFuture[T]
      using.scheduleOnce(duration) {
        try {
          val future = value
          future.whenComplete(new BiConsumer[T, Throwable] {
            override def accept(t: T, ex: Throwable): Unit = {
              if (t != null) p.complete(t)
              if (ex != null) p.completeExceptionally(ex)
            }
          })
        } catch {
          case NonFatal(ex) => p.completeExceptionally(ex)
        }
      }
      p
    }
}
