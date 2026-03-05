/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.function.{ Function => JFunction }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.blocking
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.util.function.Consumer
import java.util.function.BiFunction
import java.util.function.BiConsumer
import scala.concurrent.ExecutionContextExecutor
import java.util.concurrent.ForkJoinPool

// This code is adapted from https://github.com/scala/scala/blob/2.13.x/src/library/scala/jdk/javaapi/FutureConverters.scala
// and https://github.com/scala/scala/blob/2.13.x/src/library/scala/concurrent/impl/FutureConvertersImpl.scala
/** Like the Scala stdlib FutureConverters, except shifts onto a provided execution context (rather than
 *  the ForkJoin common pool as in the Scala stdlib) when converting to Java
 */
object DispatcherFutureConverters {
  def asJava[T](f: Future[T], shiftTo: Executor): CompletionStage[T] =
    f match {
      // Not strictly safe as optimizations go, as there could be in a bizarre set of circumstances a Future[T] with CompletionStage[NotT]
      case c: CompletionStage[T @unchecked] => c
      case _ =>
        val cf = new CF(f, shiftTo)
        f.onComplete(cf)(ExecutionContext.parasitic)
        cf
    }

  implicit class ScalaFutureOpsWithShiftTo[T](val scalaFuture: Future[T]) extends AnyVal {
    def asJava(shiftTo: Executor): CompletionStage[T] =
      DispatcherFutureConverters.asJava(scalaFuture, shiftTo)

    def asJava(shiftTo: ExecutionContext): CompletionStage[T] = {
      val executor = shiftTo match {
        case ece: ExecutionContextExecutor => ece
        case _                             => ForkJoinPool.commonPool // match the Scala stdlib behavior when the provided EC isn't an Executor
      }
      DispatcherFutureConverters.asJava(scalaFuture, executor)
    }
  }

  /** A minimal implementation of a Java CompletableFuture which shifts further computation onto the given
   *  Executor, rather than be parasitic on the completer of the wrapped Scala future.
   *
   *  Further "parasitic" computations on the returned CompletableFutures will behave as normal in Java
   *  (viz. be parasitic).
   */
  private class CF[T](val wrapped: Future[T], shiftTo: Executor) extends CompletableFuture[T] with (Try[T] => Unit) {
    override def apply(t: Try[T]): Unit =
      t match {
        case Success(v) => complete(v)
        case Failure(e) => completeExceptionally(e)
      }

    override def thenApply[U](f: JFunction[_ >: T, _ <: U]): CompletableFuture[U] = thenApplyAsync(f, shiftTo)
    override def thenAccept(f: Consumer[_ >: T]): CompletableFuture[Void] = thenAcceptAsync(f, shiftTo)
    override def thenRun(f: Runnable): CompletableFuture[Void] = thenRunAsync(f, shiftTo)
    override def thenCombine[U, V](
        cs: CompletionStage[_ <: U],
        f: BiFunction[_ >: T, _ >: U, _ <: V]): CompletableFuture[V] = thenCombineAsync(cs, f, shiftTo)

    override def thenAcceptBoth[U](
        cs: CompletionStage[_ <: U],
        f: BiConsumer[_ >: T, _ >: U]): CompletableFuture[Void] = thenAcceptBothAsync(cs, f, shiftTo)

    override def runAfterBoth(cs: CompletionStage[_], f: Runnable): CompletableFuture[Void] =
      runAfterBothAsync(cs, f, shiftTo)

    override def applyToEither[U](cs: CompletionStage[_ <: T], f: JFunction[_ >: T, U]): CompletableFuture[U] =
      applyToEitherAsync(cs, f, shiftTo)

    override def acceptEither(cs: CompletionStage[_ <: T], f: Consumer[_ >: T]): CompletableFuture[Void] =
      acceptEitherAsync(cs, f, shiftTo)

    override def runAfterEither(cs: CompletionStage[_], f: Runnable): CompletableFuture[Void] =
      runAfterEitherAsync(cs, f, shiftTo)

    override def thenCompose[U](f: JFunction[_ >: T, _ <: CompletionStage[U]]): CompletableFuture[U] =
      thenComposeAsync(f, shiftTo)

    override def whenComplete(f: BiConsumer[_ >: T, _ >: Throwable]): CompletableFuture[T] =
      whenCompleteAsync(f, shiftTo)

    override def handle[U](f: BiFunction[_ >: T, Throwable, _ <: U]): CompletableFuture[U] =
      handleAsync(f, shiftTo)

    override def exceptionally(f: JFunction[Throwable, _ <: T]): CompletableFuture[T] = {
      val cf = new CompletableFuture[T]
      whenComplete((t, e) => { // implicitly shifts to shiftTo
        if (e == null) cf.complete(t)
        else {
          val n: AnyRef = try {
            f(e).asInstanceOf[AnyRef]
          } catch {
            case thr: Throwable =>
              cf.completeExceptionally(thr)
              this
          }

          if (n ne this) {
            cf.complete(n.asInstanceOf[T])
          } // else do nothing (already completed exceptionally)
        }
      })

      cf
    }

    /** @inheritdoc
     *
     *  NOTE: calling complete on this method will not complete the underlying Scala future
     */
    override def toCompletableFuture(): CompletableFuture[T] = this

    override def obtrudeValue(x: T): Unit =
      throw new UnsupportedOperationException("obtruding a value is not supported for a Scala-originating future")

    override def obtrudeException(t: Throwable): Unit =
      throw new UnsupportedOperationException("obtruding an exception is not supported for a Scala-originating future")

    override def get(): T = blocking { super.get() }

    override def get(timeout: Long, unit: TimeUnit): T = blocking { super.get(timeout, unit) }

    override def toString(): String = super[CompletableFuture].toString
  }
}
