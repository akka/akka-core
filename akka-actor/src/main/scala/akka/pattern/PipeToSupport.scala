/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.concurrent.CompletionStage
import java.util.function.BiConsumer

import scala.annotation.nowarn
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

import language.implicitConversions

import akka.actor.{ Actor, ActorRef, Status }
import akka.actor.ActorSelection

trait PipeToSupport {

  final class PipeableFuture[T](val future: Future[T])(implicit executionContext: ExecutionContext) {
    def pipeTo(recipient: ActorRef)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
      future.andThen {
        case Success(r) => recipient ! r
        case Failure(f) => recipient ! Status.Failure(f)
      }
    }
    def pipeToSelection(recipient: ActorSelection)(implicit sender: ActorRef = Actor.noSender): Future[T] = {
      future.andThen {
        case Success(r) => recipient ! r
        case Failure(f) => recipient ! Status.Failure(f)
      }
    }
    def to(recipient: ActorRef): PipeableFuture[T] = to(recipient, Actor.noSender)
    def to(recipient: ActorRef, sender: ActorRef): PipeableFuture[T] = {
      pipeTo(recipient)(sender)
      this
    }
    def to(recipient: ActorSelection): PipeableFuture[T] = to(recipient, Actor.noSender)
    def to(recipient: ActorSelection, sender: ActorRef): PipeableFuture[T] = {
      pipeToSelection(recipient)(sender)
      this
    }
  }

  final class PipeableCompletionStage[T](val future: CompletionStage[T])(
      implicit @nowarn("msg=never used") executionContext: ExecutionContext) {
    def pipeTo(recipient: ActorRef)(implicit sender: ActorRef = Actor.noSender): CompletionStage[T] = {
      future.whenComplete(new BiConsumer[T, Throwable] {
        override def accept(t: T, ex: Throwable): Unit = {
          if (t != null) recipient ! t
          if (ex != null) recipient ! Status.Failure(ex)
        }
      })
    }
    def pipeToSelection(recipient: ActorSelection)(implicit sender: ActorRef = Actor.noSender): CompletionStage[T] = {
      future.whenComplete(new BiConsumer[T, Throwable] {
        override def accept(t: T, ex: Throwable): Unit = {
          if (t != null) recipient ! t
          if (ex != null) recipient ! Status.Failure(ex)
        }
      })
    }
    def to(recipient: ActorRef): PipeableCompletionStage[T] = to(recipient, Actor.noSender)
    def to(recipient: ActorRef, sender: ActorRef): PipeableCompletionStage[T] = {
      pipeTo(recipient)(sender)
      this
    }
    def to(recipient: ActorSelection): PipeableCompletionStage[T] = to(recipient, Actor.noSender)
    def to(recipient: ActorSelection, sender: ActorRef): PipeableCompletionStage[T] = {
      pipeToSelection(recipient)(sender)
      this
    }
  }

  /**
   * Import this implicit conversion to gain the `pipeTo` method on [[scala.concurrent.Future]]:
   *
   * {{{
   * import akka.pattern.pipe
   * // requires implicit ExecutionContext, e.g. by importing `context.dispatcher` inside an Actor
   *
   * Future { doExpensiveCalc() } pipeTo nextActor
   *
   * or
   *
   * pipe(someFuture) to nextActor
   *
   * }}}
   *
   * The successful result of the future is sent as a message to the recipient, or
   * the failure is sent in a [[akka.actor.Status.Failure]] to the recipient.
   */
  implicit def pipe[T](future: Future[T])(implicit executionContext: ExecutionContext): PipeableFuture[T] =
    new PipeableFuture(future)

  /**
   * Import this implicit conversion to gain the `pipeTo` method on [[scala.concurrent.Future]]:
   *
   * {{{
   * import akka.pattern.pipe
   * // requires implicit ExecutionContext, e.g. by importing `context.dispatcher` inside an Actor
   *
   * Future { doExpensiveCalc() } pipeTo nextActor
   *
   * or
   *
   * pipe(someFuture) to nextActor
   *
   * }}}
   *
   * The successful result of the future is sent as a message to the recipient, or
   * the failure is sent in a [[akka.actor.Status.Failure]] to the recipient.
   */
  implicit def pipeCompletionStage[T](future: CompletionStage[T])(
      implicit executionContext: ExecutionContext): PipeableCompletionStage[T] = new PipeableCompletionStage(future)
}
