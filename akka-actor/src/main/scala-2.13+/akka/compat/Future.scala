/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.compat

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future => SFuture }

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * Compatibility wrapper for `scala.concurrent.Future` to be able to compile the same code
 * against Scala 2.12, 2.13
 *
 * Remove these classes as soon as support for Scala 2.12 is dropped!
 */
@InternalApi private[akka] object Future {
  def fold[T, R](futures: IterableOnce[SFuture[T]])(zero: R)(op: (R, T) => R)(
      implicit executor: ExecutionContext): SFuture[R] = {
    // This will have performance implications since the elements are copied to a Vector
    SFuture.foldLeft[T, R](futures.iterator.to(immutable.Iterable))(zero)(op)(executor)
  }

  def fold[T, R](futures: immutable.Iterable[SFuture[T]])(zero: R)(op: (R, T) => R)(
      implicit executor: ExecutionContext): SFuture[R] =
    SFuture.foldLeft[T, R](futures)(zero)(op)(executor)

  def reduce[T, R >: T](futures: IterableOnce[SFuture[T]])(op: (R, T) => R)(
      implicit executor: ExecutionContext): SFuture[R] = {
    // This will have performance implications since the elements are copied to a Vector
    SFuture.reduceLeft[T, R](futures.iterator.to(immutable.Iterable))(op)(executor)
  }

  def reduce[T, R >: T](futures: immutable.Iterable[SFuture[T]])(op: (R, T) => R)(
      implicit executor: ExecutionContext): SFuture[R] =
    SFuture.reduceLeft[T, R](futures)(op)(executor)

  def find[T](futures: IterableOnce[SFuture[T]])(p: T => Boolean)(
      implicit executor: ExecutionContext): SFuture[Option[T]] = {
    // This will have performance implications since the elements are copied to a Vector
    SFuture.find[T](futures.iterator.to(immutable.Iterable))(p)(executor)
  }

  def find[T](futures: immutable.Iterable[SFuture[T]])(p: T => Boolean)(
      implicit executor: ExecutionContext): SFuture[Option[T]] =
    SFuture.find[T](futures)(p)(executor)
}
