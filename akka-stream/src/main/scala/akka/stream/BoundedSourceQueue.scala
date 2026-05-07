/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.annotation.DoNotInherit

/**
 * A queue of the given size that gives immediate feedback whether an element could be enqueued or not.
 *
 * Not for user extension
 */
@DoNotInherit
trait BoundedSourceQueue[T] {

  /**
   * Returns a [[akka.stream.QueueOfferResult]] that notifies the caller if the element could be enqueued or not, or
   * the completion status of the queue.
   *
   * A result of `QueueOfferResult.Enqueued` does not guarantee that an element also has been or will be processed by
   * the downstream.
   */
  def offer(elem: T): QueueOfferResult

  /**
   * Completes the stream normally.
   */
  def complete(): Unit

  /**
   * Completes the stream with a failure.
   */
  def fail(ex: Throwable): Unit

  /**
   * Returns the approximate number of elements in this queue.
   */
  def size(): Int
}
