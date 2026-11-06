/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

/**
 * Holds a result of an IO operation.
 *
 * @param count Numeric value depending on context, for example IO operations performed or bytes processed.
 */
final case class IOResult(count: Long) {

  def withCount(value: Long): IOResult = copy(count = value)

  /**
   * Java API: Numeric value depending on context, for example IO operations performed or bytes processed.
   */
  def getCount: Long = count

}

object IOResult {

  /** JAVA API: Creates successful IOResult */
  def createSuccessful(count: Long): IOResult =
    new IOResult(count)
}

/**
 * This exception signals that a stream has been completed or has an error while
 * there was still IO operations in progress
 *
 * @param count The number of bytes read/written up until the error
 * @param cause cause
 */
final class IOOperationIncompleteException(message: String, val count: Long, cause: Throwable)
    extends RuntimeException(message, cause) {

  def this(count: Long, cause: Throwable) =
    this(s"IO operation was stopped unexpectedly after $count bytes because of $cause", count, cause)

}
