/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.nowarn
import scala.util.{ Failure, Success, Try }
import scala.util.control.NoStackTrace

import akka.Done

/**
 * Holds a result of an IO operation.
 *
 * @param count Numeric value depending on context, for example IO operations performed or bytes processed.
 * @param status Status of the result. Can be either [[akka.Done]] or an exception.
 */
@nowarn("msg=deprecated") // Status
final case class IOResult(
    count: Long,
    @deprecated("status is always set to Success(Done)", "2.6.0") status: Try[Done]) {

  def withCount(value: Long): IOResult = copy(count = value)

  @deprecated("status is always set to Success(Done)", "2.6.0")
  def withStatus(value: Try[Done]): IOResult = copy(status = value)

  /**
   * Java API: Numeric value depending on context, for example IO operations performed or bytes processed.
   */
  def getCount: Long = count

  /**
   * Java API: Indicates whether IO operation completed successfully or not.
   */
  @deprecated("status is always set to Success(Done)", "2.6.0")
  def wasSuccessful: Boolean = status.isSuccess

  /**
   * Java API: If the IO operation resulted in an error, returns the corresponding [[Throwable]]
   * or throws [[UnsupportedOperationException]] otherwise.
   */
  @deprecated("status is always set to Success(Done)", "2.6.0")
  def getError: Throwable = status match {
    case Failure(t) => t
    case Success(_) => throw new UnsupportedOperationException("IO operation was successful.")
  }

}

object IOResult {

  def apply(count: Long): IOResult = IOResult(count, Success(Done))

  /** JAVA API: Creates successful IOResult */
  def createSuccessful(count: Long): IOResult =
    new IOResult(count, Success(Done))

  /** JAVA API: Creates failed IOResult, `count` should be the number of bytes (or other unit, please document in your APIs) processed before failing */
  @deprecated("use IOOperationIncompleteException", "2.6.0")
  def createFailed(count: Long, ex: Throwable): IOResult =
    new IOResult(count, Failure(ex))
}

/**
 * This exception signals that a stream has been completed by an onError signal
 * while there was still IO operations in progress.
 */
@deprecated("use IOOperationIncompleteException", "2.6.0")
final case class AbruptIOTerminationException(ioResult: IOResult, cause: Throwable)
    extends RuntimeException("Stream terminated without completing IO operation.", cause)
    with NoStackTrace

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
