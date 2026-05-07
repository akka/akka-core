/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{ Failure => ScalaFailure }
import scala.util.{ Success => ScalaSuccess }
import scala.util.Try
import scala.util.control.NoStackTrace

import akka.Done
import akka.actor.InvalidMessageException
import akka.annotation.InternalApi
import akka.pattern.StatusReply.ErrorMessage

/**
 * Generic top-level message type for replies that signal failure or success. Convenient to use together with the
 * `askWithStatus` ask variants.
 *
 * Create using the factory methods [[StatusReply#success]] and [[StatusReply#error]].
 *
 * Akka contains predefined serializers for the wrapper type and the textual error messages.
 *
 * @tparam T the type of value a successful reply would have
 */
final class StatusReply[+T] private (private val status: Try[T]) {
  if (status == null)
    throw InvalidMessageException("[null] is not an allowed status")

  /**
   * Java API: in the case of a successful reply returns the value, if the reply was not successful the exception
   * the failure was created with is thrown
   */
  def getValue: T = status.get

  /**
   * Java API: returns the exception if the reply is a failure, or throws an exception if not.
   */
  def getError: Throwable = status match {
    case ScalaFailure(ex) => ex
    case _                => throw new IllegalArgumentException("Expected reply to be a failure, but was a success")
  }

  def isError: Boolean = status.isFailure
  def isSuccess: Boolean = status.isSuccess

  override def equals(other: Any): Boolean = other match {
    case that: StatusReply[_] => status == that.status
    case _                    => false
  }

  override def hashCode(): Int = status.hashCode

  override def toString: String = status match {
    case ScalaSuccess(t)                => s"Success($t)"
    case ScalaFailure(ex: ErrorMessage) => s"Error(${ex.getMessage})"
    case ScalaFailure(ex)               => s"Error($ex)"
  }

}

object StatusReply {

  /**
   * Scala API: A general purpose message for using as an Ack
   */
  val Ack: StatusReply[Done] = success(Done)

  /**
   * Java API: A general purpose message for using as an Ack
   */
  def ack(): StatusReply[Done] = Ack

  /**
   * Java API: Create a successful reply containing `value`
   */
  def success[T](value: T): StatusReply[T] = new StatusReply(ScalaSuccess(value))

  /**
   * Java API: Create an status response with a error message describing why the request was failed or denied.
   */
  def error[T](errorMessage: String): StatusReply[T] = Error(errorMessage)

  /**
   * Java API: Create an error response with a user defined [[Throwable]].
   *
   * Prefer the string based error response over this one when possible to avoid tightly coupled logic across
   * actors and passing internal failure details on to callers that can not do much to handle them.
   *
   * For cases where types are needed to identify errors and behave differently enumerating them with a specific
   * set of response messages may be a better alternative to encoding them as generic exceptions.
   *
   * Also note that Akka does not contain pre-build serializers for arbitrary exceptions.
   */
  def error[T](exception: Throwable): StatusReply[T] = Error(exception)

  /**
   * Scala API: Turn a Try into a status reply.
   *
   * Prefer the string based error response over this one when possible to avoid tightly coupled logic across
   * actors and passing internal failure details on to callers that can not do much to handle them. [[#fromTry]]
   * provides a convenience factory doing that for [[scala.util.Try]].
   *
   * For cases where types are needed to identify errors and behave differently enumerating them with a specific
   * set of response messages may be a better alternative to encoding them as generic exceptions.
   *
   * Also note that Akka does not contain pre-built serializers for arbitrary exceptions.
   */
  def fromTryKeepException[T](status: Try[T]): StatusReply[T] = new StatusReply(status)

  /**
   * Scala API: Turn a try into a status reply.
   *
   * Transforms exceptions into status reply errors containing just the exception message string.
   *
   * See [[#fromTryKeepException]] for passing the exception along as is.
   */
  def fromTry[T](status: Try[T]): StatusReply[T] = status match {
    case scala.util.Success(value) => success(value)
    case scala.util.Failure(t)     => error[T](t.getMessage)
  }

  /**
   * Carrier exception used for textual error descriptions.
   *
   * Not meant for usage outside of [[StatusReply]].
   */
  final case class ErrorMessage(private val errorMessage: String)
      extends RuntimeException(errorMessage)
      with NoStackTrace {
    override def toString: String = errorMessage
  }

  /**
   * Scala API for creation and pattern matching a successful response.
   *
   * For example:
   * ```
   *   case StatusReply.Success(value: String) => ...
   * ```
   */
  object Success {

    /**
     * Scala API: Create a successful reply containing `value`
     */
    def apply[T](value: T): StatusReply[T] = new StatusReply(ScalaSuccess(value))
    def unapply(status: StatusReply[Any]): Option[Any] =
      if (status != null && status.isSuccess) Some(status.getValue)
      else None
  }

  /**
   * Scala API for creating and pattern matching an error response
   *
   * For example:
   * ```
   *   case StatusReply.Error(exception) => ...
   * ```
   */
  object Error {

    /**
     * Scala API: Create an status response with a error message describing why the request was failed or denied.
     */
    def apply[T](errorMessage: String): StatusReply[T] = error(new ErrorMessage(errorMessage))

    /**
     * Scala API: Create an error response with a user defined [[Throwable]].
     *
     * Prefer the string based error response over this one when possible to avoid tightly coupled logic across
     * actors and passing internal failure details on to callers that can not do much to handle them.
     *
     * For cases where types are needed to identify errors and behave differently enumerating them with a specific
     * set of response messages may be a better alternative to encoding them as generic exceptions.
     *
     * Also note that Akka does not contain pre-build serializers for arbitrary exceptions.
     */
    def apply[T](exception: Throwable): StatusReply[T] = new StatusReply(ScalaFailure(exception))
    def unapply(status: StatusReply[_]): Option[Throwable] =
      if (status != null && status.isError) Some(status.getError)
      else None
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def flattenStatusFuture[T](f: Future[StatusReply[T]]): Future[T] =
    f.transform {
      case ScalaSuccess(s) =>
        s match {
          case StatusReply.Success(v) => ScalaSuccess(v.asInstanceOf[T])
          case StatusReply.Error(ex)  => ScalaFailure[T](ex)
          case unexpected =>
            ScalaFailure(new IllegalArgumentException(s"Unexpected status reply success value: ${unexpected}"))
        }
      case fail @ ScalaFailure(_) => fail.asInstanceOf[Try[T]]
    }(ExecutionContext.parasitic)
}
