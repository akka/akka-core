/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import java.util.Optional
import scala.jdk.OptionConverters.RichOption
import scala.jdk.OptionConverters.RichOptional

/**
 * (Optional) mechanism for query implementations to pick up a correlation id from the caller, to use in logging and
 * error messages. Used by akka-projections to make correlating projection logs with debug and trace logging from the
 * underlying akka persistence query implementations possible.
 */
object QueryCorrelationId {

  private val threadLocal = new ThreadLocal[String]

  /**
   * Scala API: Expected to be used "around" calls to plugin query method, will clear the correlation id from thread local
   * to make sure there is no leak between logic executed on shared threads.
   */
  def withCorrelationId[T](correlationId: String)(f: () => T): T = {
    threadLocal.set(correlationId)
    try {
      f()
    } finally {
      threadLocal.remove()
    }
  }

  /**
   * Scala API: Expected to be used "around" calls to plugin query method to pass along a previously extracted optional correlation id,
   * will clear the correlation id from thread local to make sure there is no leak between logic executed on shared threads.
   */
  def withCorrelationId[T](correlationId: Option[String])(f: () => T): T = {
    correlationId match {
      case None           => f()
      case Some(actualId) => withCorrelationId(actualId)(f)
    }
  }

  /**
   * Scala API: Expected to be called by query plugins directly after receiving a query call, before starting any asynchronous tasks.
   * Calling code is responsible to clear it out after method returns. The value is stored in a thread local so is not available
   * across threads or streams. Further passing around of the uuid inside the query plugin implementation is up to the implementer.
   */
  def get(): Option[String] =
    Option(threadLocal.get)

  /**
   * Java API: Expected to be used "around" calls to plugin query method to pass along a previously extracted optional correlation id,
   * will clear the correlation id from thread local to make sure there is no leak between logic executed on shared threads.
   */
  def callWithCorrelationId[T](correlationId: Optional[String], function: java.util.function.Supplier[T]): T =
    withCorrelationId(correlationId.toScala)(function.get _)

  /**
   * Java API: Expected to be used "around" calls to plugin query method, will clear the correlation id from thread local
   * to make sure there is no leak between logic executed on shared threads.
   */
  def callWithCorrelationId[T](correlationId: String, function: java.util.function.Supplier[T]): T =
    withCorrelationId(correlationId)(function.get _)

  /**
   * Java API: Expected to be called by query plugins directly after receiving a query call, before starting any asynchronous tasks.
   * Calling code is responsible to clear it out after method returns. The value is stored in a thread local so is not available
   * across threads or streams. Further passing around of the uuid inside the query plugin implementation is up to the implementer.
   */
  def getCorrelationId(): Optional[String] =
    get().toJava

}
