/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

/**
 * (Optional) mechanism for query implementations to pick up a correlation id from the caller, to use in logging and
 * error messages. Used by akka-projections to make correlating projection logs with debug and trace logging from the
 * underlying akka persistence query implementations possible.
 */
object QueryCorrelationId {

  private val threadLocal = new ThreadLocal[String]

  /**
   * Expected to be used "around" calls to plugin query method, will clear the correlation id from thread local
   * to make sure there is no leak between logic executed on shared threads.
   */
  def withCorrelationId[T](correlationId: String)(block: () => T): T = {
    threadLocal.set(correlationId)
    try {
      block()
    } finally {
      threadLocal.remove()
    }
  }

  /**
   * Expected to be used "around" calls to plugin query method to pass along a prevously extracted optional correlation id,
   * will clear the correlation id from thread local to make sure there is no leak between logic executed on shared threads.
   */
  def withCorrelationId[T](correlationId: Option[String])(block: () => T): T = {
    correlationId match {
      case None           => block()
      case Some(actualId) => withCorrelationId(actualId)(block)
    }
  }

  /**
   * @return Expected to be called directly after receiving a query call, before starting any asynchronous tasks,
   *         returns and clears out the correlation id to make sure there is no leak between tasks. Further passing
   *         around of the uuid inside the query plugin implementation is up to the implementer.
   */
  def get(): Option[String] =
    Option(threadLocal.get)

}
