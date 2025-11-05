/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.util.control.NoStackTrace

/**
 * This exception signals that materialized value is already detached from stream. This usually happens
 * when stream is completed and an ActorSystem is shut down while materialized object is still available.
 */
final class StreamDetachedException(message: String) extends RuntimeException(message) with NoStackTrace {

  def this() = this("Stream is terminated. Materialized value is detached.")
}
