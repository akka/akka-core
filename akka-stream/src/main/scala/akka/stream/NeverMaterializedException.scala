/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

final class NeverMaterializedException(cause: Throwable)
    extends RuntimeException("Downstream canceled without triggering lazy source materialization", cause) {

  def this() = this(null)

}
