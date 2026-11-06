/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.util.control.NoStackTrace

class StreamTcpException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) with NoStackTrace {
  def this(msg: String) = this(msg, null)
}

class BindFailedException(msg: String, cause: Throwable) extends StreamTcpException(msg, cause) {
  def this() = this("bind failed", null)
}

class ConnectionException(msg: String) extends StreamTcpException(msg)
