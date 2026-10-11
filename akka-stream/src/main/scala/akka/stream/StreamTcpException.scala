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

@deprecated("BindFailedException object will never be thrown. Match on the class instead.", "2.4.19")
case object BindFailedException extends BindFailedException

class ConnectionException(msg: String) extends StreamTcpException(msg)
