/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

/**
 * Akka base Exception.
 */
@SerialVersionUID(1L)
class AkkaException(message: String, cause: Throwable) extends RuntimeException(message, cause) with Serializable {
  def this(msg: String) = this(msg, null)
}

/**
 * Mix in this trait to suppress the StackTrace for the instance of the exception but not the cause,
 * scala.util.control.NoStackTrace suppresses all the StackTraces.
 */
trait OnlyCauseStackTrace { self: Throwable =>
  override def fillInStackTrace(): Throwable = {
    setStackTrace(getCause match {
      case null => Array.empty
      case some => some.getStackTrace
    })
    this
  }
}

/**
 * This exception is thrown when Akka detects a problem with the provided configuration
 */
class ConfigurationException(message: String, cause: Throwable) extends AkkaException(message, cause) {
  def this(msg: String) = this(msg, null)
}
