/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.annotation.DoNotInherit
import akka.event.LoggingAdapter

/**
 * Not for user extension
 */
@DoNotInherit
trait MaterializerLoggingProvider { this: Materializer =>

  def makeLogger(logSource: Class[Any]): LoggingAdapter

}
