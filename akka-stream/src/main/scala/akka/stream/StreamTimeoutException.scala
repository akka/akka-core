/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.concurrent.TimeoutException
import scala.util.control.NoStackTrace

import akka.annotation.DoNotInherit

/**
 * Base class for timeout exceptions specific to Akka Streams
 *
 * Not for user extension
 */
@DoNotInherit
sealed class StreamTimeoutException(msg: String) extends TimeoutException(msg) with NoStackTrace

final class InitialTimeoutException(msg: String) extends StreamTimeoutException(msg)

final class CompletionTimeoutException(msg: String) extends StreamTimeoutException(msg)

final class StreamIdleTimeoutException(msg: String) extends StreamTimeoutException(msg)

final class BackpressureTimeoutException(msg: String) extends StreamTimeoutException(msg)
