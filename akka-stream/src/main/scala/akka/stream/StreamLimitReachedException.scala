/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package akka.stream

class StreamLimitReachedException(val n: Long) extends RuntimeException(s"limit of $n reached")
