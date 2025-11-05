/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.util.control.NoStackTrace

/**
 * This exception signals that the maximum number of substreams declared has been exceeded.
 * A finite limit is imposed so that memory usage is controlled.
 */
final class TooManySubstreamsOpenException
    extends IllegalStateException("Cannot open a new substream as there are too many substreams open")
    with NoStackTrace {}
