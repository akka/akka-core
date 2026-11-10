/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import akka.AkkaException

/**
 * Thrown if replay inactivity exceeds a specified timeout.
 */
@SerialVersionUID(1L)
class AsyncReplayTimeoutException(msg: String) extends AkkaException(msg)
