/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package akka.coordination.lease

class LeaseException(message: String) extends RuntimeException(message)

final class LeaseTimeoutException(message: String) extends LeaseException(message)
