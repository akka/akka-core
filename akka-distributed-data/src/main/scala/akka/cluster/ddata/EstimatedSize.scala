/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.annotation.InternalApi

/**
 * INTERNAL API: Rough estimate in bytes of some serialized data elements. Used
 * when creating gossip messages.
 */
@InternalApi private[akka] object EstimatedSize {
  val LongValue = 8
  val Address = 50
  val UniqueAddress = Address + LongValue
}
