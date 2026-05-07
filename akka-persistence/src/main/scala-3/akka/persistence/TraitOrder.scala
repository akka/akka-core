/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[persistence] object TraitOrder {
  val canBeChecked = false

  // No-op on Scala 3
  def checkBefore(clazz: Class[_], one: Class[_], other: Class[_]): Unit =
    ()
}
