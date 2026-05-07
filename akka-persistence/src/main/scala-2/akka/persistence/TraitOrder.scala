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
  val canBeChecked = true

  def checkBefore(clazz: Class[_], one: Class[_], other: Class[_]): Unit = {
    val interfaces = clazz.getInterfaces
    val i = interfaces.indexOf(other)
    val j = interfaces.indexOf(one)
    if (i != -1 && j != -1 && i < j)
      throw new IllegalStateException(
        s"For ${clazz.getName}, use ${one.getName} with ${other.getName}, instead of ${other.getName} with ${one.getName}")
  }
}
