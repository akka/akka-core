/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import akka.annotation.InternalApi
import akka.persistence._
import akka.persistence.testkit.EventStorage

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] class SimpleEventStorageImpl extends EventStorage {

  override type InternalRepr = PersistentRepr

  override def toInternal(repr: PersistentRepr): PersistentRepr = repr

  override def toRepr(internal: PersistentRepr): PersistentRepr = internal

}
