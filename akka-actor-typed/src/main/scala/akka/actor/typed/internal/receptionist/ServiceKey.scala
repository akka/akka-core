/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.receptionist

import akka.actor.typed.receptionist.ServiceKey
import akka.annotation.InternalApi

/**
 * Internal representation of [[ServiceKey]] which is needed
 * in order to use a TypedMultiMap (using keys with a type parameter does not
 * work in Scala 2.x).
 *
 * Internal API
 */
@InternalApi
private[akka] abstract class AbstractServiceKey {
  type Protocol

  /** Type-safe down-cast */
  def asServiceKey: ServiceKey[Protocol]
}

/**
 * This is the only actual concrete service key type
 *
 * Internal API
 */
@InternalApi
final case class DefaultServiceKey[T](id: String, typeName: String) extends ServiceKey[T] {
  override def toString: String = s"ServiceKey[$typeName]($id)"
}
