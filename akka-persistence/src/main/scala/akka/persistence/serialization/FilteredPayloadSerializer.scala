/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API
 */
@InternalApi
final class FilteredPayloadSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val FilteredPayloadManifest = "F"

  override def manifest(o: AnyRef): String = o match {
    case FilteredPayload => FilteredPayloadManifest
    case _               => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case FilteredPayload => Array.empty
    case _               => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = FilteredPayload

}
