/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

/**
 * Some journal implementations may support events of this type by writing the event payload and
 * serialization information without having to serialize it.
 */
final class SerializedEvent(val bytes: Array[Byte], val serializerId: Int, val serializerManifest: String)
