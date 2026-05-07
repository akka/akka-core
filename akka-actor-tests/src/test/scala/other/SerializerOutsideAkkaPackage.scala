/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package other

import java.nio.charset.StandardCharsets

import akka.serialization.SerializerWithStringManifest

class SerializerOutsideAkkaPackage extends SerializerWithStringManifest {
  override def identifier: Int = 999

  override def manifest(o: AnyRef): String = "A"

  override def toBinary(o: AnyRef): Array[Byte] =
    o.toString.getBytes(StandardCharsets.UTF_8)

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    new String(bytes, StandardCharsets.UTF_8)
}
