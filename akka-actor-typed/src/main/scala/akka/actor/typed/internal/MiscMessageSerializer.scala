/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets

import akka.actor.typed.{ ActorRef, ActorRefResolver }
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.serialization.{ BaseSerializer, SerializerWithStringManifest }

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class MiscMessageSerializer(val system: akka.actor.ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  // Serializers are initialized early on. `toTyped` might then try to initialize the classic ActorSystemAdapter extension.
  private lazy val resolver = ActorRefResolver(system.toTyped)
  private val ActorRefManifest = "a"

  def manifest(o: AnyRef): String = o match {
    case _: ActorRef[_] => ActorRefManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case ref: ActorRef[_] => resolver.toSerializationFormat(ref).getBytes(StandardCharsets.UTF_8)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  def fromBinary(bytes: Array[Byte], manifest: String): ActorRef[Any] = manifest match {
    case ActorRefManifest => resolver.resolveActorRef(new String(bytes, StandardCharsets.UTF_8))
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

}
