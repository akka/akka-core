/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.typed

import java.nio.charset.StandardCharsets

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import docs.akka.cluster.typed.PingPongExample.PingService

//#serializer
class PingSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val actorRefResolver = ActorRefResolver(system.toTyped)

  private val PingManifest = "a"
  private val PongManifest = "b"

  override def identifier = 41

  override def manifest(msg: AnyRef) = msg match {
    case _: PingService.Ping => PingManifest
    case PingService.Pong    => PongManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${msg.getClass} in [${getClass.getName}]")
  }

  override def toBinary(msg: AnyRef) = msg match {
    case PingService.Ping(who) =>
      actorRefResolver.toSerializationFormat(who).getBytes(StandardCharsets.UTF_8)
    case PingService.Pong =>
      Array.emptyByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${msg.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String) = {
    manifest match {
      case PingManifest =>
        val str = new String(bytes, StandardCharsets.UTF_8)
        val ref = actorRefResolver.resolveActorRef[PingService.Pong.type](str)
        PingService.Ping(ref)
      case PongManifest =>
        PingService.Pong
      case _ =>
        throw new IllegalArgumentException(s"Unknown manifest [$manifest]")
    }
  }
}
//#serializer
