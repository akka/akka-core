/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal

import java.io.NotSerializableException

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.typed.internal.protobuf.ClusterMessages
import akka.cluster.typed.internal.receptionist.ClusterReceptionist.Entry
import akka.remote.serialization.WrappedPayloadSupport
import akka.serialization.{ BaseSerializer, SerializerWithStringManifest }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class AkkaClusterTypedSerializer(override val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  // Serializers are initialized early on. `toTyped` might then try to initialize the classic ActorSystemAdapter extension.
  private lazy val resolver = ActorRefResolver(system.toTyped)
  private val payloadSupport = new WrappedPayloadSupport(system)

  private val ReceptionistEntryManifest = "a"
  private val PubSubPublishManifest = "b"

  override def manifest(o: AnyRef): String = o match {
    case _: Entry                         => ReceptionistEntryManifest
    case _: TopicImpl.MessagePublished[_] => PubSubPublishManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case e: Entry                         => receptionistEntryToBinary(e)
    case m: TopicImpl.MessagePublished[_] => pubSubPublishToBinary(m)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case ReceptionistEntryManifest => receptionistEntryFromBinary(bytes)
    case PubSubPublishManifest     => pubSubMessageFromBinary(bytes)
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

  private def pubSubPublishToBinary(m: TopicImpl.MessagePublished[_]): Array[Byte] = {
    ClusterMessages.PubSubMessagePublished
      .newBuilder()
      .setMessage(payloadSupport.payloadBuilder(m.message))
      .build()
      .toByteArray
  }

  private def receptionistEntryToBinary(e: Entry): Array[Byte] = {
    val b = ClusterMessages.ReceptionistEntry
      .newBuilder()
      .setActorRef(resolver.toSerializationFormat(e.ref))
      .setSystemUid(e.systemUid)

    if (e.createdTimestamp != 0L)
      b.setCreatedTimestamp(e.createdTimestamp)

    b.build().toByteArray
  }

  private def pubSubMessageFromBinary(bytes: Array[Byte]): TopicImpl.MessagePublished[_] = {
    val parsed = ClusterMessages.PubSubMessagePublished.parseFrom(bytes)
    val userMessage = payloadSupport.deserializePayload(parsed.getMessage)
    TopicImpl.MessagePublished(userMessage)
  }

  private def receptionistEntryFromBinary(bytes: Array[Byte]): Entry = {
    val re = ClusterMessages.ReceptionistEntry.parseFrom(bytes)
    val createdTimestamp = if (re.hasCreatedTimestamp) re.getCreatedTimestamp else 0L
    Entry(resolver.resolveActorRef(re.getActorRef), re.getSystemUid)(createdTimestamp)
  }

}
