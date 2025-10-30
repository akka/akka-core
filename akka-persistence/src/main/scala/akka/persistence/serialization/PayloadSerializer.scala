/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.persistence.CompositeMetadata
import akka.persistence.FilteredPayload
import akka.persistence.SerializedEvent
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest
import akka.persistence.serialization.{ MessageFormats => mf }
import akka.protobufv3.internal.ByteString
import akka.protobufv3.internal.UnsafeByteOperations
import akka.serialization.SerializationExtension
import akka.serialization.Serializers

/**
 * INTERNAL API
 */
@InternalApi
object PayloadSerializer {
  case object NoDeserializableMetadataMarker
  val NoDeserializableMetadataComposite = CompositeMetadata(Seq(NoDeserializableMetadataMarker))
}

/**
 * INTERNAL API
 */
@InternalApi
final class PayloadSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val FilteredPayloadManifest = "F"
  private val SerializedEventManifest = "S"
  private val MetadataManifest = "M"

  private lazy val serialization = SerializationExtension(system)

  override def manifest(o: AnyRef): String = o match {
    case FilteredPayload      => FilteredPayloadManifest
    case _: SerializedEvent   => SerializedEventManifest
    case _: CompositeMetadata => MetadataManifest
    case _                    => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case FilteredPayload      => Array.empty
    case se: SerializedEvent  => payloadBuilder(se).build.toByteArray
    case m: CompositeMetadata => metadataToBinary(m)
    case _                    => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  private def payloadBuilder(se: SerializedEvent): mf.PersistentPayload.Builder = {
    val builder = mf.PersistentPayload.newBuilder()
    builder.setPayload(UnsafeByteOperations.unsafeWrap(se.bytes))
    builder.setSerializerId(se.serializerId)
    if (se.serializerManifest.nonEmpty) builder.setPayloadManifest(ByteString.copyFromUtf8(se.serializerManifest))
    builder
  }

  private def metadataToBinary(m: CompositeMetadata): Array[Byte] = {
    def metadataBuilder(metadata: AnyRef): mf.PersistentPayload.Builder = {
      val serializer = serialization.findSerializerFor(metadata)
      val builder = mf.PersistentPayload.newBuilder()

      val ms = Serializers.manifestFor(serializer, metadata)
      if (ms.nonEmpty) builder.setPayloadManifest(ByteString.copyFromUtf8(ms))

      builder.setPayload(UnsafeByteOperations.unsafeWrap(serializer.toBinary(metadata)))
      builder.setSerializerId(serializer.identifier)
      builder
    }

    val metadataBuilders = m.entries.map(m => metadataBuilder(m.asInstanceOf[AnyRef]))
    val builder = mf.CompositeMetadata.newBuilder()
    metadataBuilders.foreach(builder.addPayloads)

    builder.build().toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case FilteredPayloadManifest => FilteredPayload
    case SerializedEventManifest => serializedEvent(mf.PersistentPayload.parseFrom(bytes))
    case MetadataManifest        => metadataFromBinary(bytes)
    case _                       => throw new IllegalArgumentException(s"Unknown manifest [$manifest]")
  }

  private def serializedEvent(persistentPayload: mf.PersistentPayload): SerializedEvent = {
    val manifest =
      if (persistentPayload.hasPayloadManifest)
        persistentPayload.getPayloadManifest.toStringUtf8
      else ""

    new SerializedEvent(persistentPayload.getPayload.toByteArray, persistentPayload.getSerializerId, manifest)
  }

  private def metadataFromBinary(bytes: Array[Byte]): CompositeMetadata = {
    import scala.jdk.CollectionConverters._
    val protoCompositeMetadata = mf.CompositeMetadata.parseFrom(bytes)

    val metadataEntries =
      protoCompositeMetadata.getPayloadsList.iterator.asScala.flatMap { persistentPayload =>
        val manifest =
          if (persistentPayload.hasPayloadManifest)
            persistentPayload.getPayloadManifest.toStringUtf8
          else ""

        // if the metadata type is unknown to the deserializing service, we do not want to fail deserialization
        // of the event as a whole, so such metadata payloads are ignored
        serialization
          .deserialize(persistentPayload.getPayload.toByteArray, persistentPayload.getSerializerId, manifest)
          .toOption
      }.toSeq

    if (metadataEntries.nonEmpty) CompositeMetadata(metadataEntries)
    else
      PayloadSerializer.NoDeserializableMetadataComposite // we always need to return a CompositeMetadata with at least one entry
  }

}
