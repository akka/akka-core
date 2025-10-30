/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import akka.persistence.CompositeMetadata
import akka.persistence.FilteredPayload
import akka.persistence.SerializedEvent
import akka.persistence.serialization.{ MessageFormats => mf }
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers
import akka.testkit.AkkaSpec
import akka.protobufv3.internal.{ ByteString => ProtoByteString }

class PayloadSerializerSpec extends AkkaSpec {

  private val serialization = SerializationExtension(system)

  "PayloadSerializer" should {
    "serialize FilteredPayload to zero-byte array" in {
      val serializer = serialization.findSerializerFor(FilteredPayload).asInstanceOf[SerializerWithStringManifest]
      val manifest = serializer.manifest(FilteredPayload)
      val serialized = serializer.toBinary(FilteredPayload)
      serialized should have(size(0))
      serializer.fromBinary(serialized, manifest) should be(FilteredPayload)
    }

    "serialize SerializedEvent" in {
      val event = "event1"
      val eventSerializer = serialization.findSerializerFor(event)
      val serializedEvent = new SerializedEvent(
        bytes = eventSerializer.toBinary(event),
        serializerId = eventSerializer.identifier,
        serializerManifest = Serializers.manifestFor(eventSerializer, event))

      val serializedEventSerializer = serialization.findSerializerFor(serializedEvent)
      val serializedEventBytes = serialization.serialize(serializedEvent).get

      val deserializied = serialization
        .deserialize(
          serializedEventBytes,
          serializedEventSerializer.identifier,
          Serializers.manifestFor(serializedEventSerializer, serializedEvent))
        .get
        .asInstanceOf[SerializedEvent]

      val deserializiedEvent =
        serialization.deserialize(deserializied.bytes, deserializied.serializerId, deserializied.serializerManifest).get
      deserializiedEvent shouldBe event
    }

    "serialize CompositeMetadata" in {
      val meta = CompositeMetadata(List("a", 17L))
      val serializer = serialization.findSerializerFor(meta).asInstanceOf[SerializerWithStringManifest]
      serializer.getClass shouldBe classOf[PayloadSerializer]
      val bytes = serializer.toBinary(meta)
      val manifest = serializer.manifest(meta)
      val serializerId = serializer.identifier

      val deserialized = serialization.deserialize(bytes, serializerId, manifest).get.asInstanceOf[CompositeMetadata]
      deserialized.entries.size shouldBe 2
      deserialized.entries.head shouldBe "a"
      deserialized.entries(1) shouldBe 17L
    }

    "ignore some metadata unknown when deserializing rather than fail" in {
      val stringSerializer = serialization.serializerFor(classOf[String])
      val stringMeta = "suchMetaWow!"
      val metadataWithUnknownEntry = mf.CompositeMetadata
        .newBuilder()
        .addPayloads(
          mf.PersistentPayload
            .newBuilder()
            .setSerializerId(Int.MinValue)
            .setPayloadManifest(ProtoByteString.copyFromUtf8("Q"))
            .setPayload(ProtoByteString.empty())
            .build())
        .addPayloads(
          mf.PersistentPayload
            .newBuilder()
            .setSerializerId(stringSerializer.identifier)
            .setPayloadManifest(ProtoByteString.empty())
            .setPayload(ProtoByteString.copyFrom(stringSerializer.toBinary(stringMeta))))
        .build()
      val bytes = metadataWithUnknownEntry.toByteArray

      val someMeta = CompositeMetadata(List("dummy"))
      val serializer = serialization.findSerializerFor(someMeta).asInstanceOf[SerializerWithStringManifest]
      val manifest = serializer.manifest(someMeta)
      val deserialized =
        serialization.deserialize(bytes, serializer.identifier, manifest).get.asInstanceOf[CompositeMetadata]

      deserialized.entries should have size (1)
      deserialized.entries shouldEqual Seq(stringMeta)
    }

    "ignore all metadata unknown when deserializing rather than fail" in {
      val metadataWithUnknownEntry = mf.CompositeMetadata
        .newBuilder()
        .addPayloads(
          mf.PersistentPayload
            .newBuilder()
            .setSerializerId(Int.MinValue)
            .setPayloadManifest(ProtoByteString.copyFromUtf8("Q"))
            .setPayload(ProtoByteString.empty())
            .build())
        .build()
      val bytes = metadataWithUnknownEntry.toByteArray

      val someMeta = CompositeMetadata(List("dummy"))
      val serializer = serialization.findSerializerFor(someMeta).asInstanceOf[SerializerWithStringManifest]
      val manifest = serializer.manifest(someMeta)
      val deserialized =
        serialization.deserialize(bytes, serializer.identifier, manifest).get.asInstanceOf[CompositeMetadata]

      deserialized shouldBe theSameInstanceAs(PayloadSerializer.NoDeserializableMetadataComposite)
    }
  }

}
