/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.protobufv3.internal.ByteString
import akka.remote.ByteStringUtils
import akka.remote.ContainerFormats
import akka.serialization.{ SerializationExtension, Serializers }
import akka.serialization.ByteBufferSerializer
import akka.serialization.DisabledJavaSerializer
import akka.serialization.Serialization
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API
 */
private[akka] class WrappedPayloadSupport(system: ExtendedActorSystem) {

  private lazy val serialization = SerializationExtension(system)
  private val log = Logging(system, classOf[WrappedPayloadSupport])

  /**
   * Serialize the `input` along with its `manifest` and `serializerId`.
   *
   * If `input` is a `Throwable` and can't be serialized because Java serialization is disabled it
   * will fallback to `ThrowableNotSerializableException`.
   */
  def payloadBuilder(input: Any): ContainerFormats.Payload.Builder =
    WrappedPayloadSupport.payloadBuilder(input, serialization, log)

  def deserializePayload(payload: ContainerFormats.Payload): Any =
    WrappedPayloadSupport.deserializePayload(payload, serialization)
}

private[akka] object WrappedPayloadSupport {

  /**
   * Serialize the `input` along with its `manifest` and `serializerId`.
   *
   * If `input` is a `Throwable` and can't be serialized because Java serialization is disabled it
   * will fallback to `ThrowableNotSerializableException`.
   */
  def payloadBuilder(
      input: Any,
      serialization: Serialization,
      log: LoggingAdapter): ContainerFormats.Payload.Builder = {
    val payload = input.asInstanceOf[AnyRef]
    val builder = ContainerFormats.Payload.newBuilder()
    val serializer = serialization.findSerializerFor(payload)

    payload match {
      case t: Throwable if serializer.isInstanceOf[DisabledJavaSerializer] =>
        val notSerializableException =
          new ThrowableNotSerializableException(t.getMessage, t.getClass.getName, t.getCause)
        log.debug(
          "Couldn't serialize [{}] because Java serialization is disabled. Fallback to " +
          "ThrowableNotSerializableException. {}",
          notSerializableException.originalClassName,
          notSerializableException.originalMessage)
        val serializer2 = serialization.findSerializerFor(notSerializableException)
        builder
          .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(serializer2.toBinary(notSerializableException)))
          .setSerializerId(serializer2.identifier)
        val manifest = Serializers.manifestFor(serializer2, notSerializableException)
        if (manifest.nonEmpty) builder.setMessageManifest(ByteString.copyFromUtf8(manifest))

      case _ =>
        // already zero copy of the serialized byte string so no point in going via bytebuf even if supported here
        builder
          .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(serializer.toBinary(payload)))
          .setSerializerId(serializer.identifier)
        val manifest = Serializers.manifestFor(serializer, payload)
        if (manifest.nonEmpty) builder.setMessageManifest(ByteString.copyFromUtf8(manifest))
    }
    builder
  }

  def deserializePayload(payload: ContainerFormats.Payload, serialization: Serialization): Any = {
    val manifest = if (payload.hasMessageManifest) payload.getMessageManifest.toStringUtf8 else ""
    serialization.serializerByIdentity(payload.getSerializerId) match {
      case serializer: ByteBufferSerializer =>
        // may avoid one copy of the serialized payload if the proto byte is the right kind and the
        // underlying payload serializer handles byte buffers
        val buffer = payload.getEnclosedMessage.asReadOnlyByteBuffer()
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        serializer.fromBinary(buffer, manifest)
      case serializer: SerializerWithStringManifest =>
        serializer.fromBinary(payload.getEnclosedMessage.toByteArray, manifest)
      case _ =>
        // only old class based manifest serializers?
        serialization.deserialize(payload.getEnclosedMessage.toByteArray, payload.getSerializerId, manifest).get
    }
  }

}
