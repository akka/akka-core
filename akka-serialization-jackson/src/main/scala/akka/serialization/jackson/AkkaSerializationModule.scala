/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import com.fasterxml.jackson.core.{ JsonGenerator, JsonParser, ObjectCodec }
import com.fasterxml.jackson.databind.{ DeserializationContext, JsonNode, SerializerProvider }
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer

import akka.serialization.{ SerializationExtension, Serializer, Serializers }

final class AkkaSerializationSerializer extends StdScalarSerializer[AnyRef](classOf[AnyRef]) with ActorSystemAccess {
  def serialization = SerializationExtension(currentSystem())
  override def serialize(value: AnyRef, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    val serializer: Serializer = serialization.findSerializerFor(value)
    val serId = serializer.identifier
    val manifest = Serializers.manifestFor(serializer, value)
    val serialized = serializer.toBinary(value)
    jgen.writeStartObject()
    jgen.writeStringField("serId", serId.toString)
    jgen.writeStringField("serManifest", manifest)
    jgen.writeBinaryField("payload", serialized)
    jgen.writeEndObject()
  }
}

final class AkkaSerializationDeserializer
    extends StdScalarDeserializer[AnyRef](classOf[AnyRef])
    with ActorSystemAccess {

  def serialization = SerializationExtension(currentSystem())

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): AnyRef = {
    val codec: ObjectCodec = jp.getCodec()
    val jsonNode = codec.readTree[JsonNode](jp)
    val id = jsonNode.get("serId").textValue().toInt
    val manifest = jsonNode.get("serManifest").textValue()
    val payload = jsonNode.get("payload").binaryValue()
    serialization.deserialize(payload, id, manifest).get
  }
}
