/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

// FIXME maybe move many things to `akka.serialization.jackson.internal` package?

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer

import akka.annotation.InternalApi
import akka.stream.SinkRef
import akka.stream.SourceRef
import akka.stream.StreamRefResolver

/**
 * INTERNAL API: Adds support for serializing and deserializing [[akka.stream.SourceRef]] and [[akka.stream.SinkRef]].
 */
@InternalApi private[akka] trait StreamRefModule extends JacksonModule {
  addSerializer(classOf[SourceRef[_]], () => SourceRefSerializer.instance, () => SourceRefDeserializer.instance)
  addSerializer(classOf[SinkRef[_]], () => SinkRefSerializer.instance, () => SinkRefDeserializer.instance)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SourceRefSerializer {
  val instance: SourceRefSerializer = new SourceRefSerializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SourceRefSerializer
    extends StdScalarSerializer[SourceRef[_]](classOf[SourceRef[_]])
    with ActorSystemAccess {

  override def serialize(value: SourceRef[_], jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    val resolver = StreamRefResolver(currentSystem())
    val serializedSourceRef = resolver.toSerializationFormat(value)
    jgen.writeString(serializedSourceRef)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SourceRefDeserializer {
  val instance: SourceRefDeserializer = new SourceRefDeserializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SourceRefDeserializer
    extends StdScalarDeserializer[SourceRef[_]](classOf[SourceRef[_]])
    with ActorSystemAccess {

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): SourceRef[_] = {
    if (jp.currentTokenId() == JsonTokenId.ID_STRING) {
      val serializedSourceRef = jp.getText()
      StreamRefResolver(currentSystem()).resolveSourceRef(serializedSourceRef)
    } else
      ctxt.handleUnexpectedToken(handledType(), jp).asInstanceOf[SourceRef[_]]
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SinkRefSerializer {
  val instance: SinkRefSerializer = new SinkRefSerializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SinkRefSerializer
    extends StdScalarSerializer[SinkRef[_]](classOf[SinkRef[_]])
    with ActorSystemAccess {

  override def serialize(value: SinkRef[_], jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    val resolver = StreamRefResolver(currentSystem())
    val serializedSinkRef = resolver.toSerializationFormat(value)
    jgen.writeString(serializedSinkRef)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SinkRefDeserializer {
  val instance: SinkRefDeserializer = new SinkRefDeserializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SinkRefDeserializer
    extends StdScalarDeserializer[SinkRef[_]](classOf[SinkRef[_]])
    with ActorSystemAccess {

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): SinkRef[_] = {
    if (jp.currentTokenId() == JsonTokenId.ID_STRING) {
      val serializedSinkref = jp.getText()
      StreamRefResolver(currentSystem()).resolveSinkRef(serializedSinkref)
    } else
      ctxt.handleUnexpectedToken(handledType(), jp).asInstanceOf[SinkRef[_]]
  }
}
