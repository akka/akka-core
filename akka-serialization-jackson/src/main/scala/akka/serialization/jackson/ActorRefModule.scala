/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
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

import akka.actor.ActorRef
import akka.annotation.InternalApi

/**
 * INTERNAL API: Adds support for serializing and deserializing [[ActorRef]].
 */
@InternalApi private[akka] trait ActorRefModule extends JacksonModule {
  addSerializer(classOf[ActorRef], () => ActorRefSerializer.instance, () => ActorRefDeserializer.instance)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorRefSerializer {
  val instance: ActorRefSerializer = new ActorRefSerializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ActorRefSerializer
    extends StdScalarSerializer[ActorRef](classOf[ActorRef])
    with ActorSystemAccess {
  override def serialize(value: ActorRef, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    val serializedActorRef = value.path.toSerializationFormatWithAddress(currentSystem().provider.getDefaultAddress)
    jgen.writeString(serializedActorRef)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorRefDeserializer {
  val instance: ActorRefDeserializer = new ActorRefDeserializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ActorRefDeserializer
    extends StdScalarDeserializer[ActorRef](classOf[ActorRef])
    with ActorSystemAccess {

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): ActorRef = {
    if (jp.currentTokenId() == JsonTokenId.ID_STRING) {
      val serializedActorRef = jp.getText()
      currentSystem().provider.resolveActorRef(serializedActorRef)
    } else
      ctxt.handleUnexpectedToken(handledType(), jp).asInstanceOf[ActorRef]
  }
}
