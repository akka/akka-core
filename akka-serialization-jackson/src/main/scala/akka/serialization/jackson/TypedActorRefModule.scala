/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi

/**
 * INTERNAL API: Adds support for serializing and deserializing [[akka.actor.typed.ActorRef]].
 */
@InternalApi private[akka] trait TypedActorRefModule extends JacksonModule {
  addSerializer(classOf[ActorRef[_]], () => TypedActorRefSerializer.instance, () => TypedActorRefDeserializer.instance)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TypedActorRefSerializer {
  val instance: TypedActorRefSerializer = new TypedActorRefSerializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TypedActorRefSerializer
    extends StdScalarSerializer[ActorRef[_]](classOf[ActorRef[_]])
    with ActorSystemAccess {
  override def serialize(value: ActorRef[_], jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    val serializedActorRef = ActorRefResolver(currentSystem().toTyped).toSerializationFormat(value)
    jgen.writeString(serializedActorRef)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TypedActorRefDeserializer {
  val instance: TypedActorRefDeserializer = new TypedActorRefDeserializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TypedActorRefDeserializer
    extends StdScalarDeserializer[ActorRef[_]](classOf[ActorRef[_]])
    with ActorSystemAccess {

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): ActorRef[_] = {
    if (jp.currentTokenId() == JsonTokenId.ID_STRING) {
      val serializedActorRef = jp.getText()
      ActorRefResolver(currentSystem().toTyped).resolveActorRef(serializedActorRef)
    } else
      ctxt.handleUnexpectedToken(handledType(), jp).asInstanceOf[ActorRef[_]]
  }
}
