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

import akka.actor.Address
import akka.actor.AddressFromURIString
import akka.annotation.InternalApi

/**
 * INTERNAL API: Adds support for serializing and deserializing [[Address]].
 */
@InternalApi private[akka] trait AddressModule extends JacksonModule {
  addSerializer(classOf[Address], () => AddressSerializer.instance, () => AddressDeserializer.instance)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AddressSerializer {
  val instance: AddressSerializer = new AddressSerializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class AddressSerializer extends StdScalarSerializer[Address](classOf[Address]) {
  override def serialize(value: Address, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeString(value.toString)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AddressDeserializer {
  val instance: AddressDeserializer = new AddressDeserializer
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class AddressDeserializer extends StdScalarDeserializer[Address](classOf[Address]) {

  def deserialize(jp: JsonParser, ctxt: DeserializationContext): Address = {
    if (jp.currentTokenId() == JsonTokenId.ID_STRING) {
      val serializedAddress = jp.getText()
      AddressFromURIString(serializedAddress)
    } else
      ctxt.handleUnexpectedToken(handledType(), jp).asInstanceOf[Address]
  }
}
