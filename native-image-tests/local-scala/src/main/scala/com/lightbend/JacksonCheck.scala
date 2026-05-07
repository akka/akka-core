/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend

import akka.actor.typed.ActorSystem
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.jackson.JsonSerializable
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object JacksonCheck {

  final case class SingleParam(field: String) extends JsonSerializable

  final case class NestedTypes(field: NestedType, field2: NestedType2) extends JsonSerializable

  final case class NestedType(someField: String)
  final case class NestedType2(otherField: Double, yetAnother: Int)

  final case class EvenMoreNested(
      nestedTypes: NestedTypes,
      duration: FiniteDuration,
      option: Option[String],
      javaDuration: java.time.Duration,
      instant: Instant,
      list: List[String],
      listOfOwnType: Seq[ImInACollection])
      extends JsonSerializable

  // this needs to be explicitly marked with trait
  final case class ImInACollection(value: String) extends JsonSerializable

  // from the akka-serialization-jackson docs

  // type hierarchy / ADT, concrete subtypes auto-detected by the AkkaJacksonSerializationFeature
  final case class Zoo(primaryAttraction: Animal) extends JsonSerializable

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[Lion], name = "lion"),
      new JsonSubTypes.Type(value = classOf[Elephant], name = "elephant"),
      new JsonSubTypes.Type(value = classOf[Unicorn], name = "unicorn")))
  sealed trait Animal

  final case class Lion(name: String) extends Animal
  final case class Elephant(name: String, age: Int) extends Animal

  @JsonDeserialize(`using` = classOf[UnicornDeserializer])
  sealed trait Unicorn extends Animal
  @JsonTypeName("unicorn")
  case object Unicorn extends Unicorn

  class UnicornDeserializer extends StdDeserializer[Unicorn](Unicorn.getClass) {
    // whenever we need to deserialize an instance of Unicorn trait, we return the object Unicorn
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Unicorn = Unicorn
  }

  // object enum
  @JsonSerialize(`using` = classOf[DirectionJsonSerializer])
  @JsonDeserialize(`using` = classOf[DirectionJsonDeserializer])
  sealed trait Direction

  object Direction {
    case object North extends Direction
    case object East extends Direction
    case object South extends Direction
    case object West extends Direction
  }

  class DirectionJsonSerializer extends StdSerializer[Direction](classOf[Direction]) {
    import Direction._

    override def serialize(value: Direction, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      val strValue = value match {
        case North => "N"
        case East  => "E"
        case South => "S"
        case West  => "W"
      }
      gen.writeString(strValue)
    }
  }

  class DirectionJsonDeserializer extends StdDeserializer[Direction](classOf[Direction]) {
    import Direction._

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Direction = {
      p.getText match {
        case "N" => North
        case "E" => East
        case "S" => South
        case "W" => West
      }
    }
  }

  final case class Compass(currentDirection: Direction) extends JsonSerializable

  def checkJackson(implicit system: ActorSystem[_]): String = {
    val serializationExtension = SerializationExtension(system)
    def serializationRoundtrip(someObject: AnyRef): Unit = {
      val serializer =
        serializationExtension.serializerFor(someObject.getClass).asInstanceOf[SerializerWithStringManifest]
      val manifest = serializer.manifest(someObject)
      val bytes = serializer.toBinary(someObject)
      val deserialized = serializer.fromBinary(bytes, manifest)
      require(
        someObject == deserialized,
        s"Jackson Serialization of ${someObject.getClass} doesn't work, ${someObject} isn't equal to serialized-deserialized ${deserialized}")
    }

    serializationRoundtrip(SingleParam("some text"))
    serializationRoundtrip(NestedTypes(NestedType("some text"), NestedType2(7d, 4)))
    serializationRoundtrip(
      EvenMoreNested(
        NestedTypes(NestedType("some text"), NestedType2(1d, 9)),
        5.seconds,
        Some("optional string"),
        java.time.Duration.ofSeconds(7),
        Instant.now(),
        List("one"),
        Vector(ImInACollection("collection item"))))
    serializationRoundtrip(Zoo(Lion("aslan")))
    serializationRoundtrip(Zoo(Unicorn))
    serializationRoundtrip(Compass(Direction.South))

    serializationRoundtrip(new JavaJacksonModels.SimpleCommand("some text"))
    serializationRoundtrip(new JavaJacksonModels.Zoo(new JavaJacksonModels.Elephant("dumbo", 23)))

    "Akka Serialization Jackson works"
  }
}
