/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import java.lang
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.Arrays
import java.util.Locale
import java.util.Optional
import java.util.UUID
import java.util.logging.FileHandler

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.core.StreamWriteFeature
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.BootstrapSetup
import akka.actor.ExtendedActorSystem
import akka.actor.Status
import akka.actor.setup.ActorSystemSetup
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.testkit.TestActors
import akka.testkit.TestKit

object ScalaTestMessages {
  trait TestMessage

  final case class SimpleCommand(name: String) extends TestMessage
  // interesting that this doesn't have the same problem with single constructor param
  // as JavaTestMessages.SimpleCommand
  final class SimpleCommandNotCaseClass(val name: String) extends TestMessage {
    override def equals(obj: Any): Boolean = obj match {
      case other: SimpleCommandNotCaseClass => other.name == name
      case _                                => false
    }
    override def hashCode(): Int = name.hashCode
  }
  final case class SimpleCommand2(name: String, name2: String) extends TestMessage
  final case class OptionCommand(maybe: Option[String]) extends TestMessage
  final case class BooleanCommand(published: Boolean) extends TestMessage
  final case class TimeCommand(timestamp: LocalDateTime, duration: FiniteDuration) extends TestMessage
  final case class InstantCommand(instant: Instant) extends TestMessage
  final case class UUIDCommand(uuid: UUID) extends TestMessage
  final case class CollectionsCommand(strings: List[String], objects: Vector[SimpleCommand]) extends TestMessage
  final case class CommandWithActorRef(name: String, replyTo: ActorRef) extends TestMessage
  final case class CommandWithTypedActorRef(name: String, replyTo: akka.actor.typed.ActorRef[String])
      extends TestMessage
  final case class CommandWithAddress(name: String, address: Address) extends TestMessage
  case object SingletonCaseObject extends TestMessage

  final case class Event1(field1: String) extends TestMessage
  final case class Event2(field1V2: String, field2: Int) extends TestMessage
  final case class Event3(field1V2: String, field3: Int) extends TestMessage

  final case class Zoo(first: Animal) extends TestMessage
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[Lion], name = "lion"),
      new JsonSubTypes.Type(value = classOf[Elephant], name = "elephant")))
  sealed trait Animal
  final case class Lion(name: String) extends Animal
  final case class Elephant(name: String, age: Int) extends Animal
  // not defined in JsonSubTypes
  final case class Cockroach(name: String) extends Animal

  final case class OldCommandNotInBindings(name: String)

  // #jackson-scala-enumeration
  object Planet extends Enumeration {
    type Planet = Value
    val Mercury, Venus, Earth, Mars, Krypton = Value
  }

  // Uses default Jackson serialization format for Scala Enumerations
  final case class Alien(name: String, planet: Planet.Planet) extends TestMessage

  // Serializes planet values as a JsonString
  class PlanetType extends TypeReference[Planet.type] {}
  final case class Superhero(name: String, @JsonScalaEnumeration(classOf[PlanetType]) planet: Planet.Planet)
      extends TestMessage
  // #jackson-scala-enumeration

  //delegate to AkkaSerialization
  object HasAkkaSerializer {
    def apply(description: String): HasAkkaSerializer = new HasAkkaSerializer(description)
  }
  // make sure jackson would fail
  class HasAkkaSerializer private (@JsonIgnore val description: String) {

    override def toString: String = s"InnerSerialization($description)"

    def canEqual(other: Any): Boolean = other.isInstanceOf[HasAkkaSerializer]

    override def equals(other: Any): Boolean = other match {
      case that: HasAkkaSerializer =>
        (that.canEqual(this)) &&
        description == that.description
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(description)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  class InnerSerializationSerializer extends SerializerWithStringManifest {
    override def identifier: Int = 123451
    override def manifest(o: AnyRef): String = "M"
    override def toBinary(o: AnyRef): Array[Byte] = o.asInstanceOf[HasAkkaSerializer].description.getBytes()
    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = HasAkkaSerializer(new String(bytes))
  }

  final case class WithAkkaSerializer(
      @JsonDeserialize(`using` = classOf[AkkaSerializationDeserializer])
      @JsonSerialize(`using` = classOf[AkkaSerializationSerializer])
      akkaSerializer: HasAkkaSerializer)
      extends TestMessage
}

class JacksonCborSerializerSpec extends JacksonSerializerSpec("jackson-cbor") {
  "have compression disabled by default" in {
    val conf = JacksonObjectMapperProvider.configForBinding("jackson-cbor", system.settings.config)
    val compressionAlgo = conf.getString("compression.algorithm")
    compressionAlgo should ===("off")
  }
}

@nowarn // this test uses Jackson deprecated APIs
class JacksonJsonSerializerSpec extends JacksonSerializerSpec("jackson-json") {

  def serializeToJsonString(obj: AnyRef, sys: ActorSystem = system): String = {
    val blob = serializeToBinary(obj, sys)
    new String(blob, "utf-8")
  }

  def deserializeFromJsonString(
      json: String,
      serializerId: Int,
      manifest: String,
      sys: ActorSystem = system): AnyRef = {
    val blob = json.getBytes("utf-8")
    deserializeFromBinary(blob, serializerId, manifest, sys)
  }

  "JacksonJsonSerializer" must {

    "support lookup of same ObjectMapper via JacksonObjectMapperProvider" in {
      val mapper = serialization()
        .serializerFor(classOf[JavaTestMessages.TestMessage])
        .asInstanceOf[JacksonSerializer]
        .objectMapper
      JacksonObjectMapperProvider(system).getOrCreate("jackson-json", None) shouldBe theSameInstanceAs(mapper)

      val anotherBindingName = "jackson-json2"
      val mapper2 = JacksonObjectMapperProvider(system).getOrCreate(anotherBindingName, None)
      mapper2 should not be theSameInstanceAs(mapper)
      JacksonObjectMapperProvider(system).getOrCreate(anotherBindingName, None) shouldBe theSameInstanceAs(mapper2)
    }

    "JacksonSerializer configuration" must {

      withSystem("""
        akka.actor.serializers.jackson-json2 = "akka.serialization.jackson.JacksonJsonSerializer"
        akka.actor.serialization-identifiers.jackson-json2 = 999
        akka.serialization.jackson.jackson-json2 {

          # on is Jackson's default
          serialization-features.WRITE_DURATIONS_AS_TIMESTAMPS = off

          # on is Jackson's default
          deserialization-features.EAGER_DESERIALIZER_FETCH = off

          # off is Jackson's default
          mapper-features.SORT_PROPERTIES_ALPHABETICALLY = on

          # off is Jackson's default
          json-parser-features.ALLOW_COMMENTS = on

          # on is Jackson's default
          json-generator-features.AUTO_CLOSE_TARGET = off

          # off is Jackson's default
          stream-read-features.STRICT_DUPLICATE_DETECTION = on

          # off is Jackson's default
          stream-write-features.WRITE_BIGDECIMAL_AS_PLAIN = on

          # off is Jackson's default
          json-read-features.ALLOW_YAML_COMMENTS = on

          # off is Jackson's default
          json-write-features.ESCAPE_NON_ASCII = on
        }
      """) { sys =>
        val identifiedObjectMapper =
          serialization(sys).serializerByIdentity(999).asInstanceOf[JacksonJsonSerializer].objectMapper
        val namedObjectMapper = JacksonObjectMapperProvider(sys).getOrCreate("jackson-json2", None)
        val defaultObjectMapper =
          serializerFor(ScalaTestMessages.SimpleCommand("abc")).asInstanceOf[JacksonJsonSerializer].objectMapper

        "support serialization features" in {
          identifiedObjectMapper.isEnabled(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS) should ===(false)
          namedObjectMapper.isEnabled(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS) should ===(false)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS) should ===(false)
        }

        "support deserialization features" in {
          identifiedObjectMapper.isEnabled(DeserializationFeature.EAGER_DESERIALIZER_FETCH) should ===(false)
          namedObjectMapper.isEnabled(DeserializationFeature.EAGER_DESERIALIZER_FETCH) should ===(false)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(DeserializationFeature.EAGER_DESERIALIZER_FETCH) should ===(true)
        }

        "support mapper features" in {
          identifiedObjectMapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY) should ===(true)
          namedObjectMapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY) should ===(false)
        }

        "support json parser features" in {
          identifiedObjectMapper.isEnabled(JsonParser.Feature.ALLOW_COMMENTS) should ===(true)
          namedObjectMapper.isEnabled(JsonParser.Feature.ALLOW_COMMENTS) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(JsonParser.Feature.ALLOW_COMMENTS) should ===(false)
        }

        "support json generator features" in {
          identifiedObjectMapper.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET) should ===(false)
          namedObjectMapper.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET) should ===(false)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET) should ===(true)
        }

        "support stream read features" in {
          identifiedObjectMapper.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION) should ===(true)
          namedObjectMapper.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION) should ===(false)
        }

        "support stream write features" in {
          identifiedObjectMapper.isEnabled(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN) should ===(true)
          namedObjectMapper.isEnabled(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN) should ===(false)
        }

        "support json read features" in {
          // ATTENTION: this is trick. Although we are configuring `json-read-features`, Jackson
          // does not provides a way to check for `StreamReadFeature`s, so we need to check for
          // `JsonParser.Feature`.ALLOW_YAML_COMMENTS.
          // Same applies for json-write-features and JsonGenerator.Feature.
          identifiedObjectMapper.isEnabled(JsonParser.Feature.ALLOW_YAML_COMMENTS) should ===(true)
          namedObjectMapper.isEnabled(JsonParser.Feature.ALLOW_YAML_COMMENTS) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(JsonParser.Feature.ALLOW_YAML_COMMENTS) should ===(false)
        }

        "support json write features" in {
          // ATTENTION: this is trickier than `json-read-features` vs JsonParser.Feature
          // since the JsonWriteFeature replaces deprecated APIs in JsonGenerator.Feature.
          // But just like the test for `json-read-features` there is no API to check for
          // `JsonWriteFeature`s, so we need to use the deprecated APIs.
          identifiedObjectMapper.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII) should ===(true)
          namedObjectMapper.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII) should ===(true)

          // Default mapper follows Jackson and reference.conf default configuration
          defaultObjectMapper.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII) should ===(false)
        }

        "fallback to defaults when object mapper is not configured" in {
          val notConfigured = JacksonObjectMapperProvider(sys).getOrCreate("jackson-not-configured", None)
          // Use Jacksons and Akka defaults
          notConfigured.isEnabled(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS) should ===(false)
          notConfigured.isEnabled(DeserializationFeature.EAGER_DESERIALIZER_FETCH) should ===(true)
          notConfigured.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY) should ===(false)
          notConfigured.isEnabled(JsonParser.Feature.ALLOW_COMMENTS) should ===(false)
          notConfigured.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET) should ===(true)

          notConfigured.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION) should ===(false)
          notConfigured.isEnabled(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN) should ===(false)
          notConfigured.isEnabled(JsonParser.Feature.ALLOW_YAML_COMMENTS) should ===(false)
          notConfigured.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII) should ===(false)
        }
      }
    }
  }

  "JacksonJsonSerializer with Java message classes" must {
    import JavaTestMessages._

    // see SerializationFeature.WRITE_DATES_AS_TIMESTAMPS = off
    "by default serialize dates and durations as text with ISO-8601 date format" in {
      // Default format is defined in com.fasterxml.jackson.databind.util.StdDateFormat
      // ISO-8601 yyyy-MM-dd'T'HH:mm:ss.SSSZ (rfc3339)
      val msg = new TimeCommand(LocalDateTime.of(2019, 4, 29, 23, 15, 3, 12345), Duration.of(5, ChronoUnit.SECONDS))
      val json = serializeToJsonString(msg)
      val expected = """{"timestamp":"2019-04-29T23:15:03.000012345","duration":"PT5S"}"""
      json should ===(expected)

      // and full round trip
      checkSerialization(msg)

      // and it can still deserialize from numeric timestamps format
      val serializer = serializerFor(msg)
      val manifest = serializer.manifest(msg)
      val serializerId = serializer.identifier
      val deserializedFromTimestampsFormat = deserializeFromJsonString(
        """{"timestamp":[2019,4,29,23,15,3,12345],"duration":5.000000000}""",
        serializerId,
        manifest)
      deserializedFromTimestampsFormat should ===(msg)
    }

    // see SerializationFeature.WRITE_DATES_AS_TIMESTAMPS = on
    "be possible to serialize dates and durations as numeric timestamps" in {
      withSystem("""
        akka.serialization.jackson.serialization-features {
          WRITE_DATES_AS_TIMESTAMPS = on
          WRITE_DURATIONS_AS_TIMESTAMPS = on
        }
        """) { sys =>
        val msg = new TimeCommand(LocalDateTime.of(2019, 4, 29, 23, 15, 3, 12345), Duration.of(5, ChronoUnit.SECONDS))
        val json = serializeToJsonString(msg, sys)
        val expected = """{"timestamp":[2019,4,29,23,15,3,12345],"duration":5.000000000}"""
        json should ===(expected)

        // and full round trip
        checkSerialization(msg, sys)

        // and it can still deserialize from ISO format
        val serializer = serializerFor(msg, sys)
        val manifest = serializer.manifest(msg)
        val serializerId = serializer.identifier
        val deserializedFromIsoFormat = deserializeFromJsonString(
          """{"timestamp":"2019-04-29T23:15:03.000012345","duration":"PT5S"}""",
          serializerId,
          manifest,
          sys)
        deserializedFromIsoFormat should ===(msg)
      }
    }

    "serialize Instant as text with ISO-8601 date format (default)" in {
      val msg = new InstantCommand(Instant.ofEpochMilli(1559907792075L))
      val json = serializeToJsonString(msg)
      val expected = """{"instant":"2019-06-07T11:43:12.075Z"}"""
      json should ===(expected)

      // and full round trip
      checkSerialization(msg)
    }

    // FAIL_ON_UNKNOWN_PROPERTIES = off is default in reference.conf
    "not fail on unknown properties" in {
      val json = """{"name":"abc","name2":"def","name3":"ghi"}"""
      val expected = new SimpleCommand2("abc", "def")
      val serializer = serializerFor(expected)
      deserializeFromJsonString(json, serializer.identifier, serializer.manifest(expected)) should ===(expected)
    }

    "be possible to create custom ObjectMapper" in {
      pending
    }

    "be possible to tune the visibility at ObjectMapper level (FIELD, PUBLIC_ONLY)" in {
      withSystem("""
        akka.actor {
          serialization-bindings {
            "akka.serialization.jackson.JavaTestMessages$ClassWithVisibility" = jackson-json
          }
        }
        akka.serialization.jackson.visibility {
          FIELD = PUBLIC_ONLY
        }
        """) { sys =>
        val msg = new ClassWithVisibility();
        val json = serializeToJsonString(msg, sys)
        val expected = """{"publicField":"1234"}"""
        json should ===(expected)
      }
    }

    // This test ensures the default behavior in Akka 2.6 series
    // (that is "FIELD = ANY") stays consistent
    "be possible to tune the visibility at ObjectMapper level (Akka default)" in {
      withSystem("""
        akka.actor {
          serialization-bindings {
            "akka.serialization.jackson.JavaTestMessages$ClassWithVisibility" = jackson-json
          }
        }
        akka.serialization.jackson.visibility {
          ## No overrides
        }
        """) { sys =>
        val msg = new ClassWithVisibility();
        val json = serializeToJsonString(msg, sys)
        val expected =
          """{"publicField":"1234","defaultField":"abcd","protectedField":"vwxyz","privateField":"ABCD"}""".stripMargin
        json should ===(expected)
      }
    }

  }

  "JacksonJsonSerializer with Scala message classes" must {
    import ScalaTestMessages._

    "be possible to create custom ObjectMapper" in {
      val customJavaTimeModule = new SimpleModule() {
        import com.fasterxml.jackson.databind.ser.std._
        addSerializer(classOf[Instant], new StdSerializer[Instant](classOf[Instant]) {
          override def serialize(value: Instant, gen: JsonGenerator, provider: SerializerProvider): Unit = {
            gen.writeStartObject()
            gen.writeFieldName("nanos")
            gen.writeNumber(value.getNano)
            gen.writeFieldName("custom")
            gen.writeString("field")
            gen.writeEndObject()
          }
        })
      }

      val customJacksonObjectMapperFactory = new JacksonObjectMapperFactory {
        override def newObjectMapper(bindingName: String, jsonFactory: JsonFactory): ObjectMapper = {
          if (bindingName == "jackson-json") {
            val mapper: ObjectMapper = JsonMapper.builder(jsonFactory).build()
            // some customer configuration of the mapper
            mapper.setLocale(Locale.US)
            mapper
          } else
            super.newObjectMapper(bindingName, jsonFactory)
        }

        override def overrideConfiguredSerializationFeatures(
            bindingName: String,
            configuredFeatures: immutable.Seq[(SerializationFeature, Boolean)])
            : immutable.Seq[(SerializationFeature, Boolean)] = {
          if (bindingName == "jackson-json")
            configuredFeatures :+ (SerializationFeature.INDENT_OUTPUT -> true)
          else
            super.overrideConfiguredSerializationFeatures(bindingName, configuredFeatures)
        }

        override def overrideConfiguredModules(
            bindingName: String,
            configuredModules: immutable.Seq[Module]): immutable.Seq[Module] =
          if (bindingName == "jackson-json")
            configuredModules.filterNot(_.isInstanceOf[JavaTimeModule]) :+ customJavaTimeModule
          else
            super.overrideConfiguredModules(bindingName, configuredModules)

        override def overrideConfiguredMapperFeatures(
            bindingName: String,
            configuredFeatures: immutable.Seq[(MapperFeature, Boolean)]): immutable.Seq[(MapperFeature, Boolean)] =
          if (bindingName == "jackson-json")
            configuredFeatures :+ (MapperFeature.SORT_PROPERTIES_ALPHABETICALLY -> true)
          else
            super.overrideConfiguredMapperFeatures(bindingName, configuredFeatures)

        override def overrideConfiguredJsonParserFeatures(
            bindingName: String,
            configuredFeatures: immutable.Seq[(JsonParser.Feature, Boolean)])
            : immutable.Seq[(JsonParser.Feature, Boolean)] =
          if (bindingName == "jackson-json")
            configuredFeatures :+ (JsonParser.Feature.ALLOW_SINGLE_QUOTES -> true)
          else
            super.overrideConfiguredJsonParserFeatures(bindingName, configuredFeatures)

        override def overrideConfiguredJsonGeneratorFeatures(
            bindingName: String,
            configuredFeatures: immutable.Seq[(JsonGenerator.Feature, Boolean)])
            : immutable.Seq[(JsonGenerator.Feature, Boolean)] =
          if (bindingName == "jackson-json")
            configuredFeatures :+ (JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN -> true)
          else
            super.overrideConfiguredJsonGeneratorFeatures(bindingName, configuredFeatures)
      }

      val config = system.settings.config

      val setup = ActorSystemSetup()
        .withSetup(JacksonObjectMapperProviderSetup(customJacksonObjectMapperFactory))
        .withSetup(BootstrapSetup(config))
      withSystem(setup) { sys =>
        val mapper = JacksonObjectMapperProvider(sys).getOrCreate("jackson-json", None)
        mapper.isEnabled(SerializationFeature.INDENT_OUTPUT) should ===(true)
        mapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY) should ===(true)
        mapper.isEnabled(JsonParser.Feature.ALLOW_SINGLE_QUOTES) should ===(true)
        mapper.isEnabled(SerializationFeature.INDENT_OUTPUT) should ===(true)
        mapper.isEnabled(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN) should ===(true)

        val msg = InstantCommand(Instant.ofEpochMilli(1559907792075L))
        val json = serializeToJsonString(msg, sys)
        // using the custom ObjectMapper with pretty printing enabled, and a custom JavaTimeModule
        json should include("""  "instant" : {""")
        json should include("""    "nanos" : 75000000""")
        json should include("""    "custom" : "field"""")
      }
    }

    "allow deserialization of classes in configured allowed-class-prefix" in {
      val json = """{"name":"abc"}"""

      val old = SimpleCommand("abc")
      val serializer = serializerFor(old)

      val expected = OldCommandNotInBindings("abc")

      deserializeFromJsonString(json, serializer.identifier, serializer.manifest(expected)) should ===(expected)
    }

    "deserialize Enumerations as String when configured" in {
      val json = """{"name":"Superman", "planet":"Krypton"}"""

      val expected = Superhero("Superman", Planet.Krypton)
      val serializer = serializerFor(expected)

      deserializeFromJsonString(json, serializer.identifier, serializer.manifest(expected)) should ===(expected)
    }

    "compress large payload with gzip" in {
      val conf = JacksonObjectMapperProvider.configForBinding("jackson-json", system.settings.config)
      val compressionAlgo = conf.getString("compression.algorithm")
      compressionAlgo should ===("gzip")
      val compressLargerThan = conf.getBytes("compression.compress-larger-than")
      compressLargerThan should ===(32 * 1024)
      val msg = SimpleCommand("0" * (compressLargerThan + 1).toInt)
      val bytes = serializeToBinary(msg)
      JacksonSerializer.isGZipped(bytes) should ===(true)
      bytes.length should be < compressLargerThan.toInt
    }

    "not compress small payload with gzip" in {
      val msg = SimpleCommand("0" * 1000)
      val bytes = serializeToBinary(msg)
      JacksonSerializer.isGZipped(bytes) should ===(false)
    }

    "compress large payload with lz4" in withSystem("""
        akka.serialization.jackson.jackson-json.compression {
          algorithm = lz4
          compress-larger-than = 32 KiB
        }
      """) { sys =>
      val conf = JacksonObjectMapperProvider.configForBinding("jackson-json", sys.settings.config)
      val compressLargerThan = conf.getBytes("compression.compress-larger-than")
      def check(msg: AnyRef, compressed: Boolean): Unit = {
        val bytes = serializeToBinary(msg, sys)
        JacksonSerializer.isLZ4(bytes) should ===(compressed)
        bytes.length should be < compressLargerThan.toInt
        checkSerialization(msg, sys)
      }
      check(SimpleCommand("0" * (compressLargerThan + 1).toInt), true)
    }

    "not compress small payload with lz4" in withSystem("""
        akka.serialization.jackson.jackson-json.compression {
          algorithm = lz4
          compress-larger-than = 32 KiB
        }
      """) { sys =>
      val conf = JacksonObjectMapperProvider.configForBinding("jackson-json", sys.settings.config)
      val compressLargerThan = conf.getBytes("compression.compress-larger-than")
      def check(msg: AnyRef, compressed: Boolean): Unit = {
        val bytes = serializeToBinary(msg, sys)
        JacksonSerializer.isLZ4(bytes) should ===(compressed)
        bytes.length should be < compressLargerThan.toInt
        checkSerialization(msg, sys)
      }
      check(SimpleCommand("Bob"), false)
      check(new SimpleCommandNotCaseClass("Bob"), false)
    }
  }

  "JacksonJsonSerializer without type in manifest" should {
    import ScalaTestMessages._

    "deserialize messages using the serialization bindings" in withSystem(
      """
        akka.actor {
          serializers.animal = "akka.serialization.jackson.JacksonJsonSerializer"
          serialization-identifiers.animal = 9091
          serialization-bindings {
            "akka.serialization.jackson.ScalaTestMessages$Animal" = animal
          }
        }
        akka.serialization.jackson.animal.type-in-manifest = off
        """) { sys =>
      val msg = Elephant("Dumbo", 1)
      val serializer = serializerFor(msg, sys)
      serializer.manifest(msg) should ===("")
      val bytes = serializer.toBinary(msg)
      val deserialized = serializer.fromBinary(bytes, "")
      deserialized should ===(msg)
    }

    "deserialize messages using the configured deserialization type" in withSystem(
      """
        akka.actor {
          serializers.animal = "akka.serialization.jackson.JacksonJsonSerializer"
          serialization-identifiers.animal = 9091
          serialization-bindings {
            "akka.serialization.jackson.ScalaTestMessages$Elephant" = animal
            "akka.serialization.jackson.ScalaTestMessages$Lion" = animal
          }
        }
        akka.serialization.jackson.animal {
          type-in-manifest = off
          deserialization-type = "akka.serialization.jackson.ScalaTestMessages$Animal"
        }
      """) { sys =>
      val msg = Elephant("Dumbo", 1)
      val serializer = serializerFor(msg, sys)
      serializer.manifest(msg) should ===("")
      val bytes = serializer.toBinary(msg)
      val deserialized = serializer.fromBinary(bytes, "")
      deserialized should ===(msg)
    }

    "fail if multiple serialization bindings are declared with no deserialization type" in {
      an[IllegalArgumentException] should be thrownBy {
        withSystem("""
        akka.actor {
          serializers.animal = "akka.serialization.jackson.JacksonJsonSerializer"
          serialization-identifiers.animal = 9091
          serialization-bindings {
            "akka.serialization.jackson.ScalaTestMessages$Elephant" = animal
            "akka.serialization.jackson.ScalaTestMessages$Lion" = animal
          }
        }
        akka.serialization.jackson.animal {
          type-in-manifest = off
        }
      """)(sys => checkSerialization(Elephant("Dumbo", 1), sys))
      }
    }

    // issue #28918
    "cbor compatibility for reading json" in {
      val msg = SimpleCommand("abc")
      val jsonSerializer = serializerFor(msg)
      jsonSerializer.identifier should ===(31)
      val manifest = jsonSerializer.manifest(msg)
      val bytes = jsonSerializer.toBinary(msg)
      val deserialized = serialization().deserialize(bytes, 32, manifest).get
      deserialized should be(msg)
    }
  }
}

object JacksonSerializerSpec {
  def baseConfig(serializerName: String): String = s"""
    akka.actor {
      serialization-bindings {
        "akka.serialization.jackson.ScalaTestMessages$$TestMessage" = $serializerName
        "akka.serialization.jackson.JavaTestMessages$$TestMessage" = $serializerName
      }
    }
    akka.serialization.jackson.allowed-class-prefix = ["akka.serialization.jackson.ScalaTestMessages$$OldCommand"]
    
    akka.actor {
      serializers {
          inner-serializer = "akka.serialization.jackson.ScalaTestMessages$$InnerSerializationSerializer"
      }
      serialization-bindings {
        "akka.serialization.jackson.ScalaTestMessages$$HasAkkaSerializer" = "inner-serializer"
      }
    }
    """
}

abstract class JacksonSerializerSpec(serializerName: String)
    extends TestKit(
      ActorSystem(
        "JacksonJsonSerializerSpec",
        ConfigFactory.parseString(JacksonSerializerSpec.baseConfig(serializerName))))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def serialization(sys: ActorSystem = system): Serialization = SerializationExtension(sys)

  override def afterAll(): Unit = {
    shutdown()
  }

  def withSystem[T](config: String)(block: ActorSystem => T): T = {
    val sys = ActorSystem(system.name, ConfigFactory.parseString(config).withFallback(system.settings.config))
    try {
      block(sys)
    } finally shutdown(sys)
  }

  def withSystem[T](setup: ActorSystemSetup)(block: ActorSystem => T): T = {
    val sys = ActorSystem(system.name, setup)
    try {
      block(sys)
    } finally shutdown(sys)
  }

  def withTransportInformation[T](sys: ActorSystem = system)(block: () => T): T = {
    Serialization.withTransportInformation(sys.asInstanceOf[ExtendedActorSystem]) { () =>
      block()
    }
  }

  def checkSerialization(obj: AnyRef, sys: ActorSystem = system): Unit = {
    val serializer = serializerFor(obj, sys)
    val manifest = serializer.manifest(obj)
    val serializerId = serializer.identifier
    val blob = serializeToBinary(obj, sys)

    // Issue #28918, check that CBOR format is used (not JSON).
    if (blob.length > 0) {
      serializer match {
        case _: JacksonJsonSerializer =>
          if (!JacksonSerializer.isGZipped(blob) && !JacksonSerializer.isLZ4(blob))
            new String(blob.take(1), StandardCharsets.UTF_8) should ===("{")
        case _: JacksonCborSerializer =>
          new String(blob.take(1), StandardCharsets.UTF_8) should !==("{")
        case _ =>
          throw new IllegalArgumentException(s"Unexpected serializer $serializer")
      }
    }

    val deserialized = deserializeFromBinary(blob, serializerId, manifest, sys)
    deserialized should ===(obj)
  }

  def serializeToBinary(obj: AnyRef, sys: ActorSystem = system): Array[Byte] =
    serialization(sys).serialize(obj).get

  def deserializeFromBinary(
      blob: Array[Byte],
      serializerId: Int,
      manifest: String,
      sys: ActorSystem = system): AnyRef = {
    // TransportInformation added by serialization.deserialize
    serialization(sys).deserialize(blob, serializerId, manifest).get
  }

  def serializerFor(obj: AnyRef, sys: ActorSystem = system): JacksonSerializer =
    serialization(sys).findSerializerFor(obj) match {
      case serializer: JacksonSerializer => serializer
      case s =>
        throw new IllegalStateException(s"Wrong serializer ${s.getClass} for ${obj.getClass}")
    }

  "JacksonSerializer with Java message classes" must {
    import JavaTestMessages._

    "serialize simple message with one constructor parameter" in {
      checkSerialization(new SimpleCommand("Bob"))
    }

    "serialize simple message with two constructor parameters" in {
      checkSerialization(new SimpleCommand2("Bob", "Alice"))
      checkSerialization(new SimpleCommand2("Bob", ""))
      checkSerialization(new SimpleCommand2("Bob", null))
    }

    "serialize message with boolean property" in {
      checkSerialization(new BooleanCommand(true))
      checkSerialization(new BooleanCommand(false))
    }

    "serialize message with Optional property" in {
      checkSerialization(new OptionalCommand(Optional.of("abc")))
      checkSerialization(new OptionalCommand(Optional.empty()))
    }

    "serialize message with collections" in {
      val strings = Arrays.asList("a", "b", "c")
      val objects = Arrays.asList(new SimpleCommand("a"), new SimpleCommand("2"))
      val msg = new CollectionsCommand(strings, objects)
      checkSerialization(msg)
    }

    "serialize message with time" in {
      val msg = new TimeCommand(LocalDateTime.now(), Duration.of(5, ChronoUnit.SECONDS))
      checkSerialization(msg)
    }

    "serialize with ActorRef" in {
      val echo = system.actorOf(TestActors.echoActorProps)
      checkSerialization(new CommandWithActorRef("echo", echo))
    }

    "serialize with typed.ActorRef" in {
      import akka.actor.typed.scaladsl.adapter._
      val ref = system.spawnAnonymous(Behaviors.empty[String])
      checkSerialization(new CommandWithTypedActorRef("echo", ref))
    }

    "serialize with Address" in {
      val address = Address("akka", "sys", "localhost", 2552)
      checkSerialization(new CommandWithAddress("echo", address))
    }

    "serialize with polymorphism" in {
      checkSerialization(new Zoo(new Lion("Simba")))
      checkSerialization(new Zoo(new Elephant("Elephant", 49)))
      intercept[InvalidTypeIdException] {
        // Cockroach not listed in JsonSubTypes
        checkSerialization(new Zoo(new Cockroach("huh")))
      }
    }

    // TODO: Consider moving the migrations Specs to a separate Spec
    "deserialize with migrations" in withSystem(
      """
        akka.serialization.jackson.migrations {
          ## Usually the key is a FQCN but we're hacking the name to use multiple migrations for the
          ## same type in a single test.
          "deserialize-Java.Event1-into-Java.Event3" = "akka.serialization.jackson.JavaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sys =>
      val event1 = new Event1("a")
      val serializer = serializerFor(event1, sys)
      val blob = serializer.toBinary(event1)

      // Event1 has no migration configured so it uses the default manifest name (with no version)
      serializer.manifest(event1) should ===(classOf[Event1].getName)

      // Hack the manifest to enforce the use a particular migration when deserializing the blob of Event1
      val event3 = serializer.fromBinary(blob, "deserialize-Java.Event1-into-Java.Event3").asInstanceOf[Event3]
      event1.getField1 should ===(event3.getField1V2)
      event3.getField3 should ===(17)
    }

    "deserialize with migrations from V2" in {
      // produce a blob/manifest from an ActorSystem without migrations
      val event1 = new Event1("a")
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val manifest = serializer.manifest(event1)

      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event1" = "akka.serialization.jackson.JavaTestEventMigrationV2"
          "akka.serialization.jackson.JavaTestMessages$$Event2" = "akka.serialization.jackson.JavaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
        // read the blob/manifest from an ActorSystem with migrations
        val serializerV2: JacksonSerializer = serializerFor(event1, sysV2)
        val event2 = serializerV2.fromBinary(blob, manifest).asInstanceOf[Event2]
        event1.getField1 should ===(event2.getField1V2)
        event2.getField2 should ===(17)

        // Event2 has a migration configured so it uses a manifest with a version
        val serializerFor2 = serializerFor(event2, sysV2)
        serializerFor2.manifest(event2) should ===(classOf[Event2].getName + "#2")
      }

    }

    "use the migration's currentVersion on new serializations" in {
      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event2" = "akka.serialization.jackson.JavaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
        val event2 = new Event2("a", 17)
        // Event2 has a migration configured so it uses a manifest with a version
        val serializer2 = serializerFor(event2, sysV2)
        serializer2.manifest(event2) should ===(classOf[Event2].getName + "#2")
      }
    }

    "use the migration's currentVersion on new serializations when supporting forward versions" in {
      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event2" = "akka.serialization.jackson.JavaTestEventMigrationV2WithV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
        val event2 = new Event2("a", 17)
        // Event2 has a migration configured so it uses a manifest with a version
        val serializer2 = serializerFor(event2, sysV2)
        serializer2.manifest(event2) should ===(classOf[Event2].getName + "#2")
      }
    }

    "deserialize a V3 blob into a V2 class (forward-one support) and back" in {

      val blobV3 =
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event3" = "akka.serialization.jackson.JavaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV3 =>
          val event3 = new Event3("Steve", 49)
          val serializer = serializerFor(event3, sysV3)
          val blob = serializer.toBinary(event3)
          blob
        }

      val blobV2 =
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event2" = "akka.serialization.jackson.JavaTestEventMigrationV2WithV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2WithV3 =>
          val serializerForEvent2 =
            serialization(sysV2WithV3).serializerFor(classOf[Event2]).asInstanceOf[JacksonSerializer]
          val event2 = serializerForEvent2.fromBinary(blobV3, classOf[Event2].getName + "#3").asInstanceOf[Event2]
          event2.getField1V2 should ===("Steve")
          event2.getField2 should ===(49)
          serializerForEvent2.toBinary(event2)
        }

      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event3" = "akka.serialization.jackson.JavaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV3 =>
        val serializerForEvent3 = serialization(sysV3).serializerFor(classOf[Event3]).asInstanceOf[JacksonSerializer]
        val event3 = serializerForEvent3.fromBinary(blobV2, classOf[Event3].getName + "#2").asInstanceOf[Event3]
        event3.getField1V2 should ===("Steve")
        event3.getField3 should ===(49)
      }
    }

    "deserialize unsupported versions throws an exception" in {
      intercept[lang.IllegalStateException] {
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.JavaTestMessages$$Event1" = "akka.serialization.jackson.JavaTestEventMigrationV2"
          "akka.serialization.jackson.JavaTestMessages$$Event2" = "akka.serialization.jackson.JavaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
          // produce a blob/manifest from an ActorSystem without migrations
          val event1 = new Event1("a")
          val serializer = serializerFor(event1)
          val blob = serializer.toBinary(event1)
          val manifest = serializer.manifest(event1)
          // Event1 has no migration configured so it uses the default manifest name (with no version)
          val serializerV2: JacksonSerializer = serializerFor(event1, sysV2)
          serializerV2.fromBinary(blob, manifest + "#9").asInstanceOf[Event2]
        }

      }
    }
  }

  "JacksonSerializer with Scala message classes" must {
    import ScalaTestMessages._

    "serialize simple message with one constructor parameter" in {
      checkSerialization(SimpleCommand("Bob"))
      checkSerialization(new SimpleCommandNotCaseClass("Bob"))
    }

    "serialize simple message with two constructor parameters" in {
      checkSerialization(SimpleCommand2("Bob", "Alice"))
      checkSerialization(SimpleCommand2("Bob", ""))
      checkSerialization(SimpleCommand2("Bob", null))
    }

    "serialize message with boolean property" in {
      checkSerialization(BooleanCommand(true))
      checkSerialization(BooleanCommand(false))
    }

    "serialize message with Enumeration property (using Jackson legacy format)" in {
      checkSerialization(Alien("E.T.", Planet.Mars))
    }

    "serialize message with Enumeration property as a String" in {
      checkSerialization(Superhero("Kal El", Planet.Krypton))
    }

    "serialize message with Optional property" in {
      checkSerialization(OptionCommand(Some("abc")))
      checkSerialization(OptionCommand(None))
    }

    "serialize message with collections" in {
      val strings = "a" :: "b" :: "c" :: Nil
      val objects = Vector(SimpleCommand("a"), SimpleCommand("2"))
      val msg = CollectionsCommand(strings, objects)
      checkSerialization(msg)
    }

    "serialize message with time" in {
      val msg = TimeCommand(LocalDateTime.now(), 5.seconds)
      checkSerialization(msg)
    }

    "serialize FiniteDuration as java.time.Duration" in {
      withTransportInformation() { () =>
        val scalaMsg = TimeCommand(LocalDateTime.now(), 5.seconds)
        val scalaSerializer = serializerFor(scalaMsg)
        val blob = scalaSerializer.toBinary(scalaMsg)
        val javaMsg = new JavaTestMessages.TimeCommand(scalaMsg.timestamp, Duration.ofSeconds(5))
        val javaSerializer = serializerFor(javaMsg)
        val deserialized = javaSerializer.fromBinary(blob, javaSerializer.manifest(javaMsg))
        deserialized should ===(javaMsg)
      }
    }

    "serialize message with UUID property" in {
      val uuid = UUID.randomUUID()
      checkSerialization(UUIDCommand(uuid))
    }

    "serialize case object" in {
      checkSerialization(TopLevelSingletonCaseObject)
      checkSerialization(SingletonCaseObject)
    }

    "serialize with ActorRef" in {
      val echo = system.actorOf(TestActors.echoActorProps)
      checkSerialization(CommandWithActorRef("echo", echo))
    }

    "serialize with typed.ActorRef" in {
      import akka.actor.typed.scaladsl.adapter._
      val ref = system.spawnAnonymous(Behaviors.empty[String])
      checkSerialization(CommandWithTypedActorRef("echo", ref))
    }

    "serialize with Address" in {
      val address = Address("akka", "sys", "localhost", 2552)
      checkSerialization(CommandWithAddress("echo", address))
    }

    "serialize with polymorphism" in {
      checkSerialization(Zoo(Lion("Simba")))
      checkSerialization(Zoo(Elephant("Elephant", 49)))
      intercept[InvalidTypeIdException] {
        // Cockroach not listed in JsonSubTypes
        checkSerialization(Zoo(Cockroach("huh")))
      }
    }

    // TODO: Consider moving the migrations Specs to a separate Spec
    "deserialize with migrations" in withSystem(
      """
        akka.serialization.jackson.migrations {
          ## Usually the key is a FQCN but we're hacking the name to use multiple migrations for the
          ## same type in a single test.
          "deserialize-Event1-into-Event3" = "akka.serialization.jackson.ScalaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sys =>
      val event1 = Event1("a")
      val serializer = serializerFor(event1, sys)
      val blob = serializer.toBinary(event1)

      // Event1 has no migration configured so it uses the default manifest name (with no version)
      serializer.manifest(event1) should ===(classOf[Event1].getName)

      // Hack the manifest to enforce the use a particular migration when deserializing the blob of Event1
      val event3 = serializer.fromBinary(blob, "deserialize-Event1-into-Event3").asInstanceOf[Event3]
      event1.field1 should ===(event3.field1V2)
      event3.field3 should ===(17)
    }

    "deserialize with migrations from V2" in {
      // produce a blob/manifest from an ActorSystem without migrations
      val event1 = Event1("a")
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val manifest = serializer.manifest(event1)

      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event1" = "akka.serialization.jackson.ScalaTestEventMigrationV2"
          "akka.serialization.jackson.ScalaTestMessages$$Event2" = "akka.serialization.jackson.ScalaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
        // read the blob/manifest from an ActorSystem with migrations
        val serializerV2: JacksonSerializer = serializerFor(event1, sysV2)
        val event2 = serializerV2.fromBinary(blob, manifest).asInstanceOf[Event2]
        event1.field1 should ===(event2.field1V2)
        event2.field2 should ===(17)

        // Event2 has a migration configured so it uses a manifest with a version
        val serializerFor2 = serializerFor(event2, sysV2)
        serializerFor2.manifest(event2) should ===(classOf[Event2].getName + "#2")
      }

    }

    "use the migration's currentVersion on new serializations" in {
      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event2" = "akka.serialization.jackson.ScalaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
        val event2 = new Event2("a", 17)
        // Event2 has a migration configured so it uses a manifest with a version
        val serializer2 = serializerFor(event2, sysV2)
        serializer2.manifest(event2) should ===(classOf[Event2].getName + "#2")
      }
    }

    "deserialize a V3 blob into a V2 class (forward-one support) and back" in {

      val blobV3 =
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event3" = "akka.serialization.jackson.ScalaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV3 =>
          val event3 = new Event3("Steve", 49)
          val serializer = serializerFor(event3, sysV3)
          val blob = serializer.toBinary(event3)
          blob
        }

      val blobV2 =
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event2" = "akka.serialization.jackson.ScalaTestEventMigrationV2WithV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2WithV3 =>
          val serializerForEvent2 =
            serialization(sysV2WithV3).serializerFor(classOf[Event2]).asInstanceOf[JacksonSerializer]
          val event2 = serializerForEvent2.fromBinary(blobV3, classOf[Event2].getName + "#3").asInstanceOf[Event2]
          event2.field1V2 should ===("Steve")
          event2.field2 should ===(49)
          serializerForEvent2.toBinary(event2)
        }

      withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event3" = "akka.serialization.jackson.ScalaTestEventMigrationV3"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV3 =>
        val serializerForEvent3 = serialization(sysV3).serializerFor(classOf[Event3]).asInstanceOf[JacksonSerializer]
        val event3 = serializerForEvent3.fromBinary(blobV2, classOf[Event3].getName + "#2").asInstanceOf[Event3]
        event3.field1V2 should ===("Steve")
        event3.field3 should ===(49)
      }
    }

    "deserialize unsupported versions throws an exception" in {
      intercept[lang.IllegalStateException] {
        withSystem(s"""
        akka.serialization.jackson.migrations {
          "akka.serialization.jackson.ScalaTestMessages$$Event1" = "akka.serialization.jackson.ScalaTestEventMigrationV2"
          "akka.serialization.jackson.ScalaTestMessages$$Event2" = "akka.serialization.jackson.ScalaTestEventMigrationV2"
        }
        """ + JacksonSerializerSpec.baseConfig(serializerName)) { sysV2 =>
          // produce a blob/manifest from an ActorSystem without migrations
          val event1 = new Event1("a")
          val serializer = serializerFor(event1)
          val blob = serializer.toBinary(event1)
          val manifest = serializer.manifest(event1)
          // Event1 has no migration configured so it uses the default manifest name (with no version)
          val serializerV2: JacksonSerializer = serializerFor(event1, sysV2)
          serializerV2.fromBinary(blob, manifest + "#9").asInstanceOf[Event2]
        }

      }
    }

    "not allow serialization of deny listed class" in {
      val serializer = serializerFor(SimpleCommand("ok"))
      val fileHandler = new FileHandler(s"target/tmp-${this.getClass.getName}")
      try {
        intercept[IllegalArgumentException] {
          serializer.manifest(fileHandler)
        }.getMessage.toLowerCase should include("deny list")
      } finally fileHandler.close()
    }

    "not allow deserialization of deny list class" in {
      withTransportInformation() { () =>
        val msg = SimpleCommand("ok")
        val serializer = serializerFor(msg)
        val blob = serializer.toBinary(msg)
        intercept[IllegalArgumentException] {
          // maliciously changing manifest
          serializer.fromBinary(blob, classOf[FileHandler].getName)
        }.getMessage.toLowerCase should include("deny list")
      }
    }

    "not allow serialization of class that is not in serialization-bindings (allowed-class-prefix)" in {
      val serializer = serializerFor(SimpleCommand("ok"))
      intercept[IllegalArgumentException] {
        serializer.manifest(Status.Success("bad"))
      }.getMessage.toLowerCase should include("allowed-class-prefix")
    }

    "not allow deserialization of class that is not in serialization-bindings (allowed-class-prefix)" in {
      withTransportInformation() { () =>
        val msg = SimpleCommand("ok")
        val serializer = serializerFor(msg)
        val blob = serializer.toBinary(msg)
        intercept[IllegalArgumentException] {
          // maliciously changing manifest
          serializer.fromBinary(blob, classOf[Status.Success].getName)
        }.getMessage.toLowerCase should include("allowed-class-prefix")
      }
    }

    "not allow serialization-bindings of open-ended types" in {
      JacksonSerializer.disallowedSerializationBindings.foreach { clazz =>
        val className = clazz.getName
        withClue(className) {
          intercept[IllegalArgumentException] {
            val sys = ActorSystem(
              system.name,
              ConfigFactory.parseString(s"""
              akka.actor.serialization-bindings {
                "$className" = $serializerName
                "akka.serialization.jackson.ScalaTestMessages$$TestMessage" = $serializerName
              }
              """).withFallback(system.settings.config))
            try {
              SerializationExtension(sys).serialize(SimpleCommand("hi")).get
            } finally shutdown(sys)
          }
        }
      }
    }

    "delegate to akka serialization" in {
      checkSerialization(WithAkkaSerializer(HasAkkaSerializer("cat")))
    }

  }
}

case object TopLevelSingletonCaseObject extends ScalaTestMessages.TestMessage
