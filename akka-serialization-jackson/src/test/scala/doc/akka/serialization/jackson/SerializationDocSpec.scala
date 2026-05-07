/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.serialization.jackson

import java.util.Optional
import akka.actor.ActorSystem
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers
import akka.testkit.TestKit
import com.fasterxml.jackson.annotation.{ JsonSubTypes, JsonTypeInfo, JsonTypeName }
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

//#marker-interface
import akka.serialization.jackson.JsonSerializable

final case class Message(name: String, nr: Int) extends JsonSerializable
//#marker-interface

object SerializationDocSpec {
  val config = """
    #//#serialization-bindings
    akka.actor {
      serialization-bindings {
        "com.myservice.MySerializable" = jackson-json
      }
    }
    #//#serialization-bindings
  """

  val configMigration = """
    #//#migrations-conf
    akka.serialization.jackson.migrations {
      "com.myservice.event.ItemAdded" = "com.myservice.event.ItemAddedMigration"
    }
    #//#migrations-conf
  """

  val configMigrationRenamClass = """
    #//#migrations-conf-rename
    akka.serialization.jackson.migrations {
      "com.myservice.event.OrderAdded" = "com.myservice.event.OrderPlacedMigration"
    }
    #//#migrations-conf-rename
  """

  val configSpecific = """
    #//#specific-config
    akka.serialization.jackson.jackson-json {
      serialization-features {
        WRITE_DATES_AS_TIMESTAMPS = off
      }
    }
    akka.serialization.jackson.jackson-cbor {
      serialization-features {
        WRITE_DATES_AS_TIMESTAMPS = on
      }
    }
    #//#specific-config
  """

  val configSeveral = """
    #//#several-config
    akka.actor {
      serializers {
        jackson-json-message = "akka.serialization.jackson.JacksonJsonSerializer"
        jackson-json-event   = "akka.serialization.jackson.JacksonJsonSerializer"
      }
      serialization-identifiers {
        jackson-json-message = 9001
        jackson-json-event = 9002
      }
      serialization-bindings {
        "com.myservice.MyMessage" = jackson-json-message
        "com.myservice.MyEvent" = jackson-json-event
      }
    }
    akka.serialization.jackson {
      jackson-json-message {
        serialization-features {
          WRITE_DATES_AS_TIMESTAMPS = on
        }
      }
      jackson-json-event {
        serialization-features {
          WRITE_DATES_AS_TIMESTAMPS = off
        }
      }
    }
    #//#several-config
  """

  val configManifestless = """
    #//#manifestless
    akka.actor {
      serializers {
        jackson-json-event = "akka.serialization.jackson.JacksonJsonSerializer"
      }
      serialization-identifiers {
        jackson-json-event = 9001
      }
      serialization-bindings {
        "com.myservice.MyEvent" = jackson-json-event
      }
    }
    akka.serialization.jackson {
      jackson-json-event {
        type-in-manifest = off
        # Since there is exactly one serialization binding declared for this
        # serializer above, this is optional, but if there were none or many,
        # this would be mandatory.
        deserialization-type = "com.myservice.MyEvent"
      }
    }
    #//#manifestless
  """

  object Polymorphism {

    //#polymorphism
    final case class Zoo(primaryAttraction: Animal) extends JsonSerializable

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(
      Array(
        new JsonSubTypes.Type(value = classOf[Lion], name = "lion"),
        new JsonSubTypes.Type(value = classOf[Elephant], name = "elephant")))
    sealed trait Animal

    final case class Lion(name: String) extends Animal

    final case class Elephant(name: String, age: Int) extends Animal
    //#polymorphism
  }

  object PolymorphismMixedClassObject {

    //#polymorphism-case-object
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
    //#polymorphism-case-object
  }

  val configDateTime = """
    #//#date-time
    akka.serialization.jackson.serialization-features {
      WRITE_DATES_AS_TIMESTAMPS = on
      WRITE_DURATIONS_AS_TIMESTAMPS = on
    }
    #//#date-time
    """

  val configAllowList = """
    #//#allowed-class-prefix
    akka.serialization.jackson.allowed-class-prefix =
      ["com.myservice.event.OrderAdded", "com.myservice.command"]
    #//#allowed-class-prefix
  """

}

class SerializationDocSpec
    extends TestKit(
      ActorSystem(
        "SerializationDocSpec",
        ConfigFactory.parseString(
          """
    akka.serialization.jackson.migrations {
        # migrations for Java classes
        "jdoc.akka.serialization.jackson.v2b.ItemAdded" = "jdoc.akka.serialization.jackson.v2b.ItemAddedMigration"
        "jdoc.akka.serialization.jackson.v2c.ItemAdded" = "jdoc.akka.serialization.jackson.v2c.ItemAddedMigration"
        "jdoc.akka.serialization.jackson.v2a.Customer" = "jdoc.akka.serialization.jackson.v2a.CustomerMigration"
        "jdoc.akka.serialization.jackson.v1.OrderAdded" = "jdoc.akka.serialization.jackson.v2a.OrderPlacedMigration"

        # migrations for Scala classes
        "doc.akka.serialization.jackson.v2b.ItemAdded" = "doc.akka.serialization.jackson.v2b.ItemAddedMigration"
        "doc.akka.serialization.jackson.v2c.ItemAdded" = "doc.akka.serialization.jackson.v2c.ItemAddedMigration"
        "doc.akka.serialization.jackson.v2a.Customer" = "doc.akka.serialization.jackson.v2a.CustomerMigration"
        "doc.akka.serialization.jackson.v1.OrderAdded" = "doc.akka.serialization.jackson.v2a.OrderPlacedMigration"
    }
    akka.actor {
      allow-java-serialization = off
    }
    """)))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val serialization: Serialization = SerializationExtension(system)

  override def afterAll(): Unit = {
    shutdown()
  }

  def verifySerialization(obj: AnyRef): AnyRef = {
    val serializer = serialization.serializerFor(obj.getClass)
    val manifest = Serializers.manifestFor(serializer, obj)
    val serializerId = serializer.identifier
    val blob = serialization.serialize(obj).get
    val deserialized = serialization.deserialize(blob, serializerId, manifest).get
    deserialized
  }

  private def serializerFor(obj: Any): SerializerWithStringManifest =
    serialization.serializerFor(obj.getClass).asInstanceOf[SerializerWithStringManifest]

  "serialize trait + case classes" in {
    import doc.akka.serialization.jackson.SerializationDocSpec.Polymorphism._
    verifySerialization(Zoo(Lion("Simba"))) should ===(Zoo(Lion("Simba")))
    verifySerialization(Zoo(Elephant("Dumbo", 1))) should ===(Zoo(Elephant("Dumbo", 1)))
  }

  "serialize trait + case classes + case object" in {
    import doc.akka.serialization.jackson.SerializationDocSpec.PolymorphismMixedClassObject._
    verifySerialization(Zoo(Lion("Simba"))) should ===(Zoo(Lion("Simba")))
    verifySerialization(Zoo(Elephant("Dumbo", 1))) should ===(Zoo(Elephant("Dumbo", 1)))
    verifySerialization(Zoo(Unicorn)) should ===(Zoo(Unicorn))
  }

  "serialize trait + object ADT" in {
    import CustomAdtSerializer.Compass
    import CustomAdtSerializer.Direction._

    verifySerialization(Compass(North)) should ===(Compass(North))
    verifySerialization(Compass(East)) should ===(Compass(East))
    verifySerialization(Compass(South)) should ===(Compass(South))
    verifySerialization(Compass(West)) should ===(Compass(West))
  }

  "EventMigration doc sample Java classes" must {
    import jdoc.akka.serialization.jackson.v1.OrderAdded
    import jdoc.akka.serialization.jackson.v2a.Customer
    import jdoc.akka.serialization.jackson.v2a.ItemAdded
    import jdoc.akka.serialization.jackson.v2a.OrderPlaced

    "test add optional field" in {
      val event1 =
        new jdoc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer.fromBinary(blob, classOf[ItemAdded].getName).asInstanceOf[ItemAdded]
      event2.quantity should ===(event1.quantity)
      event2.discount should ===(Optional.empty())
      event2.note should ===("")

      verifySerialization(new ItemAdded("123", "ab123", 2, Optional.of(0.1), "thanks"))
    }

    "test add mandatory field" in {
      val event1 =
        new jdoc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer
        .fromBinary(blob, classOf[jdoc.akka.serialization.jackson.v2b.ItemAdded].getName)
        .asInstanceOf[jdoc.akka.serialization.jackson.v2b.ItemAdded]
      event2.quantity should ===(event1.quantity)
      event2.discount should be(0.0 +- 0.000001)

      verifySerialization(new jdoc.akka.serialization.jackson.v2b.ItemAdded("123", "ab123", 2, 0.1))
    }

    "test rename field" in {
      val event1 =
        new jdoc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer
        .fromBinary(blob, classOf[jdoc.akka.serialization.jackson.v2c.ItemAdded].getName)
        .asInstanceOf[jdoc.akka.serialization.jackson.v2c.ItemAdded]
      event2.itemId should ===(event1.productId)
      event2.quantity should ===(event1.quantity)
    }

    "test structural changes" in {
      val cust1 =
        new jdoc.akka.serialization.jackson.v1.Customer("A", "B", "C", "D", "E")
      val serializer = serializerFor(cust1)
      val blob = serializer.toBinary(cust1)
      val cust2 = serializer.fromBinary(blob, classOf[Customer].getName).asInstanceOf[Customer]
      cust2.name should ===(cust1.name)
      cust2.shippingAddress.street should ===(cust1.street)
      cust2.shippingAddress.city should ===(cust1.city)
      cust2.shippingAddress.zipCode should ===(cust1.zipCode)
      cust2.shippingAddress.country should ===(cust1.country)
    }

    "test rename class" in {
      val order1 = new OrderAdded("1234")
      val serializer = serializerFor(order1)
      val blob = serializer.toBinary(order1)
      val order2 = serializer.fromBinary(blob, classOf[OrderAdded].getName).asInstanceOf[OrderPlaced]
      order2.shoppingCartId should ===(order1.shoppingCartId)
    }
  }

  "EventMigration doc sample Scala classes" must {
    import doc.akka.serialization.jackson.v1.OrderAdded
    import doc.akka.serialization.jackson.v2a.Customer
    import doc.akka.serialization.jackson.v2a.ItemAdded
    import doc.akka.serialization.jackson.v2a.OrderPlaced

    "test add optional field" in {
      val event1 =
        doc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer.fromBinary(blob, classOf[ItemAdded].getName).asInstanceOf[ItemAdded]
      event2.quantity should ===(event1.quantity)
      event2.discount should ===(None)
      event2.note should ===("")

      verifySerialization(ItemAdded("123", "ab123", 2, Some(0.1), "thanks"))
    }

    "test add mandatory field" in {
      val event1 =
        doc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer
        .fromBinary(blob, classOf[doc.akka.serialization.jackson.v2b.ItemAdded].getName)
        .asInstanceOf[doc.akka.serialization.jackson.v2b.ItemAdded]
      event2.quantity should ===(event1.quantity)
      event2.discount should be(0.0 +- 0.000001)

      verifySerialization(doc.akka.serialization.jackson.v2b.ItemAdded("123", "ab123", 2, 0.1))
    }

    "test rename field" in {
      val event1 =
        doc.akka.serialization.jackson.v1.ItemAdded("123", "ab123", 2)
      val serializer = serializerFor(event1)
      val blob = serializer.toBinary(event1)
      val event2 = serializer
        .fromBinary(blob, classOf[doc.akka.serialization.jackson.v2c.ItemAdded].getName)
        .asInstanceOf[doc.akka.serialization.jackson.v2c.ItemAdded]
      event2.itemId should ===(event1.productId)
      event2.quantity should ===(event1.quantity)
    }

    "test structural changes" in {
      val cust1 =
        doc.akka.serialization.jackson.v1.Customer("A", "B", "C", "D", "E")
      val serializer = serializerFor(cust1)
      val blob = serializer.toBinary(cust1)
      val cust2 = serializer.fromBinary(blob, classOf[Customer].getName).asInstanceOf[Customer]
      cust2.name should ===(cust1.name)
      cust2.shippingAddress.street should ===(cust1.street)
      cust2.shippingAddress.city should ===(cust1.city)
      cust2.shippingAddress.zipCode should ===(cust1.zipCode)
      cust2.shippingAddress.country should ===(cust1.country)
    }

    "test rename class" in {
      val order1 = OrderAdded("1234")
      val serializer = serializerFor(order1)
      val blob = serializer.toBinary(order1)
      val order2 = serializer.fromBinary(blob, classOf[OrderAdded].getName).asInstanceOf[OrderPlaced]
      order2.shoppingCartId should ===(order1.shoppingCartId)
    }
  }

}
