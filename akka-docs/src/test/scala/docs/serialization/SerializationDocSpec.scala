/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.serialization {

  //#imports
  import akka.actor._
  import akka.actor.typed.scaladsl.Behaviors
  import akka.cluster.Cluster
  import akka.serialization._

  //#imports

  import akka.testkit._
  import com.typesafe.config.ConfigFactory
  import akka.actor.ExtendedActorSystem
  import java.nio.charset.StandardCharsets
  import scala.annotation.nowarn

  //#marker-interface
  import akka.serialization.jackson.JsonSerializable

  final case class MyMessage(name: String, nr: Int) extends JsonSerializable
  //#marker-interface

  //#my-own-serializer
  class MyOwnSerializer extends Serializer {

    // If you need logging here, introduce a constructor that takes an ExtendedActorSystem.
    // class MyOwnSerializer(actorSystem: ExtendedActorSystem) extends Serializer
    // Get a logger using:
    // private val logger = Logging(actorSystem, this)

    // This is whether "fromBinary" requires a "clazz" or not
    def includeManifest: Boolean = true

    // Pick a unique identifier for your Serializer,
    // you've got a couple of billions to choose from,
    // 0 - 40 is reserved by Akka itself
    def identifier = 1234567

    // "toBinary" serializes the given object to an Array of Bytes
    def toBinary(obj: AnyRef): Array[Byte] = {
      // Put the code that serializes the object here
      //#...
      Array[Byte]()
      //#...
    }

    // "fromBinary" deserializes the given array,
    // using the type hint (if any, see "includeManifest" above)
    def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
      // Put your code that deserializes here
      //#...
      null
      //#...
    }
  }
  //#my-own-serializer

  //#my-own-serializer2
  class MyOwnSerializer2 extends SerializerWithStringManifest {

    val CustomerManifest = "customer"
    val UserManifest = "user"
    val UTF_8 = StandardCharsets.UTF_8.name()

    // Pick a unique identifier for your Serializer,
    // you've got a couple of billions to choose from,
    // 0 - 40 is reserved by Akka itself
    def identifier = 1234567

    // The manifest (type hint) that will be provided in the fromBinary method
    // Use `""` if manifest is not needed.
    def manifest(obj: AnyRef): String =
      obj match {
        case _: Customer => CustomerManifest
        case _: User     => UserManifest
      }

    // "toBinary" serializes the given object to an Array of Bytes
    def toBinary(obj: AnyRef): Array[Byte] = {
      // Put the real code that serializes the object here
      obj match {
        case Customer(name) => name.getBytes(UTF_8)
        case User(name)     => name.getBytes(UTF_8)
      }
    }

    // "fromBinary" deserializes the given array,
    // using the type hint
    def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      // Put the real code that deserializes here
      manifest match {
        case CustomerManifest =>
          Customer(new String(bytes, UTF_8))
        case UserManifest =>
          User(new String(bytes, UTF_8))
      }
    }
  }
  //#my-own-serializer2

  trait MyOwnSerializable
  final case class Customer(name: String) extends MyOwnSerializable
  final case class User(name: String) extends MyOwnSerializable

  object SerializerIdConfig {
    val config =
      """
        #//#serialization-identifiers-config
        akka {
          actor {
            serialization-identifiers {
              "docs.serialization.MyOwnSerializer" = 1234567
            }
          }
        }
        #//#serialization-identifiers-config
        """
  }

  @nowarn("msg=never used") // sample snippets
  class SerializationDocSpec extends AkkaSpec {
    "demonstrate configuration of serialize messages" in {
      val config = ConfigFactory.parseString("""
      #//#serialize-messages-config
      akka {
        actor {
          serialize-messages = on
        }
      }
      #//#serialize-messages-config
      """)
      val a = ActorSystem("system", config)
      a.settings.SerializeAllMessages should be(true)
      shutdown(a)
    }

    "demonstrate configuration of serialize creators" in {
      val config = ConfigFactory.parseString("""
      #//#serialize-creators-config
      akka {
        actor {
          serialize-creators = on
        }
      }
      #//#serialize-creators-config
      """)
      val a = ActorSystem("system", config)
      a.settings.SerializeAllCreators should be(true)
      shutdown(a)
    }

    "demonstrate configuration of serializers" in {
      val config = ConfigFactory.parseString("""
      #//#serialize-serializers-config
      akka {
        actor {
          serializers {
            jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
            jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
            proto = "akka.remote.serialization.ProtobufSerializer"
            myown = "docs.serialization.MyOwnSerializer"
          }
        }
      }
      #//#serialize-serializers-config
      """)
      val a = ActorSystem("system", config)
      shutdown(a)
    }

    "demonstrate configuration of serialization-bindings" in {
      val config = ConfigFactory.parseString("""
      #//#serialization-bindings-config
      akka {
        actor {
          serializers {
            jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
            jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
            proto = "akka.remote.serialization.ProtobufSerializer"
            myown = "docs.serialization.MyOwnSerializer"
          }

          serialization-bindings {
            "com.google.protobuf.Message" = proto
            "docs.serialization.MyOwnSerializable" = myown
          }
        }
      }
      #//#serialization-bindings-config
      """)
      val a = ActorSystem("system", config)
      SerializationExtension(a).serializerFor(classOf[Customer]).getClass should be(classOf[MyOwnSerializer])
      shutdown(a)
    }

    "demonstrate the programmatic API" in {
      //#programmatic
      val system = ActorSystem("example")

      // Get the Serialization Extension
      val serialization = SerializationExtension(system)

      // Have something to serialize
      val original = "woohoo"

      // Turn it into bytes, and retrieve the serializerId and manifest, which are needed for deserialization
      val bytes = serialization.serialize(original).get
      val serializerId = serialization.findSerializerFor(original).identifier
      val manifest = Serializers.manifestFor(serialization.findSerializerFor(original), original)

      // Turn it back into an object
      val back = serialization.deserialize(bytes, serializerId, manifest).get
      //#programmatic

      // Voilá!
      back should be(original)

      shutdown(system)
    }

    def demonstrateTypedActorSystem(): Unit = {
      //#programmatic-typed
      import akka.actor.typed.ActorSystem

      val system = ActorSystem(Behaviors.empty, "example")

      // Get the Serialization Extension
      val serialization = SerializationExtension(system)
      //#programmatic-typed
    }

    def demonstrateSerializationOfActorRefs(): Unit = {
      val theActorRef: ActorRef = system.deadLetters
      val extendedSystem: ExtendedActorSystem = system.asInstanceOf[ExtendedActorSystem]

      //#actorref-serializer
      // Serialize
      // (beneath toBinary)
      val serializedRef: String = Serialization.serializedActorPath(theActorRef)

      // Then serialize the identifier however you like

      // Deserialize
      // (beneath fromBinary)
      val deserializedRef = extendedSystem.provider.resolveActorRef(serializedRef)
      // Then use the ActorRef
      //#actorref-serializer
    }

    def demonstrateSerializationOfActorRefs2(): Unit = {
      val theActorRef: ActorRef = system.deadLetters

      //#external-address-default
      val selfAddress = Cluster(system).selfAddress

      val serializedRef: String =
        theActorRef.path.toSerializationFormatWithAddress(selfAddress)
      //#external-address-default
    }
  }
}
