/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence

import java.io.NotSerializableException
import java.nio.charset.Charset
import akka.actor.ActorSystem
import akka.persistence.journal.{ EventAdapter, EventSeq }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import akka.testkit.TestKit
import com.typesafe.config._
import org.scalatest.wordspec.AnyWordSpec
import spray.json.JsObject

import scala.concurrent.duration._
import docs.persistence.proto.FlightAppModels

import scala.annotation.nowarn

class PersistenceSchemaEvolutionDocSpec extends AnyWordSpec {

  val customSerializerConfig =
    """
      //#custom-serializer-config
      akka.actor {
        serializers {
          my-payload = "docs.persistence.MyPayloadSerializer"
          my-snapshot = "docs.persistence.MySnapshotSerializer"
        }
        serialization-bindings {
          "docs.persistence.MyPayload" = my-payload
          "docs.persistence.MySnapshot" = my-snapshot
        }
      }
      //#custom-serializer-config
    """

  val system = ActorSystem("PersistenceSchemaEvolutionDocSpec", ConfigFactory.parseString(customSerializerConfig))
  try {
    SerializationExtension(system)
  } finally {
    TestKit.shutdownActorSystem(system, 10.seconds, false)
  }

}

class ProtobufReadOptional {

  //#protobuf-read-optional-model
  sealed abstract class SeatType { def code: String }
  object SeatType {
    def fromString(s: String) = s match {
      case Window.code => Window
      case Aisle.code  => Aisle
      case Other.code  => Other
      case _           => Unknown
    }
    case object Window extends SeatType { override val code = "W" }
    case object Aisle extends SeatType { override val code = "A" }
    case object Other extends SeatType { override val code = "O" }
    case object Unknown extends SeatType { override val code = "" }

  }

  case class SeatReserved(letter: String, row: Int, seatType: SeatType)
  //#protobuf-read-optional-model

  //#protobuf-read-optional
  /**
   * Example serializer impl which uses protocol buffers generated classes (proto.*)
   * to perform the to/from binary marshalling.
   */
  class AddedFieldsSerializerWithProtobuf extends SerializerWithStringManifest {
    override def identifier = 67876

    final val SeatReservedManifest = classOf[SeatReserved].getName

    override def manifest(o: AnyRef): String = o.getClass.getName

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      manifest match {
        case SeatReservedManifest =>
          // use generated protobuf serializer
          seatReserved(FlightAppModels.SeatReserved.parseFrom(bytes))
        case _ =>
          throw new NotSerializableException("Unable to handle manifest: " + manifest)
      }

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case s: SeatReserved =>
        FlightAppModels.SeatReserved.newBuilder
          .setRow(s.row)
          .setLetter(s.letter)
          .setSeatType(s.seatType.code)
          .build()
          .toByteArray
    }

    // -- fromBinary helpers --

    private def seatReserved(p: FlightAppModels.SeatReserved): SeatReserved =
      SeatReserved(p.getLetter, p.getRow, seatType(p))

    // handle missing field by assigning "Unknown" value
    private def seatType(p: FlightAppModels.SeatReserved): SeatType =
      if (p.hasSeatType) SeatType.fromString(p.getSeatType) else SeatType.Unknown

  }
  //#protobuf-read-optional
}

class ProtoBufRename {
  val protoIDL = """
    //#protobuf-rename-proto
    // protobuf message definition, BEFORE:
    message SeatReserved {
      required string code = 1;
    }

    // protobuf message definition, AFTER:
    message SeatReserved {
      required string seatNr = 1; // field renamed, id remains the same
    }
    //#protobuf-rename-proto
  """
}

class RenamePlainJson {
  //#rename-plain-json
  class JsonRenamedFieldAdapter extends EventAdapter {
    val marshaller = new ExampleJsonMarshaller

    val V1 = "v1"
    val V2 = "v2"

    // this could be done independently for each event type
    override def manifest(event: Any): String = V2

    override def toJournal(event: Any): JsObject =
      marshaller.toJson(event)

    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case json: JsObject =>
        EventSeq(marshaller.fromJson(manifest match {
          case V1      => rename(json, "code", "seatNr")
          case V2      => json // pass-through
          case unknown => throw new IllegalArgumentException(s"Unknown manifest: $unknown")
        }))
      case _ =>
        val c = event.getClass
        throw new IllegalArgumentException("Can only work with JSON, was: %s".format(c))
    }

    def rename(json: JsObject, from: String, to: String): JsObject = {
      val value = json.fields(from)
      val withoutOld = json.fields - from
      JsObject(withoutOld + (to -> value))
    }

  }
  //#rename-plain-json
}

object SimplestCustomSerializer {

  //#simplest-custom-serializer-model
  final case class Person(name: String, surname: String)
  //#simplest-custom-serializer-model

  //#simplest-custom-serializer
  /**
   * Simplest possible serializer, uses a string representation of the Person class.
   *
   * Usually a serializer like this would use a library like:
   * protobuf, kryo, avro, cap'n proto, flatbuffers, SBE or some other dedicated serializer backend
   * to perform the actual to/from bytes marshalling.
   */
  class SimplestPossiblePersonSerializer extends SerializerWithStringManifest {
    val Utf8 = Charset.forName("UTF-8")

    val PersonManifest = classOf[Person].getName

    // unique identifier of the serializer
    def identifier = 1234567

    // extract manifest to be stored together with serialized object
    override def manifest(o: AnyRef): String = o.getClass.getName

    // serialize the object
    override def toBinary(obj: AnyRef): Array[Byte] = obj match {
      case p: Person => s"""${p.name}|${p.surname}""".getBytes(Utf8)
      case _         => throw new IllegalArgumentException(s"Unable to serialize to bytes, clazz was: ${obj.getClass}!")
    }

    // deserialize the object, using the manifest to indicate which logic to apply
    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      manifest match {
        case PersonManifest =>
          val nameAndSurname = new String(bytes, Utf8)
          val Array(name, surname) = nameAndSurname.split("[|]")
          Person(name, surname)
        case _ =>
          throw new NotSerializableException(
            s"Unable to deserialize from bytes, manifest was: $manifest! Bytes length: " +
            bytes.length)
      }

  }

  //#simplest-custom-serializer
}

class PersonSerializerSettingsBox {
  val PersonSerializerSettings = """
  //#simplest-custom-serializer-config
  # application.conf
  akka {
    actor {
      serializers {
        person = "docs.persistence.SimplestPossiblePersonSerializer"
      }

      serialization-bindings {
        "docs.persistence.Person" = person
      }
    }
  }
  //#simplest-custom-serializer-config
  """
}

final case class SamplePayload(p: Any)

//#split-events-during-recovery
trait Version1
trait Version2

// V1 event:
final case class UserDetailsChanged(name: String, address: String) extends Version1

// corresponding V2 events:
final case class UserNameChanged(name: String) extends Version2
final case class UserAddressChanged(address: String) extends Version2

// event splitting adapter:
class UserEventsAdapter extends EventAdapter {
  override def manifest(event: Any): String = ""

  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case UserDetailsChanged(null, address) => EventSeq(UserAddressChanged(address))
    case UserDetailsChanged(name, null)    => EventSeq(UserNameChanged(name))
    case UserDetailsChanged(name, address) =>
      EventSeq(UserNameChanged(name), UserAddressChanged(address))
    case event: Version2 => EventSeq(event)
  }

  override def toJournal(event: Any): Any = event
}
//#split-events-during-recovery

final case class CustomerBlinked(customerId: Long)

//#string-serializer-skip-deleved-event-by-manifest
case object EventDeserializationSkipped

class RemovedEventsAwareSerializer extends SerializerWithStringManifest {
  val utf8 = Charset.forName("UTF-8")
  override def identifier: Int = 8337

  val SkipEventManifestsEvents = Set("docs.persistence.CustomerBlinked" // ...
  )

  override def manifest(o: AnyRef): String = o.getClass.getName

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case _ => o.toString.getBytes(utf8) // example serialization
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case m if SkipEventManifestsEvents.contains(m) =>
        EventDeserializationSkipped

      case _ => new String(bytes, utf8)
    }
}
//#string-serializer-skip-deleved-event-by-manifest

//#string-serializer-skip-deleved-event-by-manifest-adapter
class SkippedEventsAwareAdapter extends EventAdapter {
  override def manifest(event: Any) = ""
  override def toJournal(event: Any) = event

  override def fromJournal(event: Any, manifest: String) = event match {
    case EventDeserializationSkipped => EventSeq.empty
    case _                           => EventSeq(event)
  }
}
//#string-serializer-skip-deleved-event-by-manifest-adapter

//#string-serializer-handle-rename
class RenamedEventAwareSerializer extends SerializerWithStringManifest {
  val Utf8 = Charset.forName("UTF-8")
  override def identifier: Int = 8337

  val OldPayloadClassName = "docs.persistence.OldPayload" // class NOT available anymore
  val MyPayloadClassName = classOf[SamplePayload].getName

  override def manifest(o: AnyRef): String = o.getClass.getName

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case SamplePayload(data) => s"""$data""".getBytes(Utf8)
    // previously also handled "old" events here.
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case OldPayloadClassName => SamplePayload(new String(bytes, Utf8))
      case MyPayloadClassName  => SamplePayload(new String(bytes, Utf8))
      case other               => throw new NotSerializableException(s"unexpected manifest [$other]")
    }
}
//#string-serializer-handle-rename

//#detach-models
/** Domain model - highly optimised for domain language and maybe "fluent" usage */
object DomainModel {
  final case class Customer(name: String)
  final case class Seat(code: String) {
    def bookFor(customer: Customer): SeatBooked = SeatBooked(code, customer)
  }

  final case class SeatBooked(code: String, customer: Customer)
}

/** Data model - highly optimised for schema evolution and persistence */
object DataModel {
  final case class SeatBooked(code: String, customerName: String)
}
//#detach-models

//#detach-models-adapter
class DetachedModelsAdapter extends EventAdapter {
  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case DomainModel.SeatBooked(code, customer) =>
      DataModel.SeatBooked(code, customer.name)
  }
  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case DataModel.SeatBooked(code, customerName) =>
      EventSeq(DomainModel.SeatBooked(code, DomainModel.Customer(customerName)))
  }
}
//#detach-models-adapter

// act as-if JSON library
@nowarn("msg=never used")
class ExampleJsonMarshaller {
  def toJson(any: Any): JsObject = JsObject()
  def fromJson(json: JsObject): Any = new Object
}

//#detach-models-adapter-json
class JsonDataModelAdapter extends EventAdapter {
  override def manifest(event: Any): String = ""

  val marshaller = new ExampleJsonMarshaller

  override def toJournal(event: Any): JsObject =
    marshaller.toJson(event)

  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case json: JsObject =>
      EventSeq(marshaller.fromJson(json))
    case _ =>
      throw new IllegalArgumentException("Unable to fromJournal a non-JSON object! Was: " + event.getClass)
  }
}
//#detach-models-adapter-json
