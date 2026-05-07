/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.annotation.nowarn

import akka.actor.ExtendedActorSystem
import akka.remote.MessageSerializer
import akka.remote.ProtobufProtocol.MyMessage
import akka.remote.WireFormats.SerializedMessage
import akka.remote.protobuf.v3.ProtobufProtocolV3.MyMessageV3
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec

// those must be defined as top level classes, to have static parseFrom
case class MaliciousMessage() {}

object ProtobufSerializerSpec {
  trait AnotherInterface
  abstract class AnotherBase
}

object AnotherMessage {
  def parseFrom(@nowarn("msg=never used") bytes: Array[Byte]): AnotherMessage =
    new AnotherMessage
}
case class AnotherMessage() {}

object AnotherMessage2 {
  def parseFrom(@nowarn("msg=never used") bytes: Array[Byte]): AnotherMessage2 =
    new AnotherMessage2
}
case class AnotherMessage2() extends ProtobufSerializerSpec.AnotherInterface {}

object AnotherMessage3 {
  def parseFrom(@nowarn("msg=never used") bytes: Array[Byte]): AnotherMessage3 =
    new AnotherMessage3
}
case class AnotherMessage3() extends ProtobufSerializerSpec.AnotherBase {}

object MaliciousMessage {
  def parseFrom(@nowarn("msg=never used") bytes: Array[Byte]): MaliciousMessage =
    new MaliciousMessage
}

class ProtobufSerializerSpec extends AkkaSpec(s"""
  akka.serialization.protobuf.allowed-classes = [
      "com.google.protobuf.GeneratedMessage",
      "com.google.protobuf.GeneratedMessageV3",
      "scalapb.GeneratedMessageCompanion",
      "akka.protobufv3.internal.GeneratedMessageV3",
      "${classOf[AnotherMessage].getName}",
      "${classOf[ProtobufSerializerSpec.AnotherInterface].getName}",
      "${classOf[ProtobufSerializerSpec.AnotherBase].getName}"
    ]
  """) {

  val ser = SerializationExtension(system)

  "Serialization" must {

    "resolve protobuf serializer" in {
      ser.serializerFor(classOf[SerializedMessage]).getClass should ===(classOf[ProtobufSerializer])
      ser.serializerFor(classOf[MyMessage]).getClass should ===(classOf[ProtobufSerializer])
      ser.serializerFor(classOf[MyMessageV3]).getClass should ===(classOf[ProtobufSerializer])
    }

    "work for SerializedMessage (just an akka.protobuf message)" in {
      // create a protobuf message
      val protobufMessage: SerializedMessage =
        MessageSerializer.serialize(system.asInstanceOf[ExtendedActorSystem], "hello")
      // serialize it with ProtobufSerializer
      val bytes = ser.serialize(protobufMessage).get
      // deserialize the bytes with ProtobufSerializer
      val deserialized = ser.deserialize(bytes, protobufMessage.getClass).get.asInstanceOf[SerializedMessage]
      deserialized.getSerializerId should ===(protobufMessage.getSerializerId)
      deserialized.getMessage should ===(protobufMessage.getMessage) // same "hello"
    }

    "work for a serialized protobuf v3 message" in {
      val protobufV3Message: MyMessageV3 =
        MyMessageV3.newBuilder().setQuery("query1").setPageNumber(1).setResultPerPage(2).build()
      val bytes = ser.serialize(protobufV3Message).get
      val deserialized: MyMessageV3 = ser.deserialize(bytes, protobufV3Message.getClass).get
      protobufV3Message should ===(deserialized)
    }

    "disallow deserialization of classes that are not in bindings and not in configured allowed classes" in {
      val originalSerializer = ser.serializerFor(classOf[MyMessage])

      intercept[IllegalArgumentException] {
        ser.deserialize(Array[Byte](), originalSerializer.identifier, classOf[MaliciousMessage].getName).get
      }
    }

    "allow deserialization of classes in configured allowed classes" in {
      val originalSerializer = ser.serializerFor(classOf[MyMessage])

      val deserialized =
        ser.deserialize(Array[Byte](), originalSerializer.identifier, classOf[AnotherMessage].getName).get
      deserialized.getClass should ===(classOf[AnotherMessage])
    }

    "allow deserialization of interfaces in configured allowed classes" in {
      val originalSerializer = ser.serializerFor(classOf[MyMessage])

      val deserialized =
        ser.deserialize(Array[Byte](), originalSerializer.identifier, classOf[AnotherMessage2].getName).get
      deserialized.getClass should ===(classOf[AnotherMessage2])
    }

    "allow deserialization of super classes in configured allowed classes" in {
      val originalSerializer = ser.serializerFor(classOf[MyMessage])

      val deserialized =
        ser.deserialize(Array[Byte](), originalSerializer.identifier, classOf[AnotherMessage3].getName).get
      deserialized.getClass should ===(classOf[AnotherMessage3])
    }

  }
}
