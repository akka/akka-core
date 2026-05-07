/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets

import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.serialization.Serialization
import akka.serialization.SerializerWithStringManifest

final case class TestPayload(ref: ActorRef)

class TestSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  def identifier: Int = 666
  def manifest(o: AnyRef): String = o match {
    case _: TestPayload => "A"
    case _              => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
  def toBinary(o: AnyRef): Array[Byte] = o match {
    case TestPayload(ref) =>
      verifyTransportInfo()
      val refStr = Serialization.serializedActorPath(ref)
      refStr.getBytes(StandardCharsets.UTF_8)
    case _ => throw new NotSerializableException() // compiler exhaustiveness check pleaser
  }
  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    verifyTransportInfo()
    manifest match {
      case "A" =>
        val refStr = new String(bytes, StandardCharsets.UTF_8)
        val ref = system.provider.resolveActorRef(refStr)
        TestPayload(ref)
      case _ => throw new NotSerializableException() // compiler exhaustiveness check pleaser
    }
  }

  private def verifyTransportInfo(): Unit = {
    Serialization.currentTransportInformation.value match {
      case null =>
        throw new IllegalStateException("currentTransportInformation was not set")
      case t =>
        if (t.system ne system)
          throw new IllegalStateException(s"wrong system in currentTransportInformation, ${t.system} != $system")
        if (t.address != system.provider.getDefaultAddress)
          throw new IllegalStateException(
            s"wrong address in currentTransportInformation, ${t.address} != ${system.provider.getDefaultAddress}")
    }
  }
}
