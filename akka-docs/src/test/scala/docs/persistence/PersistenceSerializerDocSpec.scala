/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence

import com.typesafe.config._

import scala.concurrent.duration._
import org.scalatest.wordspec.AnyWordSpec
import akka.actor.ActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.testkit.TestKit

class PersistenceSerializerDocSpec extends AnyWordSpec {

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

  val system = ActorSystem("PersistenceSerializerDocSpec", ConfigFactory.parseString(customSerializerConfig))
  try {
    SerializationExtension(system)
  } finally {
    TestKit.shutdownActorSystem(system, 10.seconds, false)
  }
}

class MyPayload
class MySnapshot

class MyPayloadSerializer extends Serializer {
  def identifier: Int = 77124
  def includeManifest: Boolean = false
  def toBinary(o: AnyRef): Array[Byte] = ???
  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = ???
}

class MySnapshotSerializer extends Serializer {
  def identifier: Int = 77125
  def includeManifest: Boolean = false
  def toBinary(o: AnyRef): Array[Byte] = ???
  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = ???
}
