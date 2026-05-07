/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.{ JavaSerializer, SerializationExtension }

object ActorRefSerializationSpec {
  def config = ConfigFactory.parseString("""
      akka.actor {
        # test is verifying Java serialization of ActorRef
        allow-java-serialization = on
        warn-about-java-serializer-usage = off
      }
      akka.remote.artery.canonical.port = 0
    """)

  case class MessageWrappingActorRef(s: String, ref: ActorRef[Unit]) extends java.io.Serializable
}

class ActorRefSerializationSpec
    extends ScalaTestWithActorTestKit(ActorRefSerializationSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  val serialization = SerializationExtension(system)

  "ActorRef[T]" must {
    "be serialized and deserialized by MiscMessageSerializer" in {
      val obj = spawn(Behaviors.empty[Unit])
      serialization.findSerializerFor(obj) match {
        case serializer: MiscMessageSerializer =>
          val blob = serializer.toBinary(obj)
          val ref = serializer.fromBinary(blob, serializer.manifest(obj))
          ref should ===(obj)
        case s =>
          throw new IllegalStateException(s"Wrong serializer ${s.getClass} for ${obj.getClass}")
      }
    }

    "be serialized and deserialized by JavaSerializer inside another java.io.Serializable message" in {
      val ref = spawn(Behaviors.empty[Unit])
      val obj = ActorRefSerializationSpec.MessageWrappingActorRef("some message", ref)

      serialization.findSerializerFor(obj) match {
        case serializer: JavaSerializer =>
          val blob = serializer.toBinary(obj)
          val restored = serializer.fromBinary(blob, None)
          restored should ===(obj)
        case s =>
          throw new IllegalStateException(s"Wrong serializer ${s.getClass} for ${obj.getClass}")
      }
    }
  }
}
