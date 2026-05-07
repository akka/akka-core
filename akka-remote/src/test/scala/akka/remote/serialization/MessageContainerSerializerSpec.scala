/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import akka.actor.ActorSelectionMessage
import akka.actor.SelectChildName
import akka.actor.SelectChildPattern
import akka.actor.SelectParent
import akka.remote.DaemonMsgCreate
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.testkit.TestActors

class MessageContainerSerializerSpec extends AkkaSpec {

  val ser = SerializationExtension(system)

  "DaemonMsgCreateSerializer" must {

    "resolve serializer for ActorSelectionMessage" in {
      ser.serializerFor(classOf[ActorSelectionMessage]).getClass should ===(classOf[MessageContainerSerializer])
    }

    "serialize and de-serialize ActorSelectionMessage" in {
      verifySerialization(
        ActorSelectionMessage(
          "hello",
          Vector(
            SelectChildName("user"),
            SelectChildName("a"),
            SelectChildName("b"),
            SelectParent,
            SelectChildPattern("*"),
            SelectChildName("c")),
          wildcardFanOut = true))
    }

    "serialize and deserialize DaemonMsgCreate with tagged actor" in {
      val props = TestActors.echoActorProps.withActorTags("one", "two")
      val deserialized =
        verifySerialization(DaemonMsgCreate(props, props.deploy, "/user/some/path", system.deadLetters))
      deserialized.deploy.tags should ===(Set("one", "two"))
    }

    def verifySerialization[T <: AnyRef](msg: T): T = {
      val deserialized = ser.deserialize(ser.serialize(msg).get, msg.getClass).get
      deserialized should ===(msg)
      deserialized
    }

  }
}
