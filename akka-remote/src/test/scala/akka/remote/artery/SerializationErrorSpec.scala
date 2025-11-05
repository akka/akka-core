/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.{ ActorIdentity, Identify, RootActorPath }
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.TestActors

object SerializationErrorSpec {

  class NotSerializableMsg

}

class SerializationErrorSpec extends ArteryMultiNodeSpec(ArterySpecSupport.defaultConfig) with ImplicitSender {
  import SerializationErrorSpec._

  val systemB = newRemoteSystem(
    name = Some("systemB"),
    extraConfig = Some("""
       akka.actor.serialization-identifiers {
         # this will cause deserialization error
         "akka.serialization.ByteArraySerializer" = -4
       }
       """))
  systemB.actorOf(TestActors.echoActorProps, "echo")
  val addressB = address(systemB)
  val rootB = RootActorPath(addressB)

  "Serialization error" must {

    "be logged when serialize fails" in {
      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      remoteRef ! "ping"
      expectMsg("ping")

      EventFilter[java.io.NotSerializableException](start = "Failed to serialize message", occurrences = 1).intercept {
        remoteRef ! new NotSerializableMsg()
      }

      remoteRef ! "ping2"
      expectMsg("ping2")
    }

    "be logged when deserialize fails" in {
      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      remoteRef ! "ping"
      expectMsg("ping")

      EventFilter
        .error(pattern = """Failed to deserialize message from \[.*\] with serializer id \[4\]""", occurrences = 1)
        .intercept {
          remoteRef ! "boom".getBytes("utf-8")
        }(systemB)

      remoteRef ! "ping2"
      expectMsg("ping2")
    }

  }

}
