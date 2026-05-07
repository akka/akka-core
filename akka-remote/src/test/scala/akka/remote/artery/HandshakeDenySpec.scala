/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorIdentity, Identify }
import akka.actor.RootActorPath
import akka.testkit._

object HandshakeDenySpec {

  val commonConfig = ConfigFactory.parseString("""
     akka.loglevel = WARNING
     akka.remote.artery.advanced.handshake-timeout = 2s
     akka.remote.artery.advanced.aeron.image-liveness-timeout = 1.9s
  """).withFallback(ArterySpecSupport.defaultConfig)

}

class HandshakeDenySpec extends ArteryMultiNodeSpec(HandshakeDenySpec.commonConfig) with ImplicitSender {

  var systemB = newRemoteSystem(name = Some("systemB"))

  "Artery handshake" must {

    "be denied when originating address is unknown" in {
      val sel = system.actorSelection(RootActorPath(address(systemB).copy(host = Some("127.0.0.1"))) / "user" / "echo")

      systemB.actorOf(TestActors.echoActorProps, "echo")

      EventFilter
        .warning(start = "Dropping Handshake Request from")
        .intercept {
          sel ! Identify("hi echo")
          // handshake timeout and Identify message in SendQueue is sent to deadLetters,
          // which generates the ActorIdentity(None)
          expectMsg(5.seconds, ActorIdentity("hi echo", None))
        }(systemB)
    }

  }

}
