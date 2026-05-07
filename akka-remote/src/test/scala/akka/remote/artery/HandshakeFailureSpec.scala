/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorIdentity, Identify }
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestProbe

object HandshakeFailureSpec {

  val commonConfig = ConfigFactory.parseString("""
     akka.remote.artery.advanced.handshake-timeout = 2s
     akka.remote.artery.advanced.aeron.image-liveness-timeout = 1.9s
  """).withFallback(ArterySpecSupport.defaultConfig)

}

class HandshakeFailureSpec extends ArteryMultiNodeSpec(HandshakeFailureSpec.commonConfig) with ImplicitSender {

  val portB = freePort()

  "Artery handshake" must {

    "allow for timeout and later connect" in {
      def sel = system.actorSelection(s"akka://systemB@localhost:$portB/user/echo")
      sel ! "hello"
      expectNoMessage(3.seconds) // longer than handshake-timeout

      val systemB =
        newRemoteSystem(name = Some("systemB"), extraConfig = Some(s"akka.remote.artery.canonical.port = $portB"))
      systemB.actorOf(TestActors.echoActorProps, "echo")

      within(10.seconds) {
        awaitAssert {
          val probe = TestProbe()
          sel.tell("hello2", probe.ref)
          probe.expectMsg(1.second, "hello2")
        }
      }

      sel ! Identify(None)
      val remoteRef = expectMsgType[ActorIdentity].ref.get

      remoteRef ! "ping"
      expectMsg("ping")
    }

  }

}
