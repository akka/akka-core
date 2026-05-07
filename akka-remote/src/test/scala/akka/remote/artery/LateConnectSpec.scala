/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.RootActorPath
import akka.remote.RARP
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestProbe

object LateConnectSpec {

  val config = ConfigFactory.parseString("""
     akka.remote.artery.advanced.handshake-timeout = 3s
     akka.remote.artery.advanced.aeron.image-liveness-timeout = 2.9s
  """).withFallback(ArterySpecSupport.defaultConfig)

}

class LateConnectSpec extends ArteryMultiNodeSpec(LateConnectSpec.config) with ImplicitSender {

  val portB = freePort()
  lazy val systemB =
    newRemoteSystem(name = Some("systemB"), extraConfig = Some(s"akka.remote.artery.canonical.port = $portB"))

  "Connection" must {

    "be established after initial lazy restart" in {
      system.actorOf(TestActors.echoActorProps, "echoA")

      val echoB = system.actorSelection(s"akka://systemB@localhost:$portB/user/echoB")
      echoB ! "ping1"

      // let the outbound streams be restarted (lazy), systemB is not started yet
      Thread.sleep((RARP(system).provider.remoteSettings.Artery.Advanced.HandshakeTimeout + 1.second).toMillis)

      // start systemB
      systemB.actorOf(TestActors.echoActorProps, "echoB")

      val probeB = TestProbe()(systemB)
      val echoA = systemB.actorSelection(RootActorPath(RARP(system).provider.getDefaultAddress) / "user" / "echoA")
      echoA.tell("ping2", probeB.ref)
      probeB.expectMsg(10.seconds, "ping2")

      echoB ! "ping3"
      expectMsg("ping3")
    }
  }
}
