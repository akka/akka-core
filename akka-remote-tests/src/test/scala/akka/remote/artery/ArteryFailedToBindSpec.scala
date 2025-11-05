/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import akka.remote.RARP
import akka.remote.RemoteTransportException
import akka.testkit.SocketUtil
import akka.testkit.TestKit

class ArteryFailedToBindSpec extends AnyWordSpec with Matchers {

  "an ActorSystem" must {
    "not start if port is taken" in {

      // this test is tweaked in Jenkins CI by passing -Dakka.remote.artery.transport
      // therefore we must decide whether to use UDP or not based on the runtime config
      val arterySettings = ArterySettings(ConfigFactory.load().getConfig("akka.remote.artery"))
      val useUdp = arterySettings.Transport == ArterySettings.AeronUpd
      val port = SocketUtil.temporaryLocalPort(useUdp)

      val config = ConfigFactory.parseString(s"""
           |akka {
           |  actor {
           |    provider = remote
           |  }
           |  remote {
           |    artery {
           |      canonical.hostname = "127.0.0.1"
           |      canonical.port = $port
           |      aeron.log-aeron-counters = on
           |    }
           |  }
           |}
       """.stripMargin)
      val as = ActorSystem("BindTest1", config)
      try {
        val ex = intercept[RemoteTransportException] {
          ActorSystem("BindTest2", config)
        }
        RARP(as).provider.transport.asInstanceOf[ArteryTransport].settings.Transport match {
          case ArterySettings.AeronUpd =>
            ex.getMessage should ===("Inbound Aeron channel is in errored state. See Aeron logs for details.")
          case ArterySettings.Tcp | ArterySettings.TlsTcp =>
            ex.getMessage should startWith("Failed to bind TCP")
        }

      } finally {
        TestKit.shutdownActorSystem(as)
      }
    }
  }
}
