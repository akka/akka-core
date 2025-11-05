/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.remote.artery.jfr

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.remote.artery.NoOpRemotingFlightRecorder
import akka.remote.artery.RemotingFlightRecorder
import akka.testkit.AkkaSpec
import akka.testkit.TestKit

class JFRRemotingFlightRecorderSpec extends AkkaSpec {

  "The RemotingFlightRecorder" must {

    "use the JFR one on Java 11" in {
      val extension = RemotingFlightRecorder(system)
      extension shouldBe a[JFRRemotingFlightRecorder]

      extension.transportStopped() // try to actually report something and see that it doesn't throw or something
    }

    "be disabled if configured to" in {
      val system = ActorSystem(
        "JFRRemotingFlightRecorderSpec-2",
        ConfigFactory.parseString(
          """
           akka.java-flight-recorder.enabled = false
            """))
      try {
        val extension = RemotingFlightRecorder(system)
        extension should === (NoOpRemotingFlightRecorder)
      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }
  }

}
