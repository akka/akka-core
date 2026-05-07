/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorSystem, Address }
import akka.testkit._
import scala.jdk.CollectionConverters._

class DaemonicSpec extends AkkaSpec {

  "Remoting configured with daemonic = on" must {

    "shut down correctly after getting connection refused" in {
      // get all threads running before actor system is started
      val origThreads: Set[Thread] = Thread.getAllStackTraces.keySet().asScala.to(Set)
      // create a separate actor system that we can check the threads for
      val daemonicSystem = ActorSystem(
        "daemonic",
        ConfigFactory.parseString("""
        akka.daemonic = on
        akka.actor.provider = remote
        akka.remote.artery.canonical.port = 0
        akka.log-dead-letters-during-shutdown = off
        #akka.remote.artery.advanced.aeron.idle-cpu = 5
      """))

      try {
        val unusedPort = 86 // very unlikely to ever be used, "system port" range reserved for Micro Focus Cobol

        val protocol = "akka"
        val unusedAddress =
          RARP(daemonicSystem).provider.getExternalAddressFor(Address(protocol, "", "", unusedPort)).get
        val selection = daemonicSystem.actorSelection(s"$unusedAddress/user/SomeActor")
        selection ! "whatever"

        // get new non daemonic threads running
        awaitAssert({
          val newNonDaemons: Set[Thread] =
            Thread.getAllStackTraces.keySet().asScala.filter(t => !origThreads(t) && !t.isDaemon).to(Set)
          newNonDaemons should ===(Set.empty[Thread])
        }, 4.seconds)

      } finally {
        shutdown(daemonicSystem)
      }
    }
  }
}
