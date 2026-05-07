/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.event.Logging.Info
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

object LogSourceSpec {
  class Reporter extends Actor with ActorLogging {
    def receive = {
      case s: String =>
        log.info(s)
    }
  }
}

class LogSourceSpec extends AkkaSpec("""
    akka.loglevel = INFO
    akka.actor.provider = remote
    akka.remote.artery.canonical.port = 0
  """) {

  import LogSourceSpec._

  val reporter = system.actorOf(Props[Reporter](), "reporter")
  val logProbe = TestProbe()
  system.eventStream.subscribe(system.actorOf(Props(new Actor {
    def receive = {
      case i @ Info(_, _, msg: String) if msg contains "hello" => logProbe.ref ! i
      case _                                                   =>
    }
  }).withDeploy(Deploy.local), "logSniffer"), classOf[Logging.Info])

  "Log events" must {

    "should include host and port for local LogSource" in {
      reporter ! "hello"
      val info = logProbe.expectMsgType[Info]
      info.message should ===("hello")
      val defaultAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
      info.logSource should include(defaultAddress.toString)
    }
  }
}
