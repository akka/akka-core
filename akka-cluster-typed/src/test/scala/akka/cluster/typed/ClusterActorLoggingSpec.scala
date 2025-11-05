/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.internal.ActorMdc
import akka.actor.typed.scaladsl.Behaviors

object ClusterActorLoggingSpec {
  def config = ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.remote.artery.canonical.hostname = 127.0.0.1
    """)
}

class ClusterActorLoggingSpec
    extends ScalaTestWithActorTestKit(ClusterActorLoggingSpec.config)
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  "Logging from an actor in a cluster" must {

    "include host and port in sourceActorSystem mdc entry" in {

      def addressString = system.classicSystem.asInstanceOf[ExtendedActorSystem].provider.addressString

      val behavior =
        Behaviors.setup[String] { context =>
          context.log.info("Starting")
          Behaviors.empty
        }

      LoggingTestKit
        .info("Starting")
        .withCustom { event =>
          event.mdc(ActorMdc.AkkaAddressKey) == addressString
        }
        .expect {
          spawn(behavior)
        }
    }
  }
}
