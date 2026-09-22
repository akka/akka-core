/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import akka.actor.PoisonPill
import akka.cluster.Cluster
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestActors

class ClusterSingletonProxyStartupSpec
    extends AkkaSpec("""
  akka.actor.provider = cluster
  # much longer than the expected time to identify the singleton, also with a dilated timeout
  akka.cluster.singleton-proxy.singleton-identification-interval = 60s
  akka.remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  """)
    with ImplicitSender {

  "ClusterSingletonProxy" must {
    "identify a singleton that is started right after the first identification attempt" in {
      system.actorOf(
        ClusterSingletonManager.props(TestActors.echoActorProps, PoisonPill, ClusterSingletonManagerSettings(system)),
        "echo")
      val proxy =
        system.actorOf(ClusterSingletonProxy.props("/user/echo", ClusterSingletonProxySettings(system)), "echoProxy")

      Cluster(system).join(Cluster(system).selfAddress)

      proxy ! "hello"
      expectMsg(10.seconds, "hello")
    }
  }
}
