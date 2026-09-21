/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

object ClusterSingletonSingleNodeLeavingSpec {

  class TheSingleton(probe: ActorRef) extends Actor {
    probe ! "started"

    override def postStop(): Unit =
      probe ! "stopped"

    override def receive: Receive = {
      case msg => sender() ! msg
    }
  }
}

class ClusterSingletonSingleNodeLeavingSpec
    extends AkkaSpec("""
  akka.actor.provider = akka.cluster.ClusterActorRefProvider
  # both much longer than the expected stop time, also with a dilated timeout,
  # to verify that stopping does not wait for the hand-over retry
  akka.cluster.singleton.hand-over-retry-interval = 60s
  akka.coordinated-shutdown.phases.cluster-exiting.timeout = 60s
  akka.remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  """) {
  import ClusterSingletonSingleNodeLeavingSpec._

  private val singleNodeSystem = ActorSystem(system.name, system.settings.config)

  "ClusterSingleton in a single node cluster" must {
    "stop without waiting for hand-over when leaving" in {
      val probe = TestProbe()
      singleNodeSystem.actorOf(
        ClusterSingletonManager.props(
          singletonProps = Props(new TheSingleton(probe.ref)),
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings(singleNodeSystem)),
        name = "echo")

      val cluster = Cluster(singleNodeSystem)
      cluster.join(cluster.selfAddress)
      awaitAssert {
        cluster.selfMember.status should ===(MemberStatus.Up)
      }
      probe.expectMsg("started")

      CoordinatedShutdown(singleNodeSystem).run(CoordinatedShutdown.ClusterLeavingReason)
      probe.expectMsg(10.seconds, "stopped")
    }
  }

  override def afterTermination(): Unit =
    shutdown(singleNodeSystem)
}
