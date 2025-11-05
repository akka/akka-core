/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.routing

import akka.actor._
import akka.actor.OneForOneStrategy
import akka.routing.RoundRobinPool
import akka.testkit._

object ClusterRouterSupervisorSpec {

  class KillableActor() extends Actor {

    def receive = {
      case "go away" =>
        throw new IllegalArgumentException("Goodbye then!")
    }

  }

}

class ClusterRouterSupervisorSpec extends AkkaSpec("""
  akka.actor.provider = "cluster"
  akka.remote.artery.canonical.port = 0
""") {

  import ClusterRouterSupervisorSpec._

  "Cluster aware routers" must {

    "use provided supervisor strategy" in {
      val router = system.actorOf(
        ClusterRouterPool(
          RoundRobinPool(nrOfInstances = 1, supervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
            case _ =>
              testActor ! "supervised"
              SupervisorStrategy.Stop
          }),
          ClusterRouterPoolSettings(totalInstances = 1, maxInstancesPerNode = 1, allowLocalRoutees = true))
          .props(Props(classOf[KillableActor])),
        name = "therouter")

      router ! "go away"
      expectMsg("supervised")
    }

  }

}
