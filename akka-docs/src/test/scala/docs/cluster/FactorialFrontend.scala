/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package scala.docs.cluster

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.routing.FromConfig
import akka.actor.ReceiveTimeout

//#frontend
class FactorialFrontend(upToN: Int, repeat: Boolean) extends Actor with ActorLogging {

  val backend = context.actorOf(FromConfig.props(), name = "factorialBackendRouter")

  override def preStart(): Unit = {
    sendJobs()
    if (repeat) {
      context.setReceiveTimeout(10.seconds)
    }
  }

  def receive = {
    case (n: Int, factorial: BigInt) =>
      if (n == upToN) {
        log.debug("{}! = {}", n, factorial)
        if (repeat) sendJobs()
        else context.stop(self)
      }
    case ReceiveTimeout =>
      log.info("Timeout")
      sendJobs()
  }

  def sendJobs(): Unit = {
    log.info("Starting batch of factorials up to [{}]", upToN)
    (1 to upToN).foreach { backend ! _ }
  }
}
//#frontend

object FactorialFrontend {
  def main(args: Array[String]): Unit = {
    val upToN = 200

    val config =
      ConfigFactory.parseString("akka.cluster.roles = [frontend]").withFallback(ConfigFactory.load("factorial"))

    val system = ActorSystem("ClusterSystem", config)
    system.log.info("Factorials will start when 2 backend members in the cluster.")
    //#registerOnUp
    Cluster(system).registerOnMemberUp {
      system.actorOf(Props(classOf[FactorialFrontend], upToN, true), name = "factorialFrontend")
    }
    //#registerOnUp

  }
}

// not used, only for documentation
abstract class FactorialFrontend2 extends Actor {
  //#router-lookup-in-code
  import akka.cluster.routing.ClusterRouterGroup
  import akka.cluster.routing.ClusterRouterGroupSettings
  import akka.cluster.metrics.AdaptiveLoadBalancingGroup
  import akka.cluster.metrics.HeapMetricsSelector

  val backend = context.actorOf(
    ClusterRouterGroup(
      AdaptiveLoadBalancingGroup(HeapMetricsSelector),
      ClusterRouterGroupSettings(
        totalInstances = 100,
        routeesPaths = List("/user/factorialBackend"),
        allowLocalRoutees = true,
        useRoles = Set("backend"))).props(),
    name = "factorialBackendRouter2")

  //#router-lookup-in-code
}

// not used, only for documentation
abstract class FactorialFrontend3 extends Actor {
  //#router-deploy-in-code
  import akka.cluster.routing.ClusterRouterPool
  import akka.cluster.routing.ClusterRouterPoolSettings
  import akka.cluster.metrics.AdaptiveLoadBalancingPool
  import akka.cluster.metrics.SystemLoadAverageMetricsSelector

  val backend = context.actorOf(
    ClusterRouterPool(
      AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector),
      ClusterRouterPoolSettings(
        totalInstances = 100,
        maxInstancesPerNode = 3,
        allowLocalRoutees = false,
        useRoles = Set("backend"))).props(Props[FactorialBackend]()),
    name = "factorialBackendRouter3")
  //#router-deploy-in-code
}
