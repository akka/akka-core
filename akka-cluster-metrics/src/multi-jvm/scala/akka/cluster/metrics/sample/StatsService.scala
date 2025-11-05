/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics.sample

import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, Props, ReceiveTimeout }
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.routing.FromConfig

//#service
class StatsService extends Actor {
  // This router is used both with lookup and deploy of routees. If you
  // have a router with only lookup of routees you can use Props.empty
  // instead of Props[StatsWorker.class].
  val workerRouter = context.actorOf(FromConfig.props(Props[StatsWorker]()), name = "workerRouter")

  def receive = {
    case StatsJob(text) if text != "" =>
      val words = text.split(" ")
      val replyTo = sender() // important to not close over sender()
      // create actor that collects replies from workers
      val aggregator = context.actorOf(Props(classOf[StatsAggregator], words.size, replyTo))
      words.foreach { word =>
        workerRouter.tell(ConsistentHashableEnvelope(word, word), aggregator)
      }
  }
}

class StatsAggregator(expectedResults: Int, replyTo: ActorRef) extends Actor {
  var results = IndexedSeq.empty[Int]
  context.setReceiveTimeout(3.seconds)

  def receive = {
    case wordCount: Int =>
      results = results :+ wordCount
      if (results.size == expectedResults) {
        val meanWordLength = results.sum.toDouble / results.size
        replyTo ! StatsResult(meanWordLength)
        context.stop(self)
      }
    case ReceiveTimeout =>
      replyTo ! JobFailed("Service unavailable, try again later")
      context.stop(self)
  }
}
//#service

// not used, only for documentation
abstract class StatsService2 extends Actor {
  //#router-lookup-in-code
  import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
  import akka.routing.ConsistentHashingGroup

  val workerRouter = context.actorOf(
    ClusterRouterGroup(
      ConsistentHashingGroup(Nil),
      ClusterRouterGroupSettings(
        totalInstances = 100,
        routeesPaths = List("/user/statsWorker"),
        allowLocalRoutees = true,
        useRoles = Set("compute"))).props(),
    name = "workerRouter2")
  //#router-lookup-in-code
}

// not used, only for documentation
abstract class StatsService3 extends Actor {
  //#router-deploy-in-code
  import akka.cluster.routing.{ ClusterRouterPool, ClusterRouterPoolSettings }
  import akka.routing.ConsistentHashingPool

  val workerRouter = context.actorOf(
    ClusterRouterPool(
      ConsistentHashingPool(0),
      ClusterRouterPoolSettings(totalInstances = 100, maxInstancesPerNode = 3, allowLocalRoutees = false))
      .props(Props[StatsWorker]()),
    name = "workerRouter3")
  //#router-deploy-in-code
}
