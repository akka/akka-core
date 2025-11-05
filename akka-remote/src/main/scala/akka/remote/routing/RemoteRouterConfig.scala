/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn

import com.typesafe.config.ConfigFactory

import akka.actor.ActorCell
import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.japi.Util.immutableSeq
import akka.remote.RemoteScope
import akka.routing.ActorRefRoutee
import akka.routing.Pool
import akka.routing.Resizer
import akka.routing.Routee
import akka.routing.Router
import akka.routing.RouterActor
import akka.routing.RouterConfig

/**
 * [[akka.routing.RouterConfig]] implementation for remote deployment on defined
 * target nodes. Delegates other duties to the local [[akka.routing.Pool]],
 * which makes it possible to mix this with the built-in routers such as
 * [[akka.routing.RoundRobinGroup]] or custom routers.
 */
@SerialVersionUID(1L)
final case class RemoteRouterConfig(local: Pool, nodes: Iterable[Address]) extends Pool {

  require(nodes.nonEmpty, "Must specify list of remote target.nodes")

  def this(local: Pool, nodes: java.lang.Iterable[Address]) = this(local, immutableSeq(nodes))
  def this(local: Pool, nodes: Array[Address]) = this(local, nodes: Iterable[Address])

  // need this iterator as instance variable since Resizer may call createRoutees several times
  @nowarn @transient private val nodeAddressIter: Iterator[Address] = Stream.continually(nodes).flatten.iterator
  // need this counter as instance variable since Resizer may call createRoutees several times
  @transient private val childNameCounter = new AtomicInteger

  override def createRouter(system: ActorSystem): Router = local.createRouter(system)

  override def nrOfInstances(sys: ActorSystem): Int = local.nrOfInstances(sys)

  override def newRoutee(routeeProps: Props, context: ActorContext): Routee = {
    val name = "c" + childNameCounter.incrementAndGet
    val deploy = Deploy(
      config = ConfigFactory.empty(),
      routerConfig = routeeProps.routerConfig,
      scope = RemoteScope(nodeAddressIter.next()))

    // attachChild means that the provider will treat this call as if possibly done out of the wrong
    // context and use RepointableActorRef instead of LocalActorRef. Seems like a slightly sub-optimal
    // choice in a corner case (and hence not worth fixing).
    val ref = context
      .asInstanceOf[ActorCell]
      .attachChild(local.enrichWithPoolDispatcher(routeeProps, context).withDeploy(deploy), name, systemService = false)
    ActorRefRoutee(ref)
  }

  override def createRouterActor(): RouterActor = local.createRouterActor()

  override def supervisorStrategy: SupervisorStrategy = local.supervisorStrategy

  override def routerDispatcher: String = local.routerDispatcher

  override def resizer: Option[Resizer] = local.resizer

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case RemoteRouterConfig(_: RemoteRouterConfig, _) =>
      throw new IllegalStateException("RemoteRouterConfig is not allowed to wrap a RemoteRouterConfig")
    case RemoteRouterConfig(local: Pool, _) =>
      copy(local = this.local.withFallback(local).asInstanceOf[Pool])
    case _ => copy(local = this.local.withFallback(other).asInstanceOf[Pool])
  }

}
