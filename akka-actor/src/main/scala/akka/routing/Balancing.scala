/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.annotation.nowarn
import scala.collection.immutable

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.dispatch.BalancingDispatcherConfigurator
import akka.dispatch.Dispatchers

/**
 * INTERNAL API
 */
private[akka] object BalancingRoutingLogic {
  def apply(): BalancingRoutingLogic = new BalancingRoutingLogic
}

/**
 * INTERNAL API
 * Selects the first routee, balancing will be done by the dispatcher.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
private[akka] final class BalancingRoutingLogic extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else routees.head
}

/**
 * A router pool that will try to redistribute work from busy routees to idle routees.
 * All routees share the same mailbox.
 *
 * Although the technique used in this implementation is commonly known as "work stealing", the
 * actual implementation is probably best described as "work donating" because the actor of which
 * work is being stolen takes the initiative.
 *
 * The configuration parameter trumps the constructor arguments. This means that
 * if you provide `nrOfInstances` during instantiation they will be ignored if
 * the router is defined in the configuration file for the actor being used.
 *
 * <h1>Supervision Setup</h1>
 *
 * Any routees that are created by a router will be created as the router's children.
 * The router is therefore also the children's supervisor.
 *
 * The supervision strategy of the router actor can be configured with
 * [[#withSupervisorStrategy]]. If no strategy is provided, routers default to
 * a strategy of “always escalate”. This means that errors are passed up to the
 * router's supervisor for handling.
 *
 * The router's supervisor will treat the error as an error with the router itself.
 * Therefore a directive to stop or restart will cause the router itself to stop or
 * restart. The router, in turn, will cause its children to stop and restart.
 *
 * @param nrOfInstances initial number of routees in the pool
 *
 * @param supervisorStrategy strategy for supervising the routees, see 'Supervision Setup'
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   supervision, death watch and router management messages
 */
@SerialVersionUID(1L)
final case class BalancingPool(
    nrOfInstances: Int,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
    extends Pool {

  def this(config: Config) =
    this(nrOfInstances = config.getInt("nr-of-instances"))

  /**
   * Java API
   * @param nr initial number of routees in the pool
   */
  def this(nr: Int) = this(nrOfInstances = nr)

  override def createRouter(system: ActorSystem): Router = new Router(BalancingRoutingLogic())

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): BalancingPool = copy(supervisorStrategy = strategy)

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): BalancingPool = copy(routerDispatcher = dispatcherId)

  def nrOfInstances(sys: ActorSystem) = this.nrOfInstances

  /**
   * INTERNAL API
   */
  override private[akka] def newRoutee(routeeProps: Props, context: ActorContext): Routee = {

    val rawDeployPath = context.self.path.elements.drop(1).mkString("/", "/", "")
    val deployPath = BalancingPoolDeploy.invalidConfigKeyChars.foldLeft(rawDeployPath) { (replaced, c) =>
      replaced.replace(c, '_')
    }
    val dispatcherId = s"BalancingPool-$deployPath"
    def dispatchers = context.system.dispatchers

    if (!dispatchers.hasDispatcher(dispatcherId)) {
      // dynamically create the config and register the dispatcher configurator for the
      // dispatcher of this pool
      val deployDispatcherConfigPath = s"akka.actor.deployment.$deployPath.pool-dispatcher"
      val systemConfig = context.system.settings.config
      val dispatcherConfig = context.system.dispatchers.config(
        dispatcherId,
        // use the user defined 'pool-dispatcher' config as fallback, if any
        if (systemConfig.hasPath(deployDispatcherConfigPath))
          systemConfig.getConfig(deployDispatcherConfigPath)
        else ConfigFactory.empty)

      dispatchers.registerConfigurator(
        dispatcherId,
        new BalancingDispatcherConfigurator(dispatcherConfig, dispatchers.prerequisites))
    }

    val routeePropsWithDispatcher = routeeProps.withDispatcher(dispatcherId)
    ActorRefRoutee(context.actorOf(routeePropsWithDispatcher))
  }

  /**
   * Uses the supervisor strategy of the given RouterConfig
   * if this RouterConfig doesn't have one.
   */
  override def withFallback(other: RouterConfig): RouterConfig =
    if (other == NoRouter) this // NoRouter is the default, hence “neutral”
    else {

      other match {
        case p: Pool =>
          if ((this.supervisorStrategy eq Pool.defaultSupervisorStrategy)
              && (p.supervisorStrategy ne Pool.defaultSupervisorStrategy))
            this.withSupervisorStrategy(p.supervisorStrategy)
          else this

        case _ => this
      }
    }

  /**
   * Resizer cannot be used together with BalancingPool
   */
  override val resizer: Option[Resizer] = None

}

/**
 * INTERNAL API
 * Can't be in the `BalancingPool` companion for binary compatibility reasons.
 */
private[akka] object BalancingPoolDeploy {
  val invalidConfigKeyChars = List('$', '@', ':')
}
