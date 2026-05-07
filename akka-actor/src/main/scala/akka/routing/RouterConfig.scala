/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.annotation.nowarn
import scala.collection.immutable

import akka.ConfigurationException
import akka.actor.ActorContext
import akka.actor.ActorPath
import akka.actor.ActorSystem
import akka.actor.AutoReceivedMessage
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.Terminated
import akka.dispatch.Dispatchers
import akka.japi.Util.immutableSeq

/**
 * This trait represents a router factory: it produces the actual router actor
 * and creates the routing table (a function which determines the recipients
 * for each message which is to be dispatched). The resulting RoutedActorRef
 * optimizes the sending of the message so that it does NOT go through the
 * router’s mailbox unless the route returns an empty recipient set.
 *
 * '''Caution:''' This means
 * that the route function is evaluated concurrently without protection by
 * the RoutedActorRef: either provide a reentrant (i.e. pure) implementation or
 * do the locking yourself!
 *
 * '''Caution:''' Please note that the [[akka.routing.Router]] which needs to
 * be returned by `createActor()` should not send a message to itself in its
 * constructor or `preStart()` or publish its self reference from there: if
 * someone tries sending a message to that reference before the constructor of
 * RoutedActorRef has returned, there will be a `NullPointerException`!
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
trait RouterConfig extends Serializable {

  /**
   * Create the actual router, responsible for routing messages to routees.
   *
   * @param system the ActorSystem this router belongs to
   */
  def createRouter(system: ActorSystem): Router

  /**
   * Dispatcher ID to use for running the “head” actor, which handles
   * supervision, death watch and router management messages
   */
  def routerDispatcher: String

  /**
   * Possibility to define an actor for controlling the routing
   * logic from external stimuli (e.g. monitoring metrics).
   * This actor will be a child of the router "head" actor.
   * Management messages not handled by the "head" actor are
   * delegated to this controller actor.
   */
  def routingLogicController(routingLogic: RoutingLogic): Option[Props] = None

  /**
   * Is the message handled by the router head actor or the
   * [[#routingLogicController]] actor.
   */
  def isManagementMessage(msg: Any): Boolean = msg match {
    case _: AutoReceivedMessage | _: Terminated | _: RouterManagementMesssage => true
    case _                                                                    => false
  }

  /*
   * Specify that this router should stop itself when all routees have terminated (been removed).
   * By Default it is `true`, unless a `resizer` is used.
   */
  def stopRouterWhenAllRouteesRemoved: Boolean = true

  /**
   * Overridable merge strategy, by default completely prefers `this` (i.e. no merge).
   */
  def withFallback(other: RouterConfig): RouterConfig = this

  /**
   * Check that everything is there which is needed. Called in constructor of RoutedActorRef to fail early.
   */
  def verifyConfig(path: ActorPath): Unit = ()

  /**
   * INTERNAL API
   * The router "head" actor.
   */
  private[akka] def createRouterActor(): RouterActor

}

/**
 * INTERNAL API
 *
 * Used to override unset configuration in a router.
 */
private[akka] trait PoolOverrideUnsetConfig[T <: Pool] extends Pool {

  final def overrideUnsetConfig(other: RouterConfig): RouterConfig =
    if (other == NoRouter) this // NoRouter is the default, hence “neutral”
    else {

      other match {
        case p: Pool =>
          val wssConf: PoolOverrideUnsetConfig[T] =
            if ((this.supervisorStrategy eq Pool.defaultSupervisorStrategy)
                && (p.supervisorStrategy ne Pool.defaultSupervisorStrategy))
              this.withSupervisorStrategy(p.supervisorStrategy).asInstanceOf[PoolOverrideUnsetConfig[T]]
            else this

          if (wssConf.resizer.isEmpty && p.resizer.isDefined)
            wssConf.withResizer(p.resizer.get)
          else
            wssConf
        case _ => this
      }
    }

  def withSupervisorStrategy(strategy: SupervisorStrategy): T

  def withResizer(resizer: Resizer): T
}

/**
 * Java API: Base class for custom router [[Group]]
 */
abstract class GroupBase extends Group {

  def getPaths(system: ActorSystem): java.lang.Iterable[String]

  override final def paths(system: ActorSystem): immutable.Iterable[String] =
    immutableSeq(getPaths(system))
}

/**
 * `RouterConfig` for router actor with routee actors that are created external to the
 * router and the router sends messages to the specified path using actor selection,
 * without watching for termination.
 */
trait Group extends RouterConfig {

  def paths(system: ActorSystem): immutable.Iterable[String]

  /**
   * [[akka.actor.Props]] for a group router based on the settings defined by
   * this instance.
   */
  def props(): Props = Props.empty.withRouter(this)

  /**
   * INTERNAL API
   */
  private[akka] def routeeFor(path: String, context: ActorContext): Routee =
    ActorSelectionRoutee(context.actorSelection(path))

  /**
   * INTERNAL API
   */
  private[akka] override def createRouterActor(): RouterActor = new RouterActor
}

object Pool {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => SupervisorStrategy.Escalate
  }
}

/**
 * Java API: Base class for custom router [[Pool]]
 */
abstract class PoolBase extends Pool

/**
 * `RouterConfig` for router actor that creates routees as child actors and removes
 * them from the router if they terminate.
 */
trait Pool extends RouterConfig {

  /**
   * Initial number of routee instances
   */
  def nrOfInstances(sys: ActorSystem): Int

  /**
   * Use a dedicated dispatcher for the routees of the pool.
   * The dispatcher is defined in 'pool-dispatcher' configuration property in the
   * deployment section of the router.
   */
  def usePoolDispatcher: Boolean = false

  /**
   * INTERNAL API
   */
  private[akka] def newRoutee(routeeProps: Props, context: ActorContext): Routee =
    ActorRefRoutee(context.actorOf(enrichWithPoolDispatcher(routeeProps, context)))

  /**
   * INTERNAL API
   */
  private[akka] def enrichWithPoolDispatcher(routeeProps: Props, context: ActorContext): Props =
    if (usePoolDispatcher && routeeProps.dispatcher == Dispatchers.DefaultDispatcherId)
      routeeProps.withDispatcher(
        "akka.actor.deployment." + context.self.path.elements.drop(1).mkString("/", "/", "")
        + ".pool-dispatcher")
    else
      routeeProps

  /**
   * Pool with dynamically resizable number of routees return the [[akka.routing.Resizer]]
   * to use. The resizer is invoked once when the router is created, before any messages can
   * be sent to it. Resize is also triggered when messages are sent to the routees, and the
   * resizer is invoked asynchronously, i.e. not necessarily before the message has been sent.
   */
  def resizer: Option[Resizer]

  /**
   * SupervisorStrategy for the head actor, i.e. for supervising the routees of the pool.
   */
  def supervisorStrategy: SupervisorStrategy

  /**
   * [[akka.actor.Props]] for a pool router based on the settings defined by
   * this instance and the supplied [[akka.actor.Props]] for the routees created by the
   * router.
   */
  def props(routeeProps: Props): Props = routeeProps.withRouter(this)

  /*
   * Specify that this router should stop itself when all routees have terminated (been removed).
   * By Default it is `true`, unless a `resizer` is used.
   */
  override def stopRouterWhenAllRouteesRemoved: Boolean = resizer.isEmpty

  /**
   * INTERNAL API
   */
  private[akka] override def createRouterActor(): RouterActor =
    resizer match {
      case None    => new RouterPoolActor(supervisorStrategy)
      case Some(_) => new ResizablePoolActor(supervisorStrategy)
    }

}

/**
 * If a custom router implementation is not a [[Group]] nor
 * a [[Pool]] it may extend this base class.
 */
abstract class CustomRouterConfig extends RouterConfig {

  /**
   * INTERNAL API
   */
  private[akka] override def createRouterActor(): RouterActor = new RouterActor

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}

/**
 * Wraps a [[akka.actor.Props]] to mark the actor as externally configurable to be used with a router.
 * If a [[akka.actor.Props]] is not wrapped with [[FromConfig]] then the actor will ignore the router part of the deployment section
 * in the configuration.
 */
case object FromConfig extends FromConfig {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
  @inline final def apply(
      resizer: Option[Resizer] = None,
      supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
      routerDispatcher: String = Dispatchers.DefaultDispatcherId) =
    new FromConfig(resizer, supervisorStrategy, routerDispatcher)

  @inline final def unapply(fc: FromConfig): Option[String] = Some(fc.routerDispatcher)
}

/**
 * Java API: Wraps a [[akka.actor.Props]] to mark the actor as externally configurable to be used with a router.
 * If a [[akka.actor.Props]] is not wrapped with [[FromConfig]] then the actor will ignore the router part of the deployment section
 * in the configuration.
 *
 * This can be used when the dispatcher to be used for the head Router needs to be configured
 * (defaults to default-dispatcher).
 */
@SerialVersionUID(1L)
class FromConfig(
    override val resizer: Option[Resizer],
    override val supervisorStrategy: SupervisorStrategy,
    override val routerDispatcher: String)
    extends Pool {

  def this() = this(None, Pool.defaultSupervisorStrategy, Dispatchers.DefaultDispatcherId)

  override def createRouter(system: ActorSystem): Router =
    throw new UnsupportedOperationException("FromConfig must not create Router")

  /**
   * INTERNAL API
   */
  override private[akka] def createRouterActor(): RouterActor =
    throw new UnsupportedOperationException("FromConfig must not create RouterActor")

  override def verifyConfig(path: ActorPath): Unit =
    throw new ConfigurationException(s"Configuration missing for router [$path] in 'akka.actor.deployment' section.")

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): FromConfig =
    new FromConfig(resizer, strategy, routerDispatcher)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): FromConfig =
    new FromConfig(Some(resizer), supervisorStrategy, routerDispatcher)

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): FromConfig =
    new FromConfig(resizer, supervisorStrategy, dispatcherId)

  override def nrOfInstances(sys: ActorSystem): Int = 0

  /**
   * [[akka.actor.Props]] for a group router based on the settings defined by
   * this instance.
   */
  def props(): Props = Props.empty.withRouter(this)

}

/**
 * Routing configuration that indicates no routing; this is also the default
 * value which hence overrides the merge strategy in order to accept values
 * from lower-precedence sources. The decision whether or not to create a
 * router is taken in the LocalActorRefProvider based on Props.
 */
@SerialVersionUID(1L)
abstract class NoRouter extends RouterConfig

case object NoRouter extends NoRouter {
  override def createRouter(system: ActorSystem): Router =
    throw new UnsupportedOperationException("NoRouter has no Router")

  /**
   * INTERNAL API
   */
  override private[akka] def createRouterActor(): RouterActor =
    throw new UnsupportedOperationException("NoRouter must not create RouterActor")
  override def routerDispatcher: String = throw new UnsupportedOperationException("NoRouter has no dispatcher")
  override def withFallback(other: akka.routing.RouterConfig): akka.routing.RouterConfig = other

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  def props(routeeProps: Props): Props = routeeProps.withRouter(this)

}

/**
 * INTERNAL API
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
private[akka] trait RouterManagementMesssage

/**
 * Sending this message to a router will make it send back its currently used routees.
 * A [[Routees]] message is sent asynchronously to the "requester" containing information
 * about what routees the router is routing over.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
abstract class GetRoutees extends RouterManagementMesssage

@SerialVersionUID(1L) case object GetRoutees extends GetRoutees {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Message used to carry information about what routees the router is currently using.
 */
@SerialVersionUID(1L)
final case class Routees(routees: immutable.IndexedSeq[Routee]) {

  /**
   * Java API
   */
  def getRoutees: java.util.List[Routee] = {
    import scala.jdk.CollectionConverters._
    routees.asJava
  }
}

/**
 * Add a routee by sending this message to the router.
 * It may be handled after other messages.
 */
@SerialVersionUID(1L)
final case class AddRoutee(routee: Routee) extends RouterManagementMesssage

/**
 * Remove a specific routee by sending this message to the router.
 * It may be handled after other messages.
 *
 * For a pool, with child routees, the routee is stopped by sending a [[akka.actor.PoisonPill]]
 * to the routee. Precautions are taken reduce the risk of dropping messages that are concurrently
 * being routed to the removed routee, but there are no guarantees.
 *
 */
@SerialVersionUID(1L)
final case class RemoveRoutee(routee: Routee) extends RouterManagementMesssage

/**
 * Increase or decrease the number of routees in a [[Pool]].
 * It may be handled after other messages.
 *
 * Positive `change` will add that number of routees to the [[Pool]].
 * Negative `change` will remove that number of routees from the [[Pool]].
 * Routees are stopped by sending a [[akka.actor.PoisonPill]] to the routee.
 * Precautions are taken reduce the risk of dropping messages that are concurrently
 * being routed to the removed routee, but it is not guaranteed that messages are not
 * lost.
 */
@SerialVersionUID(1L)
final case class AdjustPoolSize(change: Int) extends RouterManagementMesssage
