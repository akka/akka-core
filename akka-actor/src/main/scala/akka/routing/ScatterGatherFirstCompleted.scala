/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.TimeoutException

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.SupervisorStrategy
import akka.dispatch.Dispatchers
import akka.japi.Util.immutableSeq
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Helpers.ConfigOps
import akka.util.Timeout

/**
 * Broadcasts the message to all routees, and replies with the first response.
 *
 * @param within expecting at least one reply within this duration, otherwise
 *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
 */
@SerialVersionUID(1L)
final case class ScatterGatherFirstCompletedRoutingLogic(within: FiniteDuration) extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    ScatterGatherFirstCompletedRoutees(routees, within)
}

/**
 * INTERNAL API
 */
@SerialVersionUID(1L)
private[akka] final case class ScatterGatherFirstCompletedRoutees(
    routees: immutable.IndexedSeq[Routee],
    within: FiniteDuration)
    extends Routee {

  override def send(message: Any, sender: ActorRef): Unit = {
    implicit val ec = ExecutionContext.parasitic
    if (routees.isEmpty) {
      val reply = Future.failed(new TimeoutException("Timeout due to no routees"))
      reply.pipeTo(sender)
    } else {
      implicit val timeout = Timeout(within)
      val promise = Promise[Any]()
      routees.foreach {
        case ActorRefRoutee(ref) =>
          promise.completeWith(ref.ask(message))
        case ActorSelectionRoutee(sel) =>
          promise.completeWith(sel.ask(message))
        case _ =>
      }

      promise.future.pipeTo(sender)
    }
  }
}

/**
 * A router pool that broadcasts the message to all routees, and replies with the first response.
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
 * @param resizer optional resizer that dynamically adjust the pool size
 *
 * @param within expecting at least one reply within this duration, otherwise
 *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
 *
 * @param supervisorStrategy strategy for supervising the routees, see 'Supervision Setup'
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   supervision, death watch and router management messages
 */
@SerialVersionUID(1L)
final case class ScatterGatherFirstCompletedPool(
    nrOfInstances: Int,
    override val resizer: Option[Resizer] = None,
    within: FiniteDuration,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
    override val usePoolDispatcher: Boolean = false)
    extends Pool
    with PoolOverrideUnsetConfig[ScatterGatherFirstCompletedPool] {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      within = config.getMillisDuration("within"),
      resizer = Resizer.fromConfig(config),
      usePoolDispatcher = config.hasPath("pool-dispatcher"))

  /**
   * Java API
   * @param nr initial number of routees in the pool
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   */
  def this(nr: Int, within: FiniteDuration) = this(nrOfInstances = nr, within = within)

  /**
   * Java API
   * @param nr initial number of routees in the pool
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   */
  def this(nr: Int, within: java.time.Duration) = this(nr, within.toScala)

  override def createRouter(system: ActorSystem): Router = new Router(ScatterGatherFirstCompletedRoutingLogic(within))

  override def nrOfInstances(sys: ActorSystem) = this.nrOfInstances

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): ScatterGatherFirstCompletedPool =
    copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): ScatterGatherFirstCompletedPool = copy(resizer = Some(resizer))

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): ScatterGatherFirstCompletedPool = copy(routerDispatcher = dispatcherId)

  /**
   * Uses the resizer and/or the supervisor strategy of the given RouterConfig
   * if this RouterConfig doesn't have one, i.e. the resizer defined in code is used if
   * resizer was not defined in config.
   */
  override def withFallback(other: RouterConfig): RouterConfig = this.overrideUnsetConfig(other)

}

/**
 * A router group that broadcasts the message to all routees, and replies with the first response.
 *
 * The configuration parameter trumps the constructor arguments. This means that
 * if you provide `paths` during instantiation they will be ignored if
 * the router is defined in the configuration file for the actor being used.
 *
 * @param paths string representation of the actor paths of the routees, messages are
 *   sent with [[akka.actor.ActorSelection]] to these paths
 *
 * @param within expecting at least one reply within this duration, otherwise
 *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   router management messages
 */
@SerialVersionUID(1L)
final case class ScatterGatherFirstCompletedGroup(
    paths: immutable.Iterable[String],
    within: FiniteDuration,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
    extends Group {

  def this(config: Config) =
    this(paths = immutableSeq(config.getStringList("routees.paths")), within = config.getMillisDuration("within"))

  /**
   * Java API
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *   sent with [[akka.actor.ActorSelection]] to these paths
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   */
  def this(routeePaths: java.lang.Iterable[String], within: FiniteDuration) =
    this(paths = immutableSeq(routeePaths), within = within)

  /**
   * Java API
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *   sent with [[akka.actor.ActorSelection]] to these paths
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   */
  def this(routeePaths: java.lang.Iterable[String], within: java.time.Duration) =
    this(immutableSeq(routeePaths), within.toScala)

  override def paths(system: ActorSystem): immutable.Iterable[String] = this.paths

  override def createRouter(system: ActorSystem): Router = new Router(ScatterGatherFirstCompletedRoutingLogic(within))

  /**
   * Setting the dispatcher to be used for the router head actor, which handles
   * router management messages
   */
  def withDispatcher(dispatcherId: String): ScatterGatherFirstCompletedGroup = copy(routerDispatcher = dispatcherId)

}
