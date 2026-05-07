/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Promise }
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.Random

import com.typesafe.config.Config

import akka.actor._
import akka.dispatch.Dispatchers
import akka.japi.Util.immutableSeq
import akka.pattern.{ ask, pipe, AskTimeoutException }
import akka.util.Helpers.ConfigOps
import akka.util.Timeout

/**
 * As each message is sent to the router, the routees are randomly ordered. The message is sent to the
 * first routee. If no response is received before the `interval` has passed, the same message is sent
 * to the next routee. This process repeats until either a response is received from some routee, the
 * routees in the pool are exhausted, or the `within` duration has passed since the first send. If no
 * routee sends a response in time, a [[akka.actor.Status.Failure]] wrapping a [[akka.pattern.AskTimeoutException]]
 * is sent to the sender.
 *
 * The goal of this routing algorithm is to decrease tail latencies ("chop off the tail latency") in situations
 * where multiple routees can perform the same piece of work, and where a routee may occasionally respond
 * more slowly than expected. In this case, sending the same work request (also known as a "backup request")
 * to another actor results in decreased response time - because it's less probable that multiple actors
 * are under heavy load simultaneously. This technique is explained in depth in Jeff Dean's presentation on
 * <a href="https://static.googleusercontent.com/media/research.google.com/en//people/jeff/Berkeley-Latency-Mar2012.pdf">
 * Achieving Rapid Response Times in Large Online Services</a>.
 *
 * @param scheduler schedules sending messages to routees
 *
 * @param within expecting at least one reply within this duration, otherwise
 *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
 *
 * @param interval duration after which the message will be sent to the next routee
 *
 * @param context execution context used by scheduler
 */
@SerialVersionUID(1L)
final case class TailChoppingRoutingLogic(
    scheduler: Scheduler,
    within: FiniteDuration,
    interval: FiniteDuration,
    context: ExecutionContext)
    extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    if (routees.isEmpty) NoRoutee
    else TailChoppingRoutees(scheduler, routees, within, interval)(context)
  }
}

/**
 * INTERNAL API
 */
@SerialVersionUID(1L)
private[akka] final case class TailChoppingRoutees(
    scheduler: Scheduler,
    routees: immutable.IndexedSeq[Routee],
    within: FiniteDuration,
    interval: FiniteDuration)(implicit ec: ExecutionContext)
    extends Routee {

  override def send(message: Any, sender: ActorRef): Unit = {
    implicit val timeout = Timeout(within)
    val promise = Promise[Any]()
    val shuffled = Random.shuffle(routees)

    val aIdx = new AtomicInteger()
    val size = shuffled.length

    val tryWithNext = scheduler.scheduleWithFixedDelay(Duration.Zero, interval) { () =>
      val idx = aIdx.getAndIncrement
      if (idx < size) {
        shuffled(idx) match {
          case ActorRefRoutee(ref) =>
            promise.completeWith(ref.ask(message))
          case ActorSelectionRoutee(sel) =>
            promise.completeWith(sel.ask(message))
          case _ =>
        }
      }
    }

    val sendTimeout = scheduler.scheduleOnce(within)(
      promise.tryFailure(new AskTimeoutException(s"Ask timed out on [$sender] after [$within.toMillis} ms]")))

    val f = promise.future
    f.onComplete {
      case _ =>
        tryWithNext.cancel()
        sendTimeout.cancel()
    }
    f.pipeTo(sender)
  }
}

/**
 * A router pool with retry logic, intended for cases where a return message is expected in
 * response to a message sent to the routee. As each message is sent to the routing pool, the
 * routees are randomly ordered. The message is sent to the first routee. If no response is received
 * before the `interval` has passed, the same message is sent to the next routee. This process repeats
 * until either a response is received from some routee, the routees in the pool are exhausted, or
 * the `within` duration has passed since the first send. If no routee sends
 * a response in time, a [[akka.actor.Status.Failure]] wrapping a [[akka.pattern.AskTimeoutException]]
 * is sent to the sender.
 *
 * Refer to [[akka.routing.TailChoppingRoutingLogic]] for comments regarding the goal of this
 * routing algorithm.
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
 * @param interval duration after which the message will be sent to the next routee
 *
 * @param supervisorStrategy strategy for supervising the routees, see 'Supervision Setup'
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   supervision, death watch and router management messages
 */
@SerialVersionUID(1L)
final case class TailChoppingPool(
    nrOfInstances: Int,
    override val resizer: Option[Resizer] = None,
    within: FiniteDuration,
    interval: FiniteDuration,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
    override val usePoolDispatcher: Boolean = false)
    extends Pool
    with PoolOverrideUnsetConfig[TailChoppingPool] {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      within = config.getMillisDuration("within"),
      interval = config.getMillisDuration("tail-chopping-router.interval"),
      resizer = Resizer.fromConfig(config),
      usePoolDispatcher = config.hasPath("pool-dispatcher"))

  /**
   * Java API
   * @param nr initial number of routees in the pool
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   * @param interval duration after which next routee will be picked
   */
  def this(nr: Int, within: FiniteDuration, interval: FiniteDuration) =
    this(nrOfInstances = nr, within = within, interval = interval)

  /**
   * Java API
   * @param nr initial number of routees in the pool
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   * @param interval duration after which next routee will be picked
   */
  def this(nr: Int, within: java.time.Duration, interval: java.time.Duration) =
    this(nr, within.toScala, interval.toScala)

  override def createRouter(system: ActorSystem): Router =
    new Router(
      TailChoppingRoutingLogic(system.scheduler, within, interval, system.dispatchers.lookup(routerDispatcher)))

  override def nrOfInstances(sys: ActorSystem) = this.nrOfInstances

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): TailChoppingPool = copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): TailChoppingPool = copy(resizer = Some(resizer))

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): TailChoppingPool = copy(routerDispatcher = dispatcherId)

  /**
   * Uses the resizer and/or the supervisor strategy of the given RouterConfig
   * if this RouterConfig doesn't have one, i.e. the resizer defined in code is used if
   * resizer was not defined in config.
   */
  override def withFallback(other: RouterConfig): RouterConfig = this.overrideUnsetConfig(other)

}

/**
 * A router group with retry logic, intended for cases where a return message is expected in
 * response to a message sent to the routee. As each message is sent to the routing group, the
 * routees are randomly ordered. The message is sent to the first routee. If no response is received
 * before the `interval` has passed, the same message is sent to the next routee. This process repeats
 * until either a response is received from some routee, the routees in the group are exhausted, or
 * the `within` duration has passed since the first send. If no routee sends
 * a response in time, a [[akka.actor.Status.Failure]] wrapping a [[akka.pattern.AskTimeoutException]]
 * is sent to the sender.
 *
 * Refer to [[akka.routing.TailChoppingRoutingLogic]] for comments regarding the goal of this
 * routing algorithm.
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
 * @param interval duration after which the message will be sent to the next routee
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   router management messages
 */
final case class TailChoppingGroup(
    paths: immutable.Iterable[String],
    within: FiniteDuration,
    interval: FiniteDuration,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
    extends Group {

  def this(config: Config) =
    this(
      paths = immutableSeq(config.getStringList("routees.paths")),
      within = config.getMillisDuration("within"),
      interval = config.getMillisDuration("tail-chopping-router.interval"))

  /**
   * Java API
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *   sent with [[akka.actor.ActorSelection]] to these paths
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   * @param interval duration after which next routee will be picked
   */
  def this(routeePaths: java.lang.Iterable[String], within: FiniteDuration, interval: FiniteDuration) =
    this(paths = immutableSeq(routeePaths), within = within, interval = interval)

  /**
   * Java API
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *   sent with [[akka.actor.ActorSelection]] to these paths
   * @param within expecting at least one reply within this duration, otherwise
   *   it will reply with [[akka.pattern.AskTimeoutException]] in a [[akka.actor.Status.Failure]]
   * @param interval duration after which next routee will be picked
   */
  def this(routeePaths: java.lang.Iterable[String], within: java.time.Duration, interval: java.time.Duration) =
    this(immutableSeq(routeePaths), within.toScala, interval.toScala)

  override def createRouter(system: ActorSystem): Router =
    new Router(
      TailChoppingRoutingLogic(system.scheduler, within, interval, system.dispatchers.lookup(routerDispatcher)))

  override def paths(system: ActorSystem): immutable.Iterable[String] = this.paths

  /**
   * Setting the dispatcher to be used for the router head actor, which handles
   * router management messages
   */
  def withDispatcher(dispatcherId: String): TailChoppingGroup = copy(routerDispatcher = dispatcherId)

}
