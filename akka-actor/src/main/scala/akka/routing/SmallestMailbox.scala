/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import com.typesafe.config.Config

import akka.actor.ActorCell
import akka.actor.ActorRefWithCell
import akka.actor.ActorSystem
import akka.actor.SupervisorStrategy
import akka.dispatch.Dispatchers

object SmallestMailboxRoutingLogic {
  def apply(): SmallestMailboxRoutingLogic = new SmallestMailboxRoutingLogic
}

/**
 * Tries to send to the non-suspended routee with fewest messages in mailbox.
 * The selection is done in this order:
 * <ul>
 * <li>pick any idle routee (not processing message) with empty mailbox</li>
 * <li>pick any routee with empty mailbox</li>
 * <li>pick routee with fewest pending messages in mailbox</li>
 * <li>pick any remote routee, remote actors are consider lowest priority,
 *     since their mailbox size is unknown</li>
 * </ul>
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
class SmallestMailboxRoutingLogic extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else selectNext(routees)

  // Worst-case a 2-pass inspection with mailbox size checking done on second pass, and only until no one empty is found.
  // Lowest score wins, score 0 is autowin
  // If no actor with score 0 is found, it will return that, or if it is terminated, a random of the entire set.
  //   Why? Well, in case we had 0 viable actors and all we got was the default, which is the DeadLetters, anything else is better.
  // Order of interest, in ascending priority:
  // 1. The NoRoutee
  // 2. A Suspended ActorRef
  // 3. An ActorRef with unknown mailbox size but with one message being processed
  // 4. An ActorRef with unknown mailbox size that isn't processing anything
  // 5. An ActorRef with a known mailbox size
  // 6. An ActorRef without any messages
  @tailrec private def selectNext(
      targets: immutable.IndexedSeq[Routee],
      proposedTarget: Routee = NoRoutee,
      currentScore: Long = Long.MaxValue,
      at: Int = 0,
      deep: Boolean = false): Routee = {
    if (targets.isEmpty)
      NoRoutee
    else if (at >= targets.size) {
      if (deep) {
        if (isTerminated(proposedTarget)) targets(ThreadLocalRandom.current.nextInt(targets.size)) else proposedTarget
      } else selectNext(targets, proposedTarget, currentScore, 0, deep = true)
    } else {
      val target = targets(at)
      val newScore: Long =
        if (isSuspended(target)) Long.MaxValue - 1
        else { //Just about better than the DeadLetters
          (if (isProcessingMessage(target)) 1L else 0L) +
          (if (!hasMessages(target)) 0L
           else { //Race between hasMessages and numberOfMessages here, unfortunate the numberOfMessages returns 0 if unknown
             val noOfMsgs: Long = if (deep) numberOfMessages(target) else 0
             if (noOfMsgs > 0) noOfMsgs else Long.MaxValue - 3 //Just better than a suspended actorref
           })
        }

      if (newScore == 0) target
      else if (newScore < 0 || newScore >= currentScore) selectNext(targets, proposedTarget, currentScore, at + 1, deep)
      else selectNext(targets, target, newScore, at + 1, deep)
    }
  }

  // TODO should we rewrite this not to use isTerminated?
  protected def isTerminated(a: Routee): Boolean = a match {
    case ActorRefRoutee(ref) => ref.isTerminated
    case _                   => false
  }

  /**
   * Returns true if the actor is currently processing a message.
   * It will always return false for remote actors.
   * Method is exposed to subclasses to be able to implement custom
   * routers based on mailbox and actor internal state.
   */
  protected def isProcessingMessage(a: Routee): Boolean = a match {
    case ActorRefRoutee(x: ActorRefWithCell) =>
      x.underlying match {
        case cell: ActorCell => cell.mailbox.isScheduled && cell.currentMessage != null
        case _               => false
      }
    case _ => false
  }

  /**
   * Returns true if the actor currently has any pending messages
   * in the mailbox, i.e. the mailbox is not empty.
   * It will always return false for remote actors.
   * Method is exposed to subclasses to be able to implement custom
   * routers based on mailbox and actor internal state.
   */
  protected def hasMessages(a: Routee): Boolean = a match {
    case ActorRefRoutee(x: ActorRefWithCell) => x.underlying.hasMessages
    case _                                   => false
  }

  /**
   * Returns true if the actor is currently suspended.
   * It will always return false for remote actors.
   * Method is exposed to subclasses to be able to implement custom
   * routers based on mailbox and actor internal state.
   */
  protected def isSuspended(a: Routee): Boolean = a match {
    case ActorRefRoutee(x: ActorRefWithCell) =>
      x.underlying match {
        case cell: ActorCell => cell.mailbox.isSuspended
        case _               => true
      }
    case _ => false
  }

  /**
   * Returns the number of pending messages in the mailbox of the actor.
   * It will always return 0 for remote actors.
   * Method is exposed to subclasses to be able to implement custom
   * routers based on mailbox and actor internal state.
   */
  protected def numberOfMessages(a: Routee): Int = a match {
    case ActorRefRoutee(x: ActorRefWithCell) => x.underlying.numberOfMessages
    case _                                   => 0
  }
}

/**
 * A router pool that tries to send to the non-suspended routee with fewest messages in mailbox.
 * The selection is done in this order:
 * <ul>
 * <li>pick any idle routee (not processing message) with empty mailbox</li>
 * <li>pick any routee with empty mailbox</li>
 * <li>pick routee with fewest pending messages in mailbox</li>
 * <li>pick any remote routee, remote actors are consider lowest priority,
 *     since their mailbox size is unknown</li>
 * </ul>
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
 * @param supervisorStrategy strategy for supervising the routees, see 'Supervision Setup'
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   supervision, death watch and router management messages
 */
@SerialVersionUID(1L)
final case class SmallestMailboxPool(
    nrOfInstances: Int,
    override val resizer: Option[Resizer] = None,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
    override val usePoolDispatcher: Boolean = false)
    extends Pool
    with PoolOverrideUnsetConfig[SmallestMailboxPool] {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      resizer = Resizer.fromConfig(config),
      usePoolDispatcher = config.hasPath("pool-dispatcher"))

  /**
   * Java API
   * @param nr initial number of routees in the pool
   */
  def this(nr: Int) = this(nrOfInstances = nr)

  override def createRouter(system: ActorSystem): Router = new Router(SmallestMailboxRoutingLogic())

  override def nrOfInstances(sys: ActorSystem) = this.nrOfInstances

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): SmallestMailboxPool = copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): SmallestMailboxPool = copy(resizer = Some(resizer))

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): SmallestMailboxPool = copy(routerDispatcher = dispatcherId)

  /**
   * Uses the resizer and/or the supervisor strategy of the given RouterConfig
   * if this RouterConfig doesn't have one, i.e. the resizer defined in code is used if
   * resizer was not defined in config.
   */
  override def withFallback(other: RouterConfig): RouterConfig = this.overrideUnsetConfig(other)

}
