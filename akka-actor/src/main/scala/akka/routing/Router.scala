/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.collection.immutable

import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.InternalActorRef
import akka.actor.InvalidMessageException
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.WrappedMessage
import akka.japi.Util.immutableSeq

/**
 * The interface of the routing logic that is used in a [[Router]] to select
 * destination routed messages.
 *
 * The implementation must be thread safe.
 */
trait RoutingLogic extends NoSerializationVerificationNeeded {

  /**
   * Pick the destination for a given message. Normally it picks one of the
   * passed `routees`, but in the end it is up to the implementation to
   * return whatever [[Routee]] to use for sending a specific message.
   *
   * When implemented from Java it can be good to know that
   * `routees.apply(index)` can be used to get an element
   * from the `IndexedSeq`.
   */
  def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee

}

/**
 * Abstraction of a destination for messages routed via a [[Router]].
 */
trait Routee {
  def send(message: Any, sender: ActorRef): Unit
}

/**
 * [[Routee]] that sends the messages to an [[akka.actor.ActorRef]].
 */
final case class ActorRefRoutee(ref: ActorRef) extends Routee {
  override def send(message: Any, sender: ActorRef): Unit =
    ref.tell(message, sender)
}

/**
 * [[Routee]] that sends the messages to an [[akka.actor.ActorSelection]].
 */
final case class ActorSelectionRoutee(selection: ActorSelection) extends Routee {
  override def send(message: Any, sender: ActorRef): Unit =
    selection.tell(message, sender)
}

/**
 * [[Routee]] that doesn't send the message to any routee.
 * The [[Router]] will send the message to `deadLetters` if
 * `NoRoutee` is returned from [[RoutingLogic#select]]
 */
object NoRoutee extends Routee {
  override def send(message: Any, sender: ActorRef): Unit = ()
}

/**
 * [[Routee]] that sends each message to all `routees`.
 */
final case class SeveralRoutees(routees: immutable.IndexedSeq[Routee]) extends Routee {

  /**
   * Java API
   */
  def this(rs: java.lang.Iterable[Routee]) = this(routees = immutableSeq(rs).toVector)

  /**
   * Java API
   */
  def getRoutees(): java.util.List[Routee] = {
    import scala.jdk.CollectionConverters._
    routees.asJava
  }

  override def send(message: Any, sender: ActorRef): Unit =
    routees.foreach(_.send(message, sender))
}

/**
 * For each message that is sent through the router via the [[#route]] method the
 * [[RoutingLogic]] decides to which [[Routee]] to send the message. The [[Routee]] itself
 * knows how to perform the actual sending. Normally the [[RoutingLogic]] picks one of the
 * contained `routees`, but that is up to the implementation of the [[RoutingLogic]].
 *
 * A `Router` is immutable and the [[RoutingLogic]] must be thread safe.
 */
final case class Router(logic: RoutingLogic, routees: immutable.IndexedSeq[Routee] = Vector.empty) {

  /**
   * Java API
   */
  def this(logic: RoutingLogic) = this(logic, Vector.empty)

  /**
   * Java API
   */
  def this(logic: RoutingLogic, routees: java.lang.Iterable[Routee]) = this(logic, immutableSeq(routees).toVector)

  /**
   * Send the message to the destination [[Routee]] selected by the [[RoutingLogic]].
   * If the message is a [[akka.routing.RouterEnvelope]] it will be unwrapped
   * before sent to the destinations.
   * Messages wrapped in a [[Broadcast]] envelope are always sent to all `routees`.
   */
  def route(message: Any, sender: ActorRef): Unit =
    message match {
      case akka.routing.Broadcast(msg) => SeveralRoutees(routees).send(msg, sender)
      case msg                         => send(logic.select(msg, routees), message, sender)
    }

  private def send(routee: Routee, msg: Any, sender: ActorRef): Unit = {
    if (routee == NoRoutee && sender.isInstanceOf[InternalActorRef])
      sender.asInstanceOf[InternalActorRef].provider.deadLetters.tell(unwrap(msg), sender)
    else
      routee.send(unwrap(msg), sender)
  }

  private def unwrap(msg: Any): Any = msg match {
    case env: RouterEnvelope => env.message
    case _                   => msg
  }

  /**
   * Create a new instance with the specified routees and the same [[RoutingLogic]].
   */
  def withRoutees(rs: immutable.IndexedSeq[Routee]): Router = copy(routees = rs)

  /**
   * Create a new instance with one more routee and the same [[RoutingLogic]].
   */
  def addRoutee(routee: Routee): Router = copy(routees = routees :+ routee)

  /**
   * Create a new instance with one more [[ActorRefRoutee]] for the
   * specified [[akka.actor.ActorRef]] and the same [[RoutingLogic]].
   */
  def addRoutee(ref: ActorRef): Router = addRoutee(ActorRefRoutee(ref))

  /**
   * Create a new instance with one more [[ActorSelectionRoutee]] for the
   * specified [[akka.actor.ActorSelection]] and the same [[RoutingLogic]].
   */
  def addRoutee(sel: ActorSelection): Router = addRoutee(ActorSelectionRoutee(sel))

  /**
   * Create a new instance without the specified routee.
   */
  def removeRoutee(routee: Routee): Router = copy(routees = routees.filterNot(_ == routee))

  /**
   * Create a new instance without the [[ActorRefRoutee]] for the specified
   * [[akka.actor.ActorRef]].
   */
  def removeRoutee(ref: ActorRef): Router = removeRoutee(ActorRefRoutee(ref))

  /**
   * Create a new instance without the [[ActorSelectionRoutee]] for the specified
   * [[akka.actor.ActorSelection]].
   */
  def removeRoutee(sel: ActorSelection): Router = removeRoutee(ActorSelectionRoutee(sel))

}

/**
 * Used to broadcast a message to all routees in a router; only the
 * contained message will be forwarded, i.e. the `Broadcast(...)`
 * envelope will be stripped off.
 */
@SerialVersionUID(1L)
final case class Broadcast(message: Any) extends RouterEnvelope {
  if (message == null)
    throw InvalidMessageException("[null] is not an allowed message")
}

/**
 * Only the contained message will be forwarded to the
 * destination, i.e. the envelope will be stripped off.
 */
trait RouterEnvelope extends WrappedMessage {
  def message: Any
}
