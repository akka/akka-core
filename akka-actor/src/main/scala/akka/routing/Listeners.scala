/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.{ Set, TreeSet }

import akka.actor.{ Actor, ActorRef }

sealed trait ListenerMessage
final case class Listen(listener: ActorRef) extends ListenerMessage
final case class Deafen(listener: ActorRef) extends ListenerMessage
final case class WithListeners(f: (ActorRef) => Unit) extends ListenerMessage

/**
 * Listeners is a generic trait to implement listening capability on an Actor.
 * <p/>
 * Use the <code>gossip(msg)</code> method to have it sent to the listeners.
 * <p/>
 * Send <code>Listen(self)</code> to start listening.
 * <p/>
 * Send <code>Deafen(self)</code> to stop listening.
 * <p/>
 * Send <code>WithListeners(fun)</code> to traverse the current listeners.
 */
trait Listeners { self: Actor =>
  protected val listeners: Set[ActorRef] = new TreeSet[ActorRef]

  /**
   * Chain this into the receive function.
   *
   * {{{ def receive = listenerManagement orElse … }}}
   */
  protected def listenerManagement: Actor.Receive = {
    case Listen(l) => listeners.add(l)
    case Deafen(l) => listeners.remove(l)
    case WithListeners(f) =>
      val i = listeners.iterator
      while (i.hasNext) f(i.next)
  }

  /**
   * Sends the supplied message to all current listeners using the provided sender() as sender.
   */
  protected def gossip(msg: Any)(implicit sender: ActorRef = Actor.noSender): Unit = {
    val i = listeners.iterator
    while (i.hasNext) i.next ! msg
  }
}
