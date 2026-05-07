/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.AddressTerminated
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider

/**
 * INTERNAL API
 *
 * Watchers of remote actor references register themselves as subscribers
 * of [[akka.actor.AddressTerminated]] notifications. Remote and cluster
 * death watch publish `AddressTerminated` when a remote system is deemed
 * dead.
 */
private[akka] object AddressTerminatedTopic extends ExtensionId[AddressTerminatedTopic] with ExtensionIdProvider {
  override def get(system: ActorSystem): AddressTerminatedTopic = super.get(system)
  override def get(system: ClassicActorSystemProvider): AddressTerminatedTopic = super.get(system)

  override def lookup = AddressTerminatedTopic

  override def createExtension(system: ExtendedActorSystem): AddressTerminatedTopic =
    new AddressTerminatedTopic
}

/**
 * INTERNAL API
 */
private[akka] final class AddressTerminatedTopic extends Extension {

  private val subscribers = new AtomicReference[Set[ActorRef]](Set.empty[ActorRef])

  @tailrec def subscribe(subscriber: ActorRef): Unit = {
    val current = subscribers.get
    if (!subscribers.compareAndSet(current, current + subscriber))
      subscribe(subscriber) // retry
  }

  @tailrec def unsubscribe(subscriber: ActorRef): Unit = {
    val current = subscribers.get
    if (!subscribers.compareAndSet(current, current - subscriber))
      unsubscribe(subscriber) // retry
  }

  def publish(msg: AddressTerminated): Unit = {
    subscribers.get.foreach { _.tell(msg, ActorRef.noSender) }
  }

}
