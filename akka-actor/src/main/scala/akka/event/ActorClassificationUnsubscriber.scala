/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn

import akka.actor._
import akka.event.Logging.simpleName

/**
 * INTERNAL API
 *
 * Watches all actors which subscribe on the given event stream, and unsubscribes them from it when they are Terminated.
 */
protected[akka] class ActorClassificationUnsubscriber(bus: String, unsubscribe: ActorRef => Unit, debug: Boolean)
    extends Actor
    with Stash {

  import ActorClassificationUnsubscriber._

  private var atSeq = 0
  private def nextSeq = atSeq + 1

  override def preStart(): Unit = {
    super.preStart()
    if (debug) context.system.eventStream.publish(Logging.Debug(simpleName(getClass), getClass, s"will monitor $bus"))
  }

  def receive = {
    case Register(actor, seq) if seq == nextSeq =>
      if (debug)
        context.system.eventStream
          .publish(Logging.Debug(simpleName(getClass), getClass, s"registered watch for $actor in $bus"))
      context.watch(actor)
      atSeq = nextSeq
      unstashAll()

    case _: Register =>
      stash()

    case Unregister(actor, seq) if seq == nextSeq =>
      if (debug)
        context.system.eventStream
          .publish(Logging.Debug(simpleName(getClass), getClass, s"unregistered watch of $actor in $bus"))
      context.unwatch(actor)
      atSeq = nextSeq
      unstashAll()

    case _: Unregister =>
      stash()

    case Terminated(actor) =>
      if (debug)
        context.system.eventStream.publish(
          Logging.Debug(simpleName(getClass), getClass, s"actor $actor has terminated, unsubscribing it from $bus"))
      // the `unsubscribe` will trigger another `Unregister(actor, _)` message to this unsubscriber;
      // but since that actor is terminated, there cannot be any harm in processing an Unregister for it.
      unsubscribe(actor)
  }

}

/**
 * INTERNAL API
 *
 * Provides factory for [[akka.event.ActorClassificationUnsubscriber]] actors with **unique names**.
 */
private[akka] object ActorClassificationUnsubscriber {

  private val unsubscribersCount = new AtomicInteger(0)

  final case class Register(actor: ActorRef, seq: Int)
  final case class Unregister(actor: ActorRef, seq: Int)

  def start(
      system: ActorSystem,
      busName: String,
      unsubscribe: ActorRef => Unit,
      @nowarn("msg=never used") debug: Boolean = false): ActorRef = {
    val debug = system.settings.config.getBoolean("akka.actor.debug.event-stream")
    system
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(
        props(busName, unsubscribe, debug),
        "actorClassificationUnsubscriber-" + unsubscribersCount.incrementAndGet())
  }

  private def props(busName: String, unsubscribe: ActorRef => Unit, debug: Boolean) =
    Props(new ActorClassificationUnsubscriber(busName, unsubscribe, debug))

}
