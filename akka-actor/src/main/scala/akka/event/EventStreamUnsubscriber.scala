/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.dispatch.Dispatchers
import akka.event.Logging.simpleName

/**
 * INTERNAL API
 *
 * Watches all actors which subscribe on the given eventStream, and unsubscribes them from it when they are Terminated.
 *
 * Assumptions note:
 * We do not guarantee happens-before in the EventStream when 2 threads subscribe(a) / unsubscribe(a) on the same actor,
 * thus the messages sent to this actor may appear to be reordered - this is fine, because the worst-case is starting to
 * needlessly watch the actor which will not cause trouble for the stream. This is a trade-off between slowing down
 * subscribe calls * because of the need of linearizing the history message sequence and the possibility of sometimes
 * watching a few actors too much - we opt for the 2nd choice here.
 */
protected[akka] class EventStreamUnsubscriber(eventStream: EventStream, debug: Boolean = false) extends Actor {

  import EventStreamUnsubscriber._

  def receive = {
    case Register(actor) =>
      if (debug)
        eventStream.publish(
          Logging.Debug(
            simpleName(getClass),
            getClass,
            s"watching $actor in order to unsubscribe from EventStream when it terminates"))
      context.watch(actor)

    case UnregisterIfNoMoreSubscribedChannels(actor) if eventStream.hasSubscriptions(actor) =>
    // do nothing
    // hasSubscriptions can be slow, but it's better for this actor to take the hit than the EventStream

    case UnregisterIfNoMoreSubscribedChannels(actor) =>
      if (debug)
        eventStream.publish(
          Logging.Debug(simpleName(getClass), getClass, s"unwatching $actor, since has no subscriptions"))
      context.unwatch(actor)

    case Terminated(actor) =>
      if (debug)
        eventStream.publish(
          Logging
            .Debug(simpleName(getClass), getClass, s"unsubscribe $actor from $eventStream, because it was terminated"))
      eventStream.unsubscribe(actor)
  }
}

/**
 * INTERNAL API
 *
 * Provides factory for [[akka.event.EventStreamUnsubscriber]] actors with **unique names**.
 * This is needed if someone spins up more [[EventStream]]s using the same [[akka.actor.ActorSystem]],
 * each stream gets it's own unsubscriber.
 */
private[akka] object EventStreamUnsubscriber {

  private val unsubscribersCount = new AtomicInteger(0)

  final case class Register(actor: ActorRef)

  final case class UnregisterIfNoMoreSubscribedChannels(actor: ActorRef)

  private def props(eventStream: EventStream, debug: Boolean) =
    Props(new EventStreamUnsubscriber(eventStream, debug)).withDispatcher(Dispatchers.InternalDispatcherId)

  def start(system: ActorSystem, stream: EventStream) = {
    val debug = system.settings.config.getBoolean("akka.actor.debug.event-stream")
    val unsubscriber = system
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(props(stream, debug), "eventStreamUnsubscriber-" + unsubscribersCount.incrementAndGet())
    if (debug)
      stream.publish(Logging.Debug(simpleName(getClass), getClass, s"registering unsubscriber with $stream"))
    stream.initUnsubscriber(unsubscriber)
    unsubscriber
  }

}
