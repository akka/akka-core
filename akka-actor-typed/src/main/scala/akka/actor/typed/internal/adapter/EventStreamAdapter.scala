/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adapter

import akka.actor.typed.Behavior
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi

/**
 * INTERNAL API
 * Encapsulates the [[akka.actor.ActorSystem.eventStream]] in a [[Behavior]]
 */
@InternalApi private[akka] object EventStreamAdapter {

  private[akka] val behavior: Behavior[EventStream.Command] =
    Behaviors.setup { ctx =>
      val eventStream = ctx.system.toClassic.eventStream
      eventStreamBehavior(eventStream)
    }

  private def eventStreamBehavior(eventStream: akka.event.EventStream): Behavior[EventStream.Command] =
    Behaviors.receiveMessage {
      case EventStream.Publish(event) =>
        eventStream.publish(event)
        Behaviors.same
      case s @ EventStream.Subscribe(subscriber) =>
        eventStream.subscribe(subscriber.toClassic, s.topic)
        Behaviors.same
      case EventStream.Unsubscribe(subscriber) =>
        eventStream.unsubscribe(subscriber.toClassic)
        Behaviors.same
    }

}
