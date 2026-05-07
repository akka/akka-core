/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.typed._
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.internal.adapter.EventStreamAdapter
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * Exposes a typed actor that interacts with the [[akka.actor.ActorSystem.eventStream]].
 *
 * It is used as an extension to ensure a single instance per actor system.
 */
@InternalApi private[akka] final class EventStreamExtension(actorSystem: ActorSystem[_]) extends Extension {
  val ref: ActorRef[EventStream.Command] =
    actorSystem.internalSystemActorOf(EventStreamAdapter.behavior, "eventstream", Props.empty)
}

private[akka] object EventStreamExtension extends ExtensionId[EventStreamExtension] {
  override def createExtension(system: ActorSystem[_]): EventStreamExtension = new EventStreamExtension(system)
}
