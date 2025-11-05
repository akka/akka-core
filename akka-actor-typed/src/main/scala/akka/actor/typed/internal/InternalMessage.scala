/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.WrappedMessage
import akka.annotation.InternalApi

/**
 * A marker trait for internal messages.
 */
@InternalApi private[akka] sealed trait InternalMessage

/**
 * INTERNAL API: Wrapping of messages that should be adapted by
 * adapters registered with `ActorContext.messageAdapter`.
 */
@InternalApi private[akka] final case class AdaptWithRegisteredMessageAdapter[U](msg: U) extends InternalMessage

/**
 * INTERNAL API: Wrapping of messages that should be adapted by the included
 * function. Used by `ActorContext.spawnMessageAdapter` and `ActorContext.ask` so that the function is
 * applied in the "parent" actor (for better thread safety)..
 */
@InternalApi private[akka] final case class AdaptMessage[U, T](message: U, adapter: U => T)
    extends InternalMessage
    with WrappedMessage {
  def adapt(): T = adapter(message)
}
