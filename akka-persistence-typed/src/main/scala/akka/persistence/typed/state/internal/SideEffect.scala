/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import akka.actor.typed.ActorRef
import akka.annotation.InternalApi

/**
 * A [[SideEffect]] is an side effect that can be chained after a main effect.
 *
 * Persist, none and unhandled are main effects. Then any number of
 * call backs can be added to these effects.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] sealed abstract class SideEffect[State]

/** INTERNAL API */
@InternalApi
private[akka] class Callback[State](val sideEffect: State => Unit) extends SideEffect[State] {
  override def toString: String = "Callback"
}

/** INTERNAL API */
@InternalApi
final private[akka] class ReplyEffectImpl[ReplyMessage, State](
    replyTo: ActorRef[ReplyMessage],
    replyWithMessage: State => ReplyMessage)
    extends Callback[State](state => replyTo ! replyWithMessage(state)) {
  override def toString: String = "Reply"
}

/** INTERNAL API */
@InternalApi
final private[akka] class NoReplyEffectImpl[State] extends Callback[State](_ => ()) {
  override def toString: String = "NoReply"
}

/** INTERNAL API */
@InternalApi
private[akka] case object Stop extends SideEffect[Nothing]

/** INTERNAL API */
@InternalApi
private[akka] case object UnstashAll extends SideEffect[Nothing]

/** INTERNAL API */
@InternalApi
private[akka] object SideEffect {

  def apply[State](callback: State => Unit): SideEffect[State] =
    new Callback(callback)

  def unstashAll[State](): SideEffect[State] = UnstashAll.asInstanceOf[SideEffect[State]]
}
