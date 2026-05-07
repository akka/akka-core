/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.collection.immutable
import scala.concurrent.Future

import akka.actor.typed.ActorRef
import akka.annotation.InternalApi
import akka.persistence.typed.javadsl
import akka.persistence.typed.scaladsl
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventWithMetadata

/** INTERNAL API */
@InternalApi
private[akka] abstract class EffectImpl[+Event, State]
    extends javadsl.EffectBuilder[Event, State]
    with javadsl.ReplyEffect[Event, State]
    with scaladsl.ReplyEffect[Event, State]
    with scaladsl.EffectBuilder[Event, State] {
  /* All events that will be persisted in this effect */
  override def events: immutable.Seq[Event] = Nil

  override def thenRun(chainedEffect: State => Unit): EffectImpl[Event, State] =
    CompositeEffect(this, new Callback[State](chainedEffect))

  override def thenReply[ReplyMessage](replyTo: ActorRef[ReplyMessage])(
      replyWithMessage: State => ReplyMessage): EffectImpl[Event, State] =
    CompositeEffect(this, new ReplyEffectImpl[ReplyMessage, State](replyTo, replyWithMessage))

  override def thenUnstashAll(): EffectImpl[Event, State] =
    CompositeEffect(this, UnstashAll.asInstanceOf[SideEffect[State]])

  override def thenNoReply(): EffectImpl[Event, State] =
    CompositeEffect(this, new NoReplyEffectImpl[State])

  override def thenStop(): EffectImpl[Event, State] =
    CompositeEffect(this, Stop.asInstanceOf[SideEffect[State]])

}

/** INTERNAL API */
@InternalApi
private[akka] object CompositeEffect {
  def apply[Event, State](
      effect: scaladsl.EffectBuilder[Event, State],
      sideEffects: SideEffect[State]): CompositeEffect[Event, State] =
    CompositeEffect[Event, State](effect, sideEffects :: Nil)
}

/** INTERNAL API */
@InternalApi
private[akka] final case class CompositeEffect[Event, State](
    persistingEffect: scaladsl.EffectBuilder[Event, State],
    _sideEffects: immutable.Seq[SideEffect[State]])
    extends EffectImpl[Event, State] {

  override val events: immutable.Seq[Event] = persistingEffect.events

  override def toString: String =
    s"CompositeEffect($persistingEffect, sideEffects: ${_sideEffects.size})"
}

/** INTERNAL API */
@InternalApi
private[akka] case object PersistNothing extends EffectImpl[Nothing, Nothing]

/** INTERNAL API */
@InternalApi
private[akka] final case class Persist[Event, State](event: Event, metadataEntries: Seq[Any])
    extends EffectImpl[Event, State] {
  override def events = event :: Nil

  override def toString: String = s"Persist(${event.getClass.getName})"
}

/** INTERNAL API */
@InternalApi
private[akka] final case class PersistAll[Event, State](eventsWithMetadata: immutable.Seq[EventWithMetadata[Event]])
    extends EffectImpl[Event, State] {

  override def events: immutable.Seq[Event] =
    eventsWithMetadata.map(_.event)

  override def toString: String = s"PersistAll(${events.map(_.getClass.getName).mkString(",")})"
}

/** INTERNAL API */
@InternalApi
private[akka] final case class AsyncEffect[Event, State](effect: Future[Effect[Event, State]])
    extends EffectImpl[Event, State]

/** INTERNAL API */
@InternalApi
private[akka] case object Unhandled extends EffectImpl[Nothing, Nothing]

/** INTERNAL API */
@InternalApi
private[akka] case object Stash extends EffectImpl[Nothing, Nothing]
