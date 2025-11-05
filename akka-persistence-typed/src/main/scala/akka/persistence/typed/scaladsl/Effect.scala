/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import scala.collection.{ immutable => im }

import akka.actor.typed.ActorRef
import akka.annotation.DoNotInherit
import akka.persistence.typed.internal._
import akka.persistence.typed.internal.SideEffect

/**
 * Factory methods for creating [[Effect]] directives - how an event sourced actor reacts on a command.
 */
object Effect {

  /**
   * Persist a single event
   *
   * Side effects can be chained with `thenRun`
   */
  def persist[Event, State](event: Event): EffectBuilder[Event, State] = Persist(event)

  /**
   * Persist multiple events
   *
   * Side effects can be chained with `thenRun`
   */
  def persist[Event, A <: Event, B <: Event, State](evt1: A, evt2: B, events: Event*): EffectBuilder[Event, State] =
    persist(evt1 :: evt2 :: events.toList)

  /**
   * Persist multiple events
   *
   * Side effects can be chained with `thenRun`
   */
  def persist[Event, State](events: im.Seq[Event]): EffectBuilder[Event, State] =
    PersistAll(events)

  /**
   * Do not persist anything
   *
   * Side effects can be chained with `thenRun`
   */
  def none[Event, State]: EffectBuilder[Event, State] = PersistNothing.asInstanceOf[EffectBuilder[Event, State]]

  /**
   * This command is not handled, but it is not an error that it isn't.
   *
   * Side effects can be chained with `thenRun`
   */
  def unhandled[Event, State]: EffectBuilder[Event, State] = Unhandled.asInstanceOf[EffectBuilder[Event, State]]

  /**
   * Stop this persistent actor
   * Side effects can be chained with `thenRun`
   */
  def stop[Event, State](): EffectBuilder[Event, State] =
    none.thenStop()

  /**
   * Stash the current command. Can be unstashed later with [[Effect.unstashAll]].
   *
   * Note that the stashed commands are kept in an in-memory buffer, so in case of a crash they will not be
   * processed. They will also be discarded if the actor is restarted (or stopped) due to that an exception was
   * thrown from processing a command or side effect after persisting. The stash buffer is preserved for persist
   * failures if a backoff supervisor strategy is defined with [[EventSourcedBehavior.onPersistFailure]].
   *
   * Side effects can be chained with `thenRun`
   */
  def stash[Event, State](): ReplyEffect[Event, State] =
    Stash.asInstanceOf[EffectBuilder[Event, State]].thenNoReply()

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   *
   * @see [[EffectBuilder.thenUnstashAll]]
   */
  def unstashAll[Event, State](): Effect[Event, State] =
    CompositeEffect(none.asInstanceOf[EffectBuilder[Event, State]], SideEffect.unstashAll[State]())

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `cmd.replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `EventSourcedBehavior` is created with [[EventSourcedBehavior.withEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def reply[ReplyMessage, Event, State](replyTo: ActorRef[ReplyMessage])(
      replyWithMessage: ReplyMessage): ReplyEffect[Event, State] =
    none[Event, State].thenReply[ReplyMessage](replyTo)(_ => replyWithMessage)

  /**
   * When [[EventSourcedBehavior.withEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `noReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def noReply[Event, State]: ReplyEffect[Event, State] =
    none.thenNoReply()

}

/**
 * A command handler returns an `Effect` directive that defines what event or events to persist.
 *
 * Instances are created through the factories in the [[Effect]] companion object.
 *
 * Not for user extension.
 */
@DoNotInherit
trait Effect[+Event, State]

/**
 *  A command handler returns an `Effect` directive that defines what event or events to persist.
 *
 * Instances are created through the factories in the [[Effect]] companion object.
 *
 * Additional side effects can be performed in the callback `thenRun`
 *
 * Not for user extension.
 */
@DoNotInherit
trait EffectBuilder[+Event, State] extends Effect[Event, State] {
  /* All events that will be persisted in this effect */
  def events: im.Seq[Event]

  /**
   * Run the given callback. Callbacks are run sequentially.
   */
  def thenRun(callback: State => Unit): EffectBuilder[Event, State]

  /** The side effect is to stop the actor */
  def thenStop(): EffectBuilder[Event, State]

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): Effect[Event, State]

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `EventSourcedBehavior` is created with [[EventSourcedBehavior.withEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def thenReply[ReplyMessage](replyTo: ActorRef[ReplyMessage])(
      replyWithMessage: State => ReplyMessage): ReplyEffect[Event, State]

  /**
   * When [[EventSourcedBehavior.withEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `thenNoReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def thenNoReply(): ReplyEffect[Event, State]

}

/**
 * [[EventSourcedBehavior.withEnforcedReplies]] can be used to enforce that replies are not forgotten.
 * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
 * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
 *
 * Not intended for user extension.
 */
@DoNotInherit trait ReplyEffect[+Event, State] extends Effect[Event, State] {

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): ReplyEffect[Event, State]
}
