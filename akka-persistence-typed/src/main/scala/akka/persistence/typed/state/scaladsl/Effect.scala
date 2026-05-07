/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import akka.actor.typed.ActorRef
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.persistence.typed.state.internal._
import akka.persistence.typed.state.internal.SideEffect

/**
 * Factory methods for creating [[Effect]] directives - how a `DurableStateBehavior` reacts on a command.
 *
 * API May Change
 */
@ApiMayChange
object Effect {

  /**
   * Persist new state.
   *
   * Side effects can be chained with `thenRun`
   */
  def persist[State](state: State): EffectBuilder[State] = Persist(state)

  /**
   * Delete the persisted state.
   *
   * Side effects can be chained with `thenRun`
   */
  def delete[State](): EffectBuilder[State] = Delete()

  /**
   * Do not persist anything
   *
   * Side effects can be chained with `thenRun`
   */
  def none[State]: EffectBuilder[State] = PersistNothing.asInstanceOf[EffectBuilder[State]]

  /**
   * This command is not handled, but it is not an error that it isn't.
   *
   * Side effects can be chained with `thenRun`
   */
  def unhandled[State]: EffectBuilder[State] = Unhandled.asInstanceOf[EffectBuilder[State]]

  /**
   * Stop this persistent actor
   * Side effects can be chained with `thenRun`
   */
  def stop[State](): EffectBuilder[State] =
    none.thenStop()

  /**
   * Stash the current command. Can be unstashed later with [[Effect.unstashAll]].
   *
   * Note that the stashed commands are kept in an in-memory buffer, so in case of a crash they will not be
   * processed. They will also be discarded if the actor is restarted (or stopped) due to that an exception was
   * thrown from processing a command or side effect after persisting. The stash buffer is preserved for persist
   * failures if a backoff supervisor strategy is defined with [[DurableStateBehavior.onPersistFailure]].
   *
   * Side effects can be chained with `thenRun`
   */
  def stash[State](): ReplyEffect[State] =
    Stash.asInstanceOf[EffectBuilder[State]].thenNoReply()

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   *
   * @see [[EffectBuilder.thenUnstashAll]]
   */
  def unstashAll[State](): Effect[State] =
    CompositeEffect(none.asInstanceOf[EffectBuilder[State]], SideEffect.unstashAll[State]())

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `cmd.replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `DurableStateBehavior` is created with [[DurableStateBehavior.withEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def reply[ReplyMessage, State](replyTo: ActorRef[ReplyMessage])(replyWithMessage: ReplyMessage): ReplyEffect[State] =
    none[State].thenReply[ReplyMessage](replyTo)(_ => replyWithMessage)

  /**
   * When [[DurableStateBehavior.withEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `noReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def noReply[State]: ReplyEffect[State] =
    none.thenNoReply()

}

/**
 * A command handler returns an `Effect` directive that defines what state to persist.
 *
 * Instances are created through the factories in the [[Effect]] companion object.
 *
 * Not for user extension.
 */
@DoNotInherit
trait Effect[+State]

/**
 *  A command handler returns an `Effect` directive that defines what state to persist.
 *
 * Instances are created through the factories in the [[Effect]] companion object.
 *
 * Additional side effects can be performed in the callback `thenRun`
 *
 * Not for user extension.
 */
@DoNotInherit
trait EffectBuilder[+State] extends Effect[State] {
  /* The state that will be persisted in this effect */
  def state: Option[State]

  /**
   * Run the given callback. Callbacks are run sequentially.
   */
  def thenRun(callback: State => Unit): EffectBuilder[State]

  /** The side effect is to stop the actor */
  def thenStop(): EffectBuilder[State]

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): Effect[State]

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `DurableStateBehavior` is created with [[DurableStateBehavior.withEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def thenReply[ReplyMessage](replyTo: ActorRef[ReplyMessage])(
      replyWithMessage: State => ReplyMessage): ReplyEffect[State]

  /**
   * When [[DurableStateBehavior.withEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `thenNoReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def thenNoReply(): ReplyEffect[State]

}

/**
 * [[DurableStateBehavior.withEnforcedReplies]] can be used to enforce that replies are not forgotten.
 * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
 * created with [[Effect.reply]], [[Effect.noReply]], [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
 *
 * Not intended for user extension.
 */
@DoNotInherit trait ReplyEffect[+State] extends Effect[State] {

  /**
   * Unstash the commands that were stashed with [[Effect.stash]].
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): ReplyEffect[State]

  /** Stops the actor as a side effect */
  def thenStop(): ReplyEffect[State]
}
