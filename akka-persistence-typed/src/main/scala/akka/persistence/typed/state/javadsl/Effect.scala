/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.javadsl

import akka.actor.typed.ActorRef
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.japi.function
import akka.persistence.typed.state.internal._
import akka.persistence.typed.state.internal.SideEffect

/**
 * INTERNAL API: see `class EffectFactories`
 */
@InternalApi private[akka] object EffectFactories extends EffectFactories[Nothing]

/**
 * Factory methods for creating [[Effect]] directives - how a `DurableStateBehavior` reacts on a command.
 * Created via [[DurableStateBehavior.Effect]].
 *
 * Not for user extension
 *
 * API May Change
 */
@ApiMayChange @DoNotInherit sealed class EffectFactories[State] {

  /**
   * Persist new state.
   *
   * Side effects can be chained with `thenRun`.
   */
  final def persist(state: State): EffectBuilder[State] = Persist(state)

  /**
   * Delete the persisted state.
   *
   * Side effects can be chained with `thenRun`.
   */
  def delete(): EffectBuilder[State] = Delete().asInstanceOf[EffectBuilder[State]]

  /**
   * Do not persist anything
   *
   * Side effects can be chained with `thenRun`
   */
  def none(): EffectBuilder[State] = PersistNothing.asInstanceOf[EffectBuilder[State]]

  /**
   * Stop this persistent actor
   *
   * Side effects can be chained with `thenRun`
   */
  def stop(): EffectBuilder[State] = none().thenStop()

  /**
   * This command is not handled, but it is not an error that it isn't.
   *
   * Side effects can be chained with `thenRun`
   */
  def unhandled(): EffectBuilder[State] = Unhandled.asInstanceOf[EffectBuilder[State]]

  /**
   * Stash the current command. Can be unstashed later with `Effect.thenUnstashAll`
   * or `EffectFactories.unstashAll`.
   *
   * Note that the stashed commands are kept in an in-memory buffer, so in case of a crash they will not be
   * processed. They will also be discarded if the actor is restarted (or stopped) due to that an exception was
   * thrown from processing a command or side effect after persisting. The stash buffer is preserved for persist
   * failures if an `onPersistFailure` backoff supervisor strategy is defined.
   *
   * Side effects can be chained with `thenRun`.
   */
  def stash(): ReplyEffect[State] =
    Stash.asInstanceOf[EffectBuilder[State]].thenNoReply()

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   *
   * @see [[EffectBuilder.thenUnstashAll]]
   */
  def unstashAll(): Effect[State] =
    none().thenUnstashAll()

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `DurableStateBehavior` is created with [[DurableStateBehaviorWithEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def reply[ReplyMessage](replyTo: ActorRef[ReplyMessage], replyWithMessage: ReplyMessage): ReplyEffect[State] =
    none().thenReply[ReplyMessage](replyTo, new function.Function[State, ReplyMessage] {
      override def apply(param: State): ReplyMessage = replyWithMessage
    })

  /**
   * When [[DurableStateBehaviorWithEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `noReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def noReply(): ReplyEffect[State] =
    none().thenNoReply()
}

/**
 * A command handler returns an `Effect` directive that defines what state to persist.
 *
 * Instances of `Effect` are available through factories [[DurableStateBehavior.Effect]].
 *
 * Not intended for user extension.
 */
@DoNotInherit trait Effect[State] {
  self: EffectImpl[State] =>
}

/**
 * A command handler returns an `Effect` directive that defines what state to persist.
 *
 * Additional side effects can be performed in the callback `thenRun`
 *
 * Instances of `Effect` are available through factories [[DurableStateBehavior.Effect]].
 *
 * Not intended for user extension.
 */
@DoNotInherit abstract class EffectBuilder[State] extends Effect[State] {
  self: EffectImpl[State] =>

  /**
   * Run the given callback. Callbacks are run sequentially.
   *
   * @tparam NewState The type of the state after the state is persisted, when not specified will be the same as `State`
   *                  but if a known subtype of `State` is expected that can be specified instead (preferably by
   *                  explicitly typing the lambda parameter like so: `thenRun((SubState state) -> { ... })`).
   *                  If the state is not of the expected type an [[java.lang.ClassCastException]] is thrown.
   *
   */
  final def thenRun[NewState <: State](callback: function.Procedure[NewState]): EffectBuilder[State] =
    CompositeEffect(this, SideEffect[State](s => callback.apply(s.asInstanceOf[NewState])))

  /**
   * Run the given callback. Callbacks are run sequentially.
   */
  final def thenRun(callback: function.Effect): EffectBuilder[State] =
    CompositeEffect(this, SideEffect[State]((_: State) => callback.apply()))

  /** The side effect is to stop the actor */
  def thenStop(): EffectBuilder[State]

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
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
   * when the `DurableStateBehavior` is created with [[DurableStateBehaviorWithEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def thenReply[ReplyMessage](
      replyTo: ActorRef[ReplyMessage],
      replyWithMessage: function.Function[State, ReplyMessage]): ReplyEffect[State] =
    CompositeEffect(this, SideEffect[State](newState => replyTo ! replyWithMessage(newState)))

  /**
   * When [[DurableStateBehaviorWithEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `thenNoReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def thenNoReply(): ReplyEffect[State]

}

/**
 * [[DurableStateBehaviorWithEnforcedReplies]] can be used to enforce that replies are not forgotten.
 * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
 * created with `Effects().reply`, `Effects().noReply`, [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
 */
@DoNotInherit trait ReplyEffect[State] extends Effect[State] {
  self: EffectImpl[State] =>

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): ReplyEffect[State]

  /** Stops the actor as a side effect */
  def thenStop(): ReplyEffect[State]
}
