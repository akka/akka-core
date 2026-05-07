/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import java.util.Collections
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorRef
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.japi.function
import akka.persistence.typed.internal._
import akka.persistence.typed.internal.SideEffect
import akka.persistence.typed.scaladsl
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import akka.persistence.CompositeMetadata

/**
 * INTERNAL API: see `class EffectFactories`
 */
@InternalApi private[akka] object EffectFactories extends EffectFactories[Nothing, Nothing]

/**
 * Factory methods for creating [[Effect]] directives - how an event sourced actor reacts on a command.
 * Created via [[EventSourcedBehavior.Effect]].
 *
 * Not for user extension
 */
@DoNotInherit sealed class EffectFactories[Event, State] {

  /**
   * Persist a single event.
   */
  final def persist(event: Event): EffectBuilder[Event, State] = Persist(event, Nil)

  /**
   * Persist multiple events. If `callback` is added through [[EffectBuilder.thenRun]] that will invoked
   * after all the events has been persisted.
   */
  final def persist(events: java.util.List[Event]): EffectBuilder[Event, State] =
    PersistAll(events.asScala.iterator.map(scaladsl.EventWithMetadata(_, Nil)).toVector)

  /**
   * Persist a single event and additional metadata together with the event.
   */
  final def persistWithMetadata(eventWithMetadata: EventWithMetadata[Event]): EffectBuilder[Event, State] =
    Persist(eventWithMetadata.event, eventWithMetadata.metadataEntries.asScala.toList)

  /**
   * Persist multiple events and additional metadata together with the events.
   * If `callback` is added through [[EffectBuilder.thenRun]] that will invoked
   * after all the events has been persisted.
   */
  final def persistWithMetadata(
      eventsWithMetadata: java.util.List[EventWithMetadata[Event]]): EffectBuilder[Event, State] =
    PersistAll(
      eventsWithMetadata.asScala.iterator.map(a => scaladsl.EventWithMetadata(a.event, a.metadataEntries)).toVector)

  /**
   * Do not persist anything
   */
  def none(): EffectBuilder[Event, State] = PersistNothing.asInstanceOf[EffectBuilder[Event, State]]

  /**
   * Stop this persistent actor
   */
  def stop(): EffectBuilder[Event, State] = none().thenStop()

  /**
   * This command is not handled, but it is not an error that it isn't.
   */
  def unhandled(): EffectBuilder[Event, State] = Unhandled.asInstanceOf[EffectBuilder[Event, State]]

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
  def stash(): ReplyEffect[Event, State] =
    Stash.asInstanceOf[EffectBuilder[Event, State]].thenNoReply()

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   *
   * @see [[EffectBuilder.thenUnstashAll]]
   */
  def unstashAll(): Effect[Event, State] =
    none().thenUnstashAll()

  /**
   * Send a reply message to the command. The type of the
   * reply message must conform to the type specified by the passed replyTo `ActorRef`.
   *
   * This has the same semantics as `replyTo.tell`.
   *
   * It is provided as a convenience (reducing boilerplate) and a way to enforce that replies are not forgotten
   * when the `EventSourcedBehavior` is created with [[EventSourcedBehaviorWithEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def reply[ReplyMessage](replyTo: ActorRef[ReplyMessage], replyWithMessage: ReplyMessage): ReplyEffect[Event, State] =
    none().thenReply[ReplyMessage](replyTo, new function.Function[State, ReplyMessage] {
      override def apply(param: State): ReplyMessage = replyWithMessage
    })

  /**
   * When [[EventSourcedBehaviorWithEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `noReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def noReply(): ReplyEffect[Event, State] =
    none().thenNoReply()

  /**
   * Asynchronous command handling. The effect is run when the `CompletionStage` has been completed.
   * Any incoming commands are stashed and processed later, after current command, when the `CompletionStage` has
   * been completed.
   *
   * This can for example be used for retrieval of external information before validating the command.
   */
  def async(effect: CompletionStage[Effect[Event, State]]): Effect[Event, State] = {
    import scala.jdk.FutureConverters._
    AsyncEffect[Event, State](effect.asScala.map(_.asInstanceOf[EffectImpl[Event, State]])(ExecutionContext.parasitic))
  }

  /**
   * Same as [[EffectFactories.async]] when the `EventSourcedBehavior` is created with
   * [[EventSourcedBehaviorWithEnforcedReplies]].
   */
  def asyncReply(effect: CompletionStage[ReplyEffect[Event, State]]): ReplyEffect[Event, State] = {
    import scala.jdk.FutureConverters._
    AsyncEffect[Event, State](effect.asScala.map(_.asInstanceOf[EffectImpl[Event, State]])(ExecutionContext.parasitic))
  }
}

/**
 * A command handler returns an `Effect` directive that defines what event or events to persist.
 *
 * Instances of `Effect` are available through factories [[EventSourcedBehavior.Effect]].
 *
 * Not intended for user extension.
 */
@DoNotInherit trait Effect[+Event, State] {
  self: EffectImpl[Event, State] =>
}

/**
 * A command handler returns an `Effect` directive that defines what event or events to persist.
 *
 * Additional side effects can be performed in the callback `thenRun`
 *
 * Instances of `Effect` are available through factories [[EventSourcedBehavior.Effect]].
 *
 * Not intended for user extension.
 */
@DoNotInherit abstract class EffectBuilder[+Event, State] extends Effect[Event, State] {
  self: EffectImpl[Event, State] =>

  /**
   * Run the given callback. Callbacks are run sequentially.
   *
   * @tparam NewState The type of the state after the event is persisted, when not specified will be the same as `State`
   *                  but if a known subtype of `State` is expected that can be specified instead (preferably by
   *                  explicitly typing the lambda parameter like so: `thenRun((SubState state) -> { ... })`).
   *                  If the state is not of the expected type an [[java.lang.ClassCastException]] is thrown.
   *
   */
  final def thenRun[NewState <: State](callback: function.Procedure[NewState]): EffectBuilder[Event, State] =
    CompositeEffect(this, SideEffect[State](s => callback.apply(s.asInstanceOf[NewState])))

  /**
   * Run the given callback. Callbacks are run sequentially.
   */
  final def thenRun(callback: function.Effect): EffectBuilder[Event, State] =
    CompositeEffect(this, SideEffect[State]((_: State) => callback.apply()))

  /** The side effect is to stop the actor */
  def thenStop(): EffectBuilder[Event, State]

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
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
   * when the `EventSourcedBehavior` is created with [[EventSourcedBehaviorWithEnforcedReplies]]. When
   * `withEnforcedReplies` is used there will be compilation errors if the returned effect isn't a [[ReplyEffect]].
   * The reply message will be sent also if `withEnforcedReplies` isn't used, but then the compiler will not help
   * finding mistakes.
   */
  def thenReply[ReplyMessage](
      replyTo: ActorRef[ReplyMessage],
      replyWithMessage: function.Function[State, ReplyMessage]): ReplyEffect[Event, State] =
    CompositeEffect(this, SideEffect[State](newState => replyTo ! replyWithMessage(newState)))

  /**
   * When [[EventSourcedBehaviorWithEnforcedReplies]] is used there will be compilation errors if the returned effect
   * isn't a [[ReplyEffect]]. This `thenNoReply` can be used as a conscious decision that a reply shouldn't be
   * sent for a specific command or the reply will be sent later.
   */
  def thenNoReply(): ReplyEffect[Event, State]

}

/**
 * [[EventSourcedBehaviorWithEnforcedReplies]] can be used to enforce that replies are not forgotten.
 * Then there will be compilation errors if the returned effect isn't a [[ReplyEffect]], which can be
 * created with `Effects().reply`, `Effects().noReply`, [[EffectBuilder.thenReply]], or [[EffectBuilder.thenNoReply]].
 */
@DoNotInherit trait ReplyEffect[+Event, State] extends Effect[Event, State] {
  self: EffectImpl[Event, State] =>

  /**
   * Unstash the commands that were stashed with `EffectFactories.stash`.
   *
   * It's allowed to stash messages while unstashing. Those newly added
   * commands will not be processed by this `unstashAll` effect and have to be unstashed
   * by another `unstashAll`.
   */
  def thenUnstashAll(): ReplyEffect[Event, State]

  /** Stops the actor as a side effect */
  def thenStop(): ReplyEffect[Event, State]
}

object EventWithMetadata {
  def create[E](event: E, metadata: AnyRef) =
    new EventWithMetadata(event, Collections.singletonList(metadata))

  def create[E](event: E, metadataEntries: java.util.List[AnyRef]) =
    new EventWithMetadata(event, metadataEntries)
}

final class EventWithMetadata[E](val event: E, val metadataEntries: java.util.List[AnyRef]) {

  /**
   * The metadata of a given type that is associated with the event.
   */
  def getMetadata[M](metadataType: Class[M]): Optional[M] = {
    import scala.jdk.OptionConverters._
    implicit val ct: ClassTag[M] = ClassTag(metadataType)
    CompositeMetadata.extract[M](metadataEntries.asScala.toSeq).toJava
  }
}
