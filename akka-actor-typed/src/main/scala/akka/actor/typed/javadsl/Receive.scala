/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import akka.actor.typed.Behavior
import akka.actor.typed.ExtensibleBehavior
import akka.actor.typed.Signal
import akka.actor.typed.TypedActorContext
import akka.annotation.DoNotInherit

/**
 * A specialized "receive" behavior that is implemented using message matching builders,
 * such as [[ReceiveBuilder]], from [[AbstractBehavior]].
 */
@DoNotInherit
abstract class Receive[T] extends ExtensibleBehavior[T] {
  // Note that this class may be opened for extensions to support other builder patterns,
  // or message dispatch without builders.

  /**
   * Process an incoming message and return the next behavior.
   *
   * The returned behavior can in addition to normal behaviors be one of the
   * canned special objects:
   *
   *  * returning `stopped` will terminate this Behavior
   *  * returning `same` designates to reuse the current Behavior
   *  * returning `unhandled` keeps the same Behavior and signals that the message was not yet handled
   *
   */
  @throws(classOf[Exception])
  def receiveMessage(msg: T): Behavior[T]

  /**
   * Process an incoming [[akka.actor.typed.Signal]] and return the next behavior. This means
   * that all lifecycle hooks, ReceiveTimeout, Terminated and Failed messages
   * can initiate a behavior change.
   *
   * The returned behavior can in addition to normal behaviors be one of the
   * canned special objects:
   *
   *  * returning `stopped` will terminate this Behavior
   *  * returning `same` designates to reuse the current Behavior
   *  * returning `unhandled` keeps the same Behavior and signals that the message was not yet handled
   */
  @throws(classOf[Exception])
  def receiveSignal(sig: Signal): Behavior[T]

  @throws(classOf[Exception])
  override final def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] =
    receiveMessage(msg)

  @throws(classOf[Exception])
  override final def receiveSignal(ctx: TypedActorContext[T], sig: Signal): Behavior[T] =
    receiveSignal(sig)

}
