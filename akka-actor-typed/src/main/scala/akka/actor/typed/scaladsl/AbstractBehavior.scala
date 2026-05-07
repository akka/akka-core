/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import akka.actor.typed.{ Behavior, ExtensibleBehavior, Signal, TypedActorContext }
import akka.actor.typed.MessageAdaptionFailure

/**
 * An actor `Behavior` can be implemented by extending this class and implement the
 * abstract method [[AbstractBehavior#onMessage]] and optionally override
 * [[AbstractBehavior#onSignal]]. Mutable state can be defined as instance variables
 * of the class.
 *
 * This is an Object-oriented style of defining a `Behavior`. A more functional style
 * alternative is provided by the factory methods in [[Behaviors]], for example
 * [[Behaviors.receiveMessage]].
 *
 * Instances of this behavior should be created via [[Behaviors.setup]] and
 * the [[ActorContext]] should be passed as a constructor parameter
 * from the factory function. This is important because a new instance
 * should be created when restart supervision is used.
 *
 * When switching `Behavior` to another `AbstractBehavior` the original `ActorContext`
 * can be used as the `context` parameter instead of wrapping in a new `Behaviors.setup`,
 * but it wouldn't be wrong to use `context` from `Behaviors.setup` since that is the same
 * `ActorContext` instance.
 *
 * It must not be created with an `ActorContext` of another actor, such as the parent actor.
 * Such mistake will be detected at runtime and throw `IllegalStateException` when the
 * first message is received.
 *
 * @see [[Behaviors.setup]]
 */
abstract class AbstractBehavior[T](protected val context: ActorContext[T]) extends ExtensibleBehavior[T] {

  if (context eq null)
    throw new IllegalArgumentException(
      "context must not be null. Wrap in Behaviors.setup and " +
      "pass the context to the constructor of AbstractBehavior.")

  /**
   * Implement this method to process an incoming message and return the next behavior.
   *
   * The returned behavior can in addition to normal behaviors be one of the canned special objects:
   * <ul>
   * <li>returning `stopped` will terminate this Behavior</li>
   * <li>returning `this` or `same` designates to reuse the current Behavior</li>
   * <li>returning `unhandled` keeps the same Behavior and signals that the message was not yet handled</li>
   * </ul>
   *
   */
  @throws(classOf[Exception])
  def onMessage(msg: T): Behavior[T]

  /**
   * Override this method to process an incoming [[akka.actor.typed.Signal]] and return the next behavior.
   * This means that all lifecycle hooks, ReceiveTimeout, Terminated and Failed messages
   * can initiate a behavior change.
   *
   * The returned behavior can in addition to normal behaviors be one of the canned special objects:
   *
   *  * returning `stopped` will terminate this Behavior
   *  * returning `this` or `same` designates to reuse the current Behavior
   *  * returning `unhandled` keeps the same Behavior and signals that the message was not yet handled
   *
   * By default, partial function is empty and does not handle any signals.
   */
  @throws(classOf[Exception])
  def onSignal: PartialFunction[Signal, Behavior[T]] = PartialFunction.empty

  private def checkRightContext(ctx: TypedActorContext[T]): Unit = {
    if (ctx.asJava ne context)
      throw new IllegalStateException(
        s"Actor [${ctx.asJava.getSelf}] of AbstractBehavior class " +
        s"[${getClass.getName}] was created with wrong ActorContext [${context.asJava.getSelf}]. " +
        "Wrap in Behaviors.setup and pass the context to the constructor of AbstractBehavior.")
  }

  @throws(classOf[Exception])
  override final def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] = {
    checkRightContext(ctx)
    onMessage(msg)
  }

  @throws(classOf[Exception])
  override final def receiveSignal(ctx: TypedActorContext[T], msg: Signal): Behavior[T] = {
    checkRightContext(ctx)
    onSignal.applyOrElse(msg, {
      case MessageAdaptionFailure(ex) => throw ex
      case _                          => Behaviors.unhandled
    }: PartialFunction[Signal, Behavior[T]])
  }
}
