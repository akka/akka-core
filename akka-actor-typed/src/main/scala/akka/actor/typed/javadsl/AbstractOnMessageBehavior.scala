/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import akka.actor.typed.{ Behavior, ExtensibleBehavior, MessageAdaptionFailure, Signal, TypedActorContext }

/**
 * An actor `Behavior` can be implemented by extending this class and implementing the abstract
 * method [[AbstractOnMessageBehavior#onMessage]].  Mutable state can be defined as instance
 * variables of the class.
 *
 * This is an object-oriented style of defining a `Behavior`.  A more functional style alternative
 * is provided by the factory methods in [[Behaviors]], for example [[Behaviors.receiveMessage]].
 *
 * An alternative object-oriented style is found in [[AbstractBehavior]], which uses builders to
 * define the `Behavior`.  In contrast to extending [[AbstractBehavior]], extending this class
 * should have reduced overhead, though depending on the complexity of the protocol handled by
 * this actor and on the Java version in use, the `onMessage` and `onSignal` methods may be overly
 * complex.
 *
 * Instances of this behavior should be created via [[Behaviors.setup]] and the [[ActorContext]] should
 * be passed as a constructor parameter from the factory function.  This is important because a new
 * instance should be created when restart supervision is used.
 *
 * When switching behavior to another behavior which requires a context, the original `ActorContext` can
 * be used or a `Behaviors.setup` can be used: either will end up using the same `ActorContext` instance.
 *
 * It must not be created with an `ActorContext` of another actor (e.g. the parent actor).  Doing so
 * will be detected at runtime and throw an `IllegalStateException` when the first message is received.
 *
 * @see [[Behaviors.setup]]
 */
abstract class AbstractOnMessageBehavior[T](context: ActorContext[T]) extends ExtensibleBehavior[T] {
  if (context eq null)
    throw new IllegalArgumentException(
      "context must not be null.  Wrap in Behaviors.setup and " +
      "pass the context to the constructor of AbstractOnMessageBehavior.")

  /**
   * Implement this to define how messages are processed.  To indicate no change in behavior beyond
   * changes due to updating instance variables of this class, one may return either `this` or [[Behaviors.same]].
   */
  @throws(classOf[Exception])
  def onMessage(message: T): Behavior[T]

  /**
   * Override this to handle a signal.  The default implementation handles only `MessageAdaptionFailure` and
   * otherwise ignores the signal.
   */
  @throws(classOf[Exception])
  def onSignal(signal: Signal): Behavior[T] = {
    signal match {
      case _: MessageAdaptionFailure => Behaviors.unhandled
      case _                         => this
    }
  }

  final protected def getContext: ActorContext[T] = context

  @throws(classOf[Exception])
  override final def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] = {
    checkRightContext(ctx)
    onMessage(msg)
  }

  @throws(classOf[Exception])
  override final def receiveSignal(ctx: TypedActorContext[T], signal: Signal): Behavior[T] = {
    checkRightContext(ctx)
    onSignal(signal)
  }

  private def checkRightContext(ctx: TypedActorContext[T]): Unit =
    if (ctx.asJava ne context)
      throw new IllegalStateException(
        s"Actor [${ctx.asJava.getSelf}] of AbstractOnMessageBehavior class " +
        s"[${getClass.getName}] was created with the wrong ActorContext [${context.asJava.getSelf}]. " +
        "Wrap in Behaviors.setup and pass the context to the constructor of AbstractOnMessageBehavior.")
}
