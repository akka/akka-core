/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import akka.actor.typed.{ Behavior, ExtensibleBehavior, Signal, TypedActorContext }
import akka.util.OptionVal

/**
 * An actor `Behavior` can be implemented by extending this class and implement the
 * abstract method [[AbstractBehavior#createReceive]]. Mutable state can be defined
 * as instance variables of the class.
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
abstract class AbstractBehavior[T](context: ActorContext[T]) extends ExtensibleBehavior[T] {

  if (context eq null)
    throw new IllegalArgumentException(
      "context must not be null. Wrap in Behaviors.setup and " +
      "pass the context to the constructor of AbstractBehavior.")

  private var _receive: OptionVal[Receive[T]] = OptionVal.None
  private def receive: Receive[T] = _receive match {
    case OptionVal.Some(r) => r
    case _ =>
      val receive = createReceive
      _receive = OptionVal.Some(receive)
      receive
  }

  protected def getContext: ActorContext[T] = context

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
    receive.receive(ctx, msg)
  }

  @throws(classOf[Exception])
  override final def receiveSignal(ctx: TypedActorContext[T], msg: Signal): Behavior[T] = {
    checkRightContext(ctx)
    receive.receiveSignal(ctx, msg)
  }

  /**
   * Implement this to define how messages and signals are processed. Use the
   * [[AbstractBehavior.newReceiveBuilder]] to define the message dispatch.
   */
  protected def createReceive: Receive[T]

  /**
   * Create a new [[ReceiveBuilder]] to define the message dispatch of the `Behavior`.
   * Typically used from [[AbstractBehavior.createReceive]].
   */
  protected def newReceiveBuilder: ReceiveBuilder[T] = ReceiveBuilder.create
}
