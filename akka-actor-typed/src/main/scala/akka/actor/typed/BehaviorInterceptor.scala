/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.reflect.ClassTag

import akka.annotation.{ DoNotInherit, InternalApi }
import akka.util.BoxedType

/**
 * A behavior interceptor allows for intercepting message and signal reception and perform arbitrary logic -
 * transform, filter, send to a side channel etc. It is the core API for decoration of behaviors.
 *
 * The `BehaviorInterceptor` API is considered a low level tool for building other features and
 * shouldn't be used for "normal" application logic. Several built-in intercepting behaviors
 * are provided through factories in the respective `Behaviors`.
 *
 * If the interceptor does keep mutable state care must be taken to create a new instance from
 * the factory function of `Behaviors.intercept` so that a new instance is created per spawned
 * actor rather than shared among actor instance.
 *
 * @param interceptMessageClass Ensures that the interceptor will only receive `O` message types.
 *                              If the message is not of this class or a subclass thereof
 *                              (e.g. a private protocol) will bypass the interceptor and be
 *                              continue to the inner behavior untouched.
 *
 * @tparam Outer The outer message type – the type of messages the intercepting behavior will accept
 * @tparam Inner The inner message type - the type of message the wrapped behavior accepts
 *
 * @see [[BehaviorSignalInterceptor]]
 */
abstract class BehaviorInterceptor[Outer, Inner](val interceptMessageClass: Class[Outer]) {
  import BehaviorInterceptor._

  /**
   * Scala API: The `ClassTag` for `Outer` ensures that only messages of this class or a subclass
   * thereof will be intercepted. Other message types (e.g. a private protocol) will bypass the
   * interceptor and be continue to the inner behavior untouched.
   */
  def this()(implicit interceptMessageClassTag: ClassTag[Outer]) =
    this({
      val runtimeClass = interceptMessageClassTag.runtimeClass
      (if (runtimeClass eq null) runtimeClass else BoxedType(runtimeClass)).asInstanceOf[Class[Outer]]
    })

  /**
   * Override to intercept actor startup. To trigger startup of
   * the next behavior in the stack, call `target.start()`.
   * @return The returned behavior will be the "started" behavior of the actor used to accept
   *         the next message or signal.
   */
  def aroundStart(ctx: TypedActorContext[Outer], target: PreStartTarget[Inner]): Behavior[Inner] =
    target.start(ctx)

  /**
   * Intercept a message sent to the running actor. Pass the message on to the next behavior
   * in the stack by passing it to `target.apply`, return `Behaviors.same` without invoking `target`
   * to filter out the message.
   *
   * @return The behavior for next message or signal
   */
  def aroundReceive(ctx: TypedActorContext[Outer], msg: Outer, target: ReceiveTarget[Inner]): Behavior[Inner]

  /**
   * Override to intercept a signal sent to the running actor. Pass the signal on to the next behavior
   * in the stack by passing it to `target.apply`.
   *
   * @return The behavior for next message or signal
   *
   * @see [[BehaviorSignalInterceptor]]
   */
  def aroundSignal(ctx: TypedActorContext[Outer], signal: Signal, target: SignalTarget[Inner]): Behavior[Inner] =
    target(ctx, signal)

  /**
   * @return `true` if this behavior logically the same as another behavior interceptor and can therefore be eliminated
   *         (to avoid building infinitely growing stacks of behaviors)? Default implementation is based on instance
   *         equality. Override to provide use case specific logic.
   */
  def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = this eq other

}

object BehaviorInterceptor {

  /**
   * Abstraction of passing the on further in the behavior stack in [[BehaviorInterceptor#aroundStart]].
   *
   * Not for user extension
   */
  @DoNotInherit
  trait PreStartTarget[T] {
    def start(ctx: TypedActorContext[_]): Behavior[T]
  }

  /**
   * Abstraction of passing the message on further in the behavior stack in [[BehaviorInterceptor#aroundReceive]].
   *
   * Not for user extension
   */
  @DoNotInherit
  trait ReceiveTarget[T] {
    def apply(ctx: TypedActorContext[_], msg: T): Behavior[T]

    /**
     * INTERNAL API
     *
     * Signal that the received message will result in a simulated restart
     * by the [[BehaviorInterceptor]]. A [[PreRestart]] will be sent to the
     * current behavior but the returned Behavior is ignored as a restart
     * is taking place.
     */
    @InternalApi
    private[akka] def signalRestart(ctx: TypedActorContext[_]): Unit
  }

  /**
   * Abstraction of passing the signal on further in the behavior stack in [[BehaviorInterceptor#aroundReceive]].
   *
   * Not for user extension
   */
  @DoNotInherit
  trait SignalTarget[T] {
    def apply(ctx: TypedActorContext[_], signal: Signal): Behavior[T]
  }

}

/**
 * A behavior interceptor allows for intercepting signals reception and perform arbitrary logic -
 * transform, filter, send to a side channel etc.
 *
 * The `BehaviorSignalInterceptor` API is considered a low level tool for building other features and
 * shouldn't be used for "normal" application logic. Several built-in intercepting behaviors
 * are provided through factories in the respective `Behaviors`.
 *
 * If the interceptor does keep mutable state care must be taken to create a new instance from
 * the factory function of `Behaviors.intercept` so that a new instance is created per spawned
 * actor rather than shared among actor instance.
 *
 * @tparam Inner The inner message type - the type of message the wrapped behavior accepts
 *
 * @see [[BehaviorInterceptor]]
 */
abstract class BehaviorSignalInterceptor[Inner] extends BehaviorInterceptor[Inner, Inner](null) {
  import BehaviorInterceptor._

  /**
   * Only signals and not messages are intercepted by `BehaviorSignalInterceptor`.
   */
  final override def aroundReceive(
      ctx: TypedActorContext[Inner],
      msg: Inner,
      target: ReceiveTarget[Inner]): Behavior[Inner] = {
    // by using `null` as interceptMessageClass of `BehaviorInterceptor` no messages will pass here
    throw new IllegalStateException(s"Unexpected message in ${getClass.getName}, it should only intercept signals.")
  }

  /**
   * Intercept a signal sent to the running actor. Pass the signal on to the next behavior
   * in the stack by passing it to `target.apply`.
   *
   * @return The behavior for next message or signal
   */
  override def aroundSignal(ctx: TypedActorContext[Inner], signal: Signal, target: SignalTarget[Inner]): Behavior[Inner]

}
