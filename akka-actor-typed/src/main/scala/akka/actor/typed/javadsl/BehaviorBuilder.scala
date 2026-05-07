/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import scala.annotation.tailrec

import BehaviorBuilder._

import akka.actor.typed.Behavior
import akka.actor.typed.ExtensibleBehavior
import akka.actor.typed.Signal
import akka.actor.typed.TypedActorContext
import akka.annotation.InternalApi
import akka.japi.function.{ Function => JFunction }
import akka.japi.function.{ Predicate => JPredicate }
import akka.japi.function.Creator
import akka.util.OptionVal

/**
 * Immutable builder used for creating a [[Behavior]] by 'chaining' message and signal handlers.
 *
 * When handling a message or signal, this [[Behavior]] will consider all handlers in the order they were added,
 * looking for the first handler for which both the type and the (optional) predicate match.
 *
 * Akka `akka.japi.function` lambda types are used throughout to allow handlers to throw checked exceptions
 * (which will fail the actor).
 *
 * @tparam T the common superclass of all supported messages.
 */
final class BehaviorBuilder[T] private (messageHandlers: List[Case[T, T]], signalHandlers: List[Case[T, Signal]]) {

  /**
   * Build a Behavior from the current state of the builder
   */
  def build(): Behavior[T] = {
    new BuiltBehavior[T](messageHandlers.reverse.toArray, signalHandlers.reverse.toArray)
  }

  /**
   * Add a new case to the message handling.
   *
   * @param type    type of message to match
   * @param handler action to apply if the type matches
   * @tparam M type of message to match
   * @return a new behavior builder with the specified handling appended
   */
  def onMessage[M <: T](`type`: Class[M], handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] =
    withMessage(OptionVal.Some(`type`), OptionVal.None, handler)

  /**
   * Add a new predicated case to the message handling.
   *
   * @param type    type of message to match
   * @param test    a predicate that will be evaluated on the argument if the type matches
   * @param handler action to apply if the type matches and the predicate returns true
   * @tparam M type of message to match
   * @return a new behavior builder with the specified handling appended
   */
  def onMessage[M <: T](`type`: Class[M], test: JPredicate[M], handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] =
    withMessage(OptionVal.Some(`type`), OptionVal.Some((t: T) => test.test(t.asInstanceOf[M])), handler)

  /**
   * Add a new case to the message handling without compile time type check.
   *
   * Should normally not be used, but when matching on class with generic type
   * argument it can be useful, e.g. <code>List.class</code> and <code>(List&lt;String&gt; list) -> {...}</code>
   *
   * @param type    type of message to match
   * @param handler action to apply when the type matches
   * @return a new behavior builder with the specified handling appended
   */
  def onMessageUnchecked[M <: T](`type`: Class[_ <: T], handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] =
    withMessage[M](OptionVal.Some(`type`.asInstanceOf[Class[M]]), OptionVal.None, handler)

  /**
   * Add a new case to the message handling matching equal messages.
   *
   * @param msg     the message to compare to
   * @param handler action to apply when the message matches
   * @return a new behavior builder with the specified handling appended
   */
  def onMessageEquals(msg: T, handler: Creator[Behavior[T]]): BehaviorBuilder[T] =
    withMessage[T](
      OptionVal.Some(msg.getClass.asInstanceOf[Class[T]]),
      OptionVal.Some(_ == msg),
      (_: T) => handler.create())

  /**
   * Add a new case to the message handling matching any message. Subsequent `onMessage` clauses will
   * never see any messages.
   *
   * @param handler action to apply for any message
   * @return a new behavior builder with the specified handling appended
   */
  def onAnyMessage(handler: JFunction[T, Behavior[T]]): BehaviorBuilder[T] =
    withMessage(OptionVal.None, OptionVal.None, handler)

  /**
   * Add a new case to the signal handling.
   *
   * @param type    type of signal to match
   * @param handler action to apply if the type matches
   * @tparam M type of signal to match
   * @return a new behavior builder with the specified handling appended
   */
  def onSignal[M <: Signal](`type`: Class[M], handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] =
    withSignal(`type`, OptionVal.None, handler.asInstanceOf[JFunction[Signal, Behavior[T]]])

  /**
   * Add a new predicated case to the signal handling.
   *
   * @param type    type of signals to match
   * @param test    a predicate that will be evaluated on the argument if the type matches
   * @param handler action to apply if the type matches and the predicate returns true
   * @tparam M type of signal to match
   * @return a new behavior builder with the specified handling appended
   */
  def onSignal[M <: Signal](
      `type`: Class[M],
      test: JPredicate[M],
      handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] =
    withSignal(
      `type`,
      OptionVal.Some((t: Signal) => test.test(t.asInstanceOf[M])),
      handler.asInstanceOf[JFunction[Signal, Behavior[T]]])

  /**
   * Add a new case to the signal handling matching equal signals.
   *
   * @param signal  the signal to compare to
   * @param handler action to apply when the message matches
   * @return a new behavior builder with the specified handling appended
   */
  def onSignalEquals(signal: Signal, handler: Creator[Behavior[T]]): BehaviorBuilder[T] =
    withSignal(signal.getClass, OptionVal.Some(_.equals(signal)), (_: Signal) => handler.create())

  private def withMessage[M <: T](
      clazz: OptionVal[Class[M]],
      test: OptionVal[M => Boolean],
      handler: JFunction[M, Behavior[T]]): BehaviorBuilder[T] = {
    val newCase = Case(clazz, test, handler)
    new BehaviorBuilder[T](newCase.asInstanceOf[Case[T, T]] +: messageHandlers, signalHandlers)
  }

  private def withSignal[M <: Signal](
      `type`: Class[M],
      test: OptionVal[Signal => Boolean],
      handler: JFunction[Signal, Behavior[T]]): BehaviorBuilder[T] = {
    new BehaviorBuilder[T](
      messageHandlers,
      Case(OptionVal.Some(`type`), test, handler).asInstanceOf[Case[T, Signal]] +: signalHandlers)
  }
}

object BehaviorBuilder {

  private val _empty = new BehaviorBuilder[Nothing](Nil, Nil)

  // used for both matching signals and messages so we throw away types after they are enforced by the builder API above

  /** INTERNAL API */
  @InternalApi
  private[javadsl] final case class Case[BT, MT](
      `type`: OptionVal[Class[_ <: MT]],
      test: OptionVal[MT => Boolean],
      handler: JFunction[MT, Behavior[BT]])

  /**
   * @return new empty immutable behavior builder.
   */
  // Empty param list to work around https://github.com/lampepfl/dotty/issues/10347
  def create[T]: BehaviorBuilder[T] = _empty.asInstanceOf[BehaviorBuilder[T]]
}

/**
 * The concrete behavior
 *
 * INTERNAL API
 */
@InternalApi
private final class BuiltBehavior[T](messageHandlers: Array[Case[T, T]], signalHandlers: Array[Case[T, Signal]])
    extends ExtensibleBehavior[T] {

  override def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] = receive(msg, messageHandlers, 0)

  override def receiveSignal(ctx: TypedActorContext[T], msg: Signal): Behavior[T] = receive(msg, signalHandlers, 0)

  @tailrec
  private def receive[M](msg: M, handlers: Array[Case[T, M]], idx: Int): Behavior[T] = {
    if (handlers.length == 0) {
      Behaviors.unhandled[T]
    } else {
      val Case(cls, predicate, handler) = handlers(idx)
      if ((cls.isEmpty || cls.get.isAssignableFrom(msg.getClass)) && (predicate.isEmpty || predicate.get.apply(msg)))
        handler(msg)
      else if (idx == handlers.length - 1)
        Behaviors.unhandled[T]
      else
        receive(msg, handlers, idx + 1)
    }
  }
}
