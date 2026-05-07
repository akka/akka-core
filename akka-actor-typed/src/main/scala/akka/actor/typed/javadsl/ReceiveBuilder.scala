/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import scala.annotation.tailrec

import akka.actor.typed.{ Behavior, MessageAdaptionFailure, Signal }
import akka.annotation.InternalApi
import akka.japi.function.{ Creator, Function => JFunction, Predicate => JPredicate }
import akka.util.OptionVal

/**
 * Mutable builder used when implementing [[AbstractBehavior]].
 *
 * When handling a message or signal, this [[Behavior]] will consider all handlers in the order they were added,
 * looking for the first handler for which both the type and the (optional) predicate match.
 *
 * @tparam T the common superclass of all supported messages.
 */
final class ReceiveBuilder[T] private (
    private var messageHandlers: List[ReceiveBuilder.Case[T, T]],
    private var signalHandlers: List[ReceiveBuilder.Case[T, Signal]]) {

  import ReceiveBuilder._

  def build(): Receive[T] = {
    // signal handlers will often be empty so optimize for that
    val builtSignalHandlers =
      if (signalHandlers.isEmpty) defaultSignalHandlers[T]
      else (adapterExceptionSignalHandler[T] :: signalHandlers).reverse
    new BuiltReceive[T](messageHandlers.reverse.toArray, builtSignalHandlers.toArray)
  }

  /**
   * Add a new case to the message handling.
   *
   * @param type type of message to match
   * @param handler action to apply if the type matches
   * @tparam M type of message to match
   * @return this behavior builder
   */
  def onMessage[M <: T](`type`: Class[M], handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] =
    withMessage(OptionVal.Some(`type`), OptionVal.None, handler)

  /**
   * Add a new predicated case to the message handling.
   *
   * @param type type of message to match
   * @param test a predicate that will be evaluated on the argument if the type matches
   * @param handler action to apply if the type matches and the predicate returns true
   * @tparam M type of message to match
   * @return this behavior builder
   */
  def onMessage[M <: T](`type`: Class[M], test: JPredicate[M], handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] =
    withMessage(OptionVal.Some(`type`), OptionVal.Some(test), handler)

  /**
   * Add a new case to the message handling without compile time type check.
   *
   * Should normally not be used, but when matching on class with generic type
   * argument it can be useful, e.g. <code>List.class</code> and <code>(List&lt;String&gt; list) -> {...}</code>
   *
   * @param type type of message to match
   * @param handler action to apply when the type matches
   * @return this behavior builder
   */
  def onMessageUnchecked[M <: T](`type`: Class[_ <: T], handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] =
    withMessage[M](OptionVal.Some(`type`.asInstanceOf[Class[M]]), OptionVal.None, handler)

  /**
   * Add a new case to the message handling matching equal messages.
   *
   * @param msg the message to compare to
   * @param handler action to apply when the message matches
   * @return this behavior builder
   */
  def onMessageEquals(msg: T, handler: Creator[Behavior[T]]): ReceiveBuilder[T] =
    withMessage(OptionVal.Some(msg.getClass), OptionVal.Some(new JPredicate[T] {
      override def test(param: T): Boolean = param == (msg)
    }), new JFunction[T, Behavior[T]] {
      // invoke creator without the message
      override def apply(param: T): Behavior[T] = handler.create()
    })

  /**
   * Add a new case to the message handling matching any message. Subsequent `onMessage` clauses will
   * never see any messages.
   *
   * @param handler action to apply for any message
   * @return this behavior builder
   */
  def onAnyMessage(handler: JFunction[T, Behavior[T]]): ReceiveBuilder[T] =
    withMessage(OptionVal.None, OptionVal.None, handler)

  /**
   * Add a new case to the signal handling.
   *
   * @param type type of signal to match
   * @param handler action to apply if the type matches
   * @tparam M type of signal to match
   * @return this behavior builder
   */
  def onSignal[M <: Signal](`type`: Class[M], handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] =
    withSignal(`type`, OptionVal.None, handler)

  /**
   * Add a new predicated case to the signal handling.
   *
   * @param type type of signals to match
   * @param test a predicate that will be evaluated on the argument if the type matches
   * @param handler action to apply if the type matches and the predicate returns true
   * @tparam M type of signal to match
   * @return this behavior builder
   */
  def onSignal[M <: Signal](
      `type`: Class[M],
      test: JPredicate[M],
      handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] =
    withSignal(`type`, OptionVal.Some(test), handler)

  /**
   * Add a new case to the signal handling matching equal signals.
   *
   * @param signal the signal to compare to
   * @param handler action to apply when the message matches
   * @return this behavior builder
   */
  def onSignalEquals(signal: Signal, handler: Creator[Behavior[T]]): ReceiveBuilder[T] =
    withSignal(signal.getClass, OptionVal.Some(new JPredicate[Signal] {
      override def test(param: Signal): Boolean = param == signal
    }), new JFunction[Signal, Behavior[T]] {
      override def apply(param: Signal): Behavior[T] = handler.create()
    })

  private def withMessage[M <: T](
      `type`: OptionVal[Class[M]],
      test: OptionVal[JPredicate[M]],
      handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] = {
    messageHandlers = Case[T, M](`type`, test, handler).asInstanceOf[Case[T, T]] +: messageHandlers
    this
  }

  private def withSignal[M <: Signal](
      `type`: Class[M],
      test: OptionVal[JPredicate[M]],
      handler: JFunction[M, Behavior[T]]): ReceiveBuilder[T] = {
    signalHandlers = Case[T, M](OptionVal.Some(`type`), test, handler).asInstanceOf[Case[T, Signal]] +: signalHandlers
    this
  }
}

object ReceiveBuilder {

  /** Create a new mutable receive builder */
  def create[T]: ReceiveBuilder[T] = new ReceiveBuilder[T](Nil, Nil)

  /** INTERNAL API */
  @InternalApi
  private[javadsl] final case class Case[BT, MT](
      `type`: OptionVal[Class[_ <: MT]],
      test: OptionVal[JPredicate[MT]],
      handler: JFunction[MT, Behavior[BT]])

  /** INTERNAL API */
  @InternalApi
  private val _adapterExceptionSignalHandler = Case[Any, MessageAdaptionFailure](
    OptionVal.Some(classOf[MessageAdaptionFailure]),
    OptionVal.None,
    failure => throw failure.exception)

  /** INTERNAL API */
  @InternalApi
  private def adapterExceptionSignalHandler[T]: Case[T, Signal] =
    _adapterExceptionSignalHandler.asInstanceOf[Case[T, Signal]]

  /** INTERNAL API */
  @InternalApi
  private val _defaultSignalHandlers = adapterExceptionSignalHandler :: Nil

  /** INTERNAL API */
  @InternalApi
  private def defaultSignalHandlers[T] = _defaultSignalHandlers.asInstanceOf[List[Case[T, Signal]]]

}

/**
 * Receive type for [[AbstractBehavior]]
 *
 * INTERNAL API
 */
@InternalApi
private final class BuiltReceive[T](
    messageHandlers: Array[ReceiveBuilder.Case[T, T]],
    signalHandlers: Array[ReceiveBuilder.Case[T, Signal]])
    extends Receive[T] {
  import ReceiveBuilder.Case

  override def receiveMessage(msg: T): Behavior[T] = receive[T](msg, messageHandlers, 0)

  override def receiveSignal(msg: Signal): Behavior[T] = receive[Signal](msg, signalHandlers, 0)

  @tailrec
  private def receive[M](msg: M, handlers: Array[Case[T, M]], idx: Int): Behavior[T] = {
    if (handlers.length == 0) {
      Behaviors.unhandled[T]
    } else {
      val Case(cls, predicate, handler) = handlers(idx)
      if ((cls.isEmpty || cls.get.isAssignableFrom(msg.getClass)) && (predicate.isEmpty || predicate.get.test(msg)))
        handler(msg)
      else if (idx == handlers.length - 1)
        Behaviors.unhandled[T]
      else
        receive(msg, handlers, idx + 1)
    }
  }
}
