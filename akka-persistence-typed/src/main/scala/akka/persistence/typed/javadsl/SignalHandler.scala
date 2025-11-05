/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import java.util.function.BiConsumer
import java.util.function.Consumer

import akka.actor.typed.Signal
import akka.annotation.InternalApi

object SignalHandler {
  private val Empty: SignalHandler[Any] = new SignalHandler[Any](PartialFunction.empty)

  def empty[State]: SignalHandler[State] = Empty.asInstanceOf[SignalHandler[State]]
}

final class SignalHandler[State](_handler: PartialFunction[(State, Signal), Unit]) {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def isEmpty: Boolean = _handler eq PartialFunction.empty

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def handler: PartialFunction[(State, Signal), Unit] = _handler
}

object SignalHandlerBuilder {
  def builder[State]: SignalHandlerBuilder[State] = new SignalHandlerBuilder
}

/**
 * Mutable builder for handling signals in [[EventSourcedBehavior]]
 *
 * Not for user instantiation, use [[EventSourcedBehavior.newSignalHandlerBuilder]] to get an instance.
 */
final class SignalHandlerBuilder[State] {

  private var handler: PartialFunction[(State, Signal), Unit] = PartialFunction.empty

  /**
   * If the behavior receives a signal of type `T`, `callback` is invoked with the signal instance as input.
   */
  def onSignal[T <: Signal](signalType: Class[T], callback: BiConsumer[State, T]): SignalHandlerBuilder[State] = {
    val newPF: PartialFunction[(State, Signal), Unit] = {
      case (state, t) if signalType.isInstance(t) =>
        callback.accept(state, t.asInstanceOf[T])
    }
    handler = newPF.orElse(handler)
    this
  }

  /**
   * If the behavior receives exactly the signal `signal`, `callback` is invoked.
   */
  def onSignal[T <: Signal](signal: T, callback: Consumer[State]): SignalHandlerBuilder[State] = {
    val newPF: PartialFunction[(State, Signal), Unit] = {
      case (state, `signal`) =>
        callback.accept(state)
    }
    handler = newPF.orElse(handler)
    this
  }

  def build: SignalHandler[State] = new SignalHandler(handler)

}
