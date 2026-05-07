/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import java.util.Objects
import java.util.function.{ BiFunction, Predicate, Supplier, Function => JFunction }

import scala.jdk.FunctionConverters._

import akka.annotation.InternalApi
import akka.util.OptionVal

/**
 * FunctionalInterface for reacting on events having been persisted
 *
 * Used with [[EventHandlerBuilder]] to setup the behavior of a [[EventSourcedBehavior]]
 */
@FunctionalInterface
trait EventHandler[State, Event] {
  def apply(state: State, event: Event): State
}

object EventHandlerBuilder {
  def builder[State, Event](): EventHandlerBuilder[State, Event] =
    new EventHandlerBuilder[State, Event]()

}

final class EventHandlerBuilder[State, Event]() {

  private var builders: List[EventHandlerBuilderByState[State, State, Event]] = Nil

  /**
   * Use this method to define event handlers that are selected when the passed predicate holds true.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forState(statePredicate: Predicate[State]): EventHandlerBuilderByState[State, State, Event] = {
    val builder = EventHandlerBuilderByState.builder[State, Event](statePredicate)
    builders = builder :: builders
    builder
  }

  /**
   * Use this method to define event handlers that are selected when the passed predicate holds true
   * for a given subtype of your model. Useful when the model is defined as class hierarchy.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forState[S <: State](
      stateClass: Class[S],
      statePredicate: Predicate[S]): EventHandlerBuilderByState[S, State, Event] = {
    val builder = new EventHandlerBuilderByState[S, State, Event](stateClass, statePredicate)
    builders = builder.asInstanceOf[EventHandlerBuilderByState[State, State, Event]] :: builders
    builder
  }

  /**
   * Use this method to define command handlers for a given subtype of your model. Useful when the model is defined as class hierarchy.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forStateType[S <: State](stateClass: Class[S]): EventHandlerBuilderByState[S, State, Event] = {
    val builder = EventHandlerBuilderByState.builder[S, State, Event](stateClass)
    builders = builder.asInstanceOf[EventHandlerBuilderByState[State, State, Event]] :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used when the state is `null`.
   * This variant is particular useful when the empty state of your model is defined as `null`.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forNullState(): EventHandlerBuilderByState[State, State, Event] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](s => Objects.isNull(s)).asJava
    val builder = EventHandlerBuilderByState.builder[State, Event](predicate)
    builders = builder :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used for any not `null` state.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forNonNullState(): EventHandlerBuilderByState[State, State, Event] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](s => Objects.nonNull(s)).asJava
    val builder = EventHandlerBuilderByState.builder[State, Event](predicate)
    builders = builder :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used for any state.
   * This variant is particular useful for models that have a single type (ie: no class hierarchy).
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   * Extra care should be taken when using [[forAnyState]] as it will match any state. Any event handler define after it will never be reached.
   *
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def forAnyState(): EventHandlerBuilderByState[State, State, Event] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](_ => true).asJava
    val builder = EventHandlerBuilderByState.builder[State, Event](predicate)
    builders = builder :: builders
    builder
  }

  def build(): EventHandler[State, Event] = {

    val combined =
      builders.reverse match {
        case head :: Nil => head
        case head :: tail =>
          tail.foldLeft(head) { (acc, builder) =>
            acc.orElse(builder)
          }
        case Nil => throw new IllegalStateException("No matchers defined")
      }

    combined.build()
  }

}

object EventHandlerBuilderByState {

  private val _trueStatePredicate: Predicate[Any] = new Predicate[Any] {
    override def test(t: Any): Boolean = true
  }

  private def trueStatePredicate[S]: Predicate[S] = _trueStatePredicate.asInstanceOf[Predicate[S]]

  /**
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def builder[S <: State, State, Event](stateClass: Class[S]): EventHandlerBuilderByState[S, State, Event] =
    new EventHandlerBuilderByState(stateClass, statePredicate = trueStatePredicate)

  /**
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`,
   *                       useful for example when state type is an Optional
   * @return A new, mutable, EventHandlerBuilderByState
   */
  def builder[State, Event](statePredicate: Predicate[State]): EventHandlerBuilderByState[State, State, Event] =
    new EventHandlerBuilderByState(classOf[Any].asInstanceOf[Class[State]], statePredicate)

  /**
   * INTERNAL API
   */
  @InternalApi private final case class EventHandlerCase[State, Event](
      statePredicate: State => Boolean,
      eventPredicate: Event => Boolean,
      handler: BiFunction[State, Event, State])
}

final class EventHandlerBuilderByState[S <: State, State, Event](
    private val stateClass: Class[S],
    private val statePredicate: Predicate[S]) {

  import EventHandlerBuilderByState.EventHandlerCase

  private var cases: List[EventHandlerCase[State, Event]] = Nil

  private def addCase(eventPredicate: Event => Boolean, handler: BiFunction[State, Event, State]): Unit = {
    cases = EventHandlerCase[State, Event](
        statePredicate = state =>
          if (state == null) statePredicate.test(state.asInstanceOf[S])
          else statePredicate.test(state.asInstanceOf[S]) && stateClass.isAssignableFrom(state.getClass),
        eventPredicate = eventPredicate,
        handler) :: cases
  }

  /**
   * Match any event which is an instance of `E` or a subtype of `E`.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   */
  def onEvent[E <: Event](
      eventClass: Class[E],
      handler: BiFunction[S, E, State]): EventHandlerBuilderByState[S, State, Event] = {
    addCase(e => eventClass.isAssignableFrom(e.getClass), handler.asInstanceOf[BiFunction[State, Event, State]])
    this
  }

  /**
   * Match any event which is an instance of `E` or a subtype of `E`.
   *
   * Use this when then `State` is not needed in the `handler`, otherwise there is an overloaded method that pass
   * the state in a `BiFunction`.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   */
  def onEvent[E <: Event](
      eventClass: Class[E],
      handler: JFunction[E, State]): EventHandlerBuilderByState[S, State, Event] = {
    onEvent[E](eventClass, new BiFunction[S, E, State] {
      override def apply(state: S, event: E): State = handler(event)
    })
  }

  /**
   * Match any event which is an instance of `E` or a subtype of `E`.
   *
   * Use this when then `State` and the `Event` are not needed in the `handler`.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   */
  def onEvent[E <: Event](
      eventClass: Class[E],
      handler: Supplier[State]): EventHandlerBuilderByState[S, State, Event] = {

    val supplierBiFunction = new BiFunction[S, E, State] {
      def apply(t: S, u: E): State = handler.get()
    }

    onEvent(eventClass, supplierBiFunction)
  }

  /**
   * Match any event.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * Extra care should be taken when using [[onAnyEvent]] as it will match any event.
   * This method builds and returns the event handler since this will not let through any states to subsequent match statements.
   *
   * @return An EventHandler from the appended states.
   */
  def onAnyEvent(handler: BiFunction[State, Event, State]): EventHandler[State, Event] = {
    addCase(_ => true, handler.asInstanceOf[BiFunction[State, Event, State]])
    build()
  }

  /**
   * Match any event.
   *
   * Use this when then `State` is not needed in the `handler`, otherwise there is an overloaded method that pass
   * the state in a `BiFunction`.
   *
   * Note: event handlers are selected in the order they are added. Once a matching is found, it's selected for handling the event
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your event handlers.
   *
   * Extra care should be taken when using [[onAnyEvent]] as it will match any event.
   * This method builds and returns the event handler since this will not let through any states to subsequent match statements.
   *
   * @return An EventHandler from the appended states.
   */
  def onAnyEvent(handler: JFunction[Event, State]): EventHandler[State, Event] = {
    onAnyEvent(new BiFunction[State, Event, State] {
      override def apply(state: State, event: Event): State = handler(event)
    })
    build()
  }

  /**
   * Compose this builder with another builder. The handlers in this builder will be tried first followed
   * by the handlers in `other`.
   */
  def orElse[S2 <: State](
      other: EventHandlerBuilderByState[S2, State, Event]): EventHandlerBuilderByState[S2, State, Event] = {
    val newBuilder = new EventHandlerBuilderByState[S2, State, Event](other.stateClass, other.statePredicate)
    // problem with overloaded constructor with `cases` as parameter
    newBuilder.cases = other.cases ::: cases
    newBuilder
  }

  /**
   * Builds and returns a handler from the appended states. The returned [[EventHandler]] will throw a [[scala.MatchError]]
   * if applied to an event that has no defined case.
   */
  def build(): EventHandler[State, Event] = {
    val builtCases = cases.reverse.toArray

    new EventHandler[State, Event] {
      def apply(state: State, event: Event): State = {
        var result: OptionVal[State] = OptionVal.none
        var idx = 0
        while (idx < builtCases.length && result.isEmpty) {
          val curr = builtCases(idx)
          if (curr.statePredicate(state) && curr.eventPredicate(event)) {
            result = OptionVal.Some[State](curr.handler.apply(state, event))
          }
          idx += 1
        }

        result match {
          case OptionVal.Some(s) => s
          case _ =>
            val stateClass = if (state == null) "null" else state.getClass.getName
            throw new MatchError(
              s"No match found for event [${event.getClass}] and state [$stateClass]. Has this event been stored using an EventAdapter?")
        }
      }
    }
  }
}
