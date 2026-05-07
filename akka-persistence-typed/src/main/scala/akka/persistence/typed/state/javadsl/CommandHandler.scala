/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.javadsl

import java.util.Objects
import java.util.function.{ Function => JFunction }
import java.util.function.BiFunction
import java.util.function.Predicate
import java.util.function.Supplier

import scala.jdk.FunctionConverters._

import akka.annotation.InternalApi
import akka.persistence.typed.state.internal._
import akka.util.OptionVal

/**
 * FunctionalInterface for reacting on commands
 *
 * Used with [[CommandHandlerBuilder]] to setup the behavior of a [[DurableStateBehavior]]
 */
@FunctionalInterface
trait CommandHandler[Command, State] {
  def apply(state: State, command: Command): Effect[State]
}

object CommandHandlerBuilder {
  def builder[Command, State](): CommandHandlerBuilder[Command, State] =
    new CommandHandlerBuilder[Command, State]
}

final class CommandHandlerBuilder[Command, State]() {

  private var builders: List[CommandHandlerBuilderByState[Command, State, State]] = Nil

  /**
   * Use this method to define command handlers that are selected when the passed predicate holds true.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`
   *
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forState(statePredicate: Predicate[State]): CommandHandlerBuilderByState[Command, State, State] = {
    val builder = CommandHandlerBuilderByState.builder[Command, State](statePredicate)
    builders = builder :: builders
    builder
  }

  /**
   * Use this method to define command handlers that are selected when the passed predicate holds true
   * for a given subtype of your model. Useful when the model is defined as class hierarchy.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`
   *
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forState[S <: State](
      stateClass: Class[S],
      statePredicate: Predicate[S]): CommandHandlerBuilderByState[Command, S, State] = {
    val builder = new CommandHandlerBuilderByState[Command, S, State](stateClass, statePredicate)
    builders = builder.asInstanceOf[CommandHandlerBuilderByState[Command, State, State]] :: builders
    builder
  }

  /**
   * Use this method to define command handlers for a given subtype of your model. Useful when the model is defined as class hierarchy.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`.
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forStateType[S <: State](stateClass: Class[S]): CommandHandlerBuilderByState[Command, S, State] = {
    val builder = CommandHandlerBuilderByState.builder[Command, S, State](stateClass)
    builders = builder.asInstanceOf[CommandHandlerBuilderByState[Command, State, State]] :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used when the state is `null`.
   * This variant is particular useful when the empty state of your model is defined as `null`.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forNullState(): CommandHandlerBuilderByState[Command, State, State] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](s => Objects.isNull(s)).asJava
    val builder = CommandHandlerBuilderByState.builder[Command, State](predicate)
    builders = builder :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used for any not `null` state.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forNonNullState(): CommandHandlerBuilderByState[Command, State, State] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](s => Objects.nonNull(s)).asJava
    val builder = CommandHandlerBuilderByState.builder[Command, State](predicate)
    builders = builder :: builders
    builder
  }

  /**
   * The handlers defined by this builder are used for any state.
   * This variant is particular useful for models that have a single type (ie: no class hierarchy).
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   * Extra care should be taken when using [[forAnyState]] as it will match any state. Any command handler define after it will never be reached.
   *
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def forAnyState(): CommandHandlerBuilderByState[Command, State, State] = {
    val predicate: Predicate[State] = enrichAsJavaPredicate[State](_ => true).asJava
    val builder = CommandHandlerBuilderByState.builder[Command, State](predicate)
    builders = builder :: builders
    builder
  }

  def build(): CommandHandler[Command, State] = {

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

object CommandHandlerBuilderByState {

  private val _trueStatePredicate: Predicate[Any] = new Predicate[Any] {
    override def test(t: Any): Boolean = true
  }

  private def trueStatePredicate[S]: Predicate[S] = _trueStatePredicate.asInstanceOf[Predicate[S]]

  /**
   * @param stateClass The handlers defined by this builder are used when the state is an instance of the `stateClass`
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def builder[Command, S <: State, State](stateClass: Class[S]): CommandHandlerBuilderByState[Command, S, State] =
    new CommandHandlerBuilderByState(stateClass, statePredicate = trueStatePredicate)

  /**
   * @param statePredicate The handlers defined by this builder are used when the `statePredicate` is `true`,
   *                       useful for example when state type is an Optional
   * @return A new, mutable, CommandHandlerBuilderByState
   */
  def builder[Command, State](statePredicate: Predicate[State]): CommandHandlerBuilderByState[Command, State, State] =
    new CommandHandlerBuilderByState(classOf[Any].asInstanceOf[Class[State]], statePredicate)

  /**
   * INTERNAL API
   */
  @InternalApi private final case class CommandHandlerCase[Command, State](
      commandPredicate: Command => Boolean,
      statePredicate: State => Boolean,
      handler: BiFunction[State, Command, Effect[State]])
}

final class CommandHandlerBuilderByState[Command, S <: State, State] @InternalApi private[akka] (
    private val stateClass: Class[S],
    private val statePredicate: Predicate[S]) {

  import CommandHandlerBuilderByState.CommandHandlerCase

  private var cases: List[CommandHandlerCase[Command, State]] = Nil

  private def addCase(predicate: Command => Boolean, handler: BiFunction[S, Command, Effect[State]]): Unit = {
    cases = CommandHandlerCase[Command, State](
        commandPredicate = predicate,
        statePredicate = state =>
          if (state == null) statePredicate.test(state.asInstanceOf[S])
          else
            statePredicate.test(state.asInstanceOf[S]) && stateClass.isAssignableFrom(state.getClass),
        handler.asInstanceOf[BiFunction[State, Command, Effect[State]]]) :: cases
  }

  /**
   * Matches any command which the given `predicate` returns true for.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   */
  def onCommand(
      predicate: Predicate[Command],
      handler: BiFunction[S, Command, Effect[State]]): CommandHandlerBuilderByState[Command, S, State] = {
    addCase(cmd => predicate.test(cmd), handler)
    this
  }

  /**
   * Matches any command which the given `predicate` returns true for.
   *
   * Use this when the `State` is not needed in the `handler`, otherwise there is an overloaded method that pass
   * the state in a `BiFunction`.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   */
  def onCommand(
      predicate: Predicate[Command],
      handler: JFunction[Command, Effect[State]]): CommandHandlerBuilderByState[Command, S, State] = {
    addCase(cmd => predicate.test(cmd), new BiFunction[S, Command, Effect[State]] {
      override def apply(state: S, cmd: Command): Effect[State] = handler(cmd)
    })
    this
  }

  /**
   * Matches commands that are of the given `commandClass` or subclass thereof
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   */
  def onCommand[C <: Command](
      commandClass: Class[C],
      handler: BiFunction[S, C, Effect[State]]): CommandHandlerBuilderByState[Command, S, State] = {
    addCase(
      cmd => commandClass.isAssignableFrom(cmd.getClass),
      handler.asInstanceOf[BiFunction[S, Command, Effect[State]]])
    this
  }

  /**
   * Matches commands that are of the given `commandClass` or subclass thereof.
   *
   * Use this when the `State` is not needed in the `handler`, otherwise there is an overloaded method that pass
   * the state in a `BiFunction`.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   */
  def onCommand[C <: Command](
      commandClass: Class[C],
      handler: JFunction[C, Effect[State]]): CommandHandlerBuilderByState[Command, S, State] = {
    onCommand[C](commandClass, new BiFunction[S, C, Effect[State]] {
      override def apply(state: S, cmd: C): Effect[State] = handler(cmd)
    })
  }

  /**
   * Matches commands that are of the given `commandClass` or subclass thereof.
   *
   * Use this when you just need to initialize the `State` without using any data from the command.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   */
  def onCommand[C <: Command](
      commandClass: Class[C],
      handler: Supplier[Effect[State]]): CommandHandlerBuilderByState[Command, S, State] = {
    onCommand[C](commandClass, new BiFunction[S, C, Effect[State]] {
      override def apply(state: S, cmd: C): Effect[State] = handler.get()
    })
  }

  /**
   * Matches any command.
   *
   * Use this to declare a command handler that will match any command. This is particular useful when encoding
   * a finite state machine in which the final state is not supposed to handle any new command.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * Extra care should be taken when using [[onAnyCommand]] as it will match any command.
   * This method builds and returns the command handler since this will not let through any states to subsequent match statements.
   *
   * @return A CommandHandler from the appended states.
   */
  def onAnyCommand(handler: BiFunction[S, Command, Effect[State]]): CommandHandler[Command, State] = {
    addCase(_ => true, handler)
    build()
  }

  /**
   * Matches any command.
   *
   * Use this to declare a command handler that will match any command. This is particular useful when encoding
   * a finite state machine in which the final state is not supposed to handle any new command.
   *
   * Use this when you just need to return an [[Effect]] without using any data from the state.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * Extra care should be taken when using [[onAnyCommand]] as it will match any command.
   * This method builds and returns the command handler since this will not let through any states to subsequent match statements.
   *
   * @return A CommandHandler from the appended states.
   */
  def onAnyCommand(handler: JFunction[Command, Effect[State]]): CommandHandler[Command, State] = {
    addCase(_ => true, new BiFunction[S, Command, Effect[State]] {
      override def apply(state: S, cmd: Command): Effect[State] = handler(cmd)
    })
    build()
  }

  /**
   * Matches any command.
   *
   * Use this to declare a command handler that will match any command. This is particular useful when encoding
   * a finite state machine in which the final state is not supposed to handle any new command.
   *
   * Use this when you just need to return an [[Effect]] without using any data from the command or from the state.
   *
   * Note: command handlers are selected in the order they are added. Once a matching is found, it's selected for handling the command
   * and no further lookup is done. Therefore you must make sure that their matching conditions don't overlap,
   * otherwise you risk to 'shadow' part of your command handlers.
   *
   * Extra care should be taken when using [[onAnyCommand]] as it will match any command.
   * This method builds and returns the command handler since this will not let through any states to subsequent match statements.
   *
   * @return A CommandHandler from the appended states.
   */
  def onAnyCommand(handler: Supplier[Effect[State]]): CommandHandler[Command, State] = {
    addCase(_ => true, new BiFunction[S, Command, Effect[State]] {
      override def apply(state: S, cmd: Command): Effect[State] = handler.get()
    })
    build()
  }

  /**
   * Compose this builder with another builder. The handlers in this builder will be tried first followed
   * by the handlers in `other`.
   */
  def orElse[S2 <: State](
      other: CommandHandlerBuilderByState[Command, S2, State]): CommandHandlerBuilderByState[Command, S2, State] = {
    val newBuilder = new CommandHandlerBuilderByState[Command, S2, State](other.stateClass, other.statePredicate)
    // problem with overloaded constructor with `cases` as parameter
    newBuilder.cases = other.cases ::: cases
    newBuilder
  }

  /**
   * Builds and returns a handler from the appended states. The returned [[CommandHandler]] will throw a [[scala.MatchError]]
   * if applied to a command that has no defined case.
   */
  def build(): CommandHandler[Command, State] = {
    val builtCases = cases.reverse.toArray

    new CommandHandler[Command, State] {
      override def apply(state: State, command: Command): Effect[State] = {
        var idx = 0
        var effect: OptionVal[Effect[State]] = OptionVal.None

        while (idx < builtCases.length && effect.isEmpty) {
          val curr = builtCases(idx)
          if (curr.statePredicate(state) && curr.commandPredicate(command)) {
            val x: Effect[State] = curr.handler.apply(state, command)
            effect = OptionVal.Some(x)
          }
          idx += 1
        }

        effect match {
          case OptionVal.Some(e) => e.asInstanceOf[EffectImpl[State]]
          case _ =>
            throw new MatchError(s"No match found for command of type [${command.getClass.getName}]")
        }
      }
    }
  }

}
