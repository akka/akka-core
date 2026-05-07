/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import java.util.{ List => JList }
import java.util.Optional
import java.util.function.{ Function => JFunction }

import scala.annotation.varargs
import scala.reflect.ClassTag

import com.typesafe.config.Config

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.persistence.testkit.scaladsl
import scala.jdk.CollectionConverters._

/**
 * Testing of [[akka.persistence.typed.javadsl.EventSourcedBehavior]] implementations.
 * It supports running one command at a time and you can assert that the synchronously returned result is as expected.
 * The result contains the events emitted by the command and the new state after applying the events.
 * It also has support for verifying the reply to a command.
 *
 * Serialization of commands, events and state are verified automatically.
 */
@ApiMayChange
object EventSourcedBehaviorTestKit {

  /**
   * The configuration to be included in the configuration of the `ActorSystem`. Typically used as
   * constructor parameter to `TestKitJunitResource`. The configuration enables the in-memory
   * journal and snapshot storage.
   */
  val config: Config = scaladsl.EventSourcedBehaviorTestKit.config

  val enabledSerializationSettings: SerializationSettings = new SerializationSettings(
    enabled = true,
    verifyEquality = false,
    verifyCommands = true,
    verifyEvents = true,
    verifyState = true)

  val disabledSerializationSettings: SerializationSettings = new SerializationSettings(
    enabled = false,
    verifyEquality = false,
    verifyCommands = false,
    verifyEvents = false,
    verifyState = false)

  /**
   * Customization of which serialization checks that are performed.
   * `equals` must be implemented (or using `case class`) when `verifyEquality` is enabled.
   */
  final class SerializationSettings(
      val enabled: Boolean,
      val verifyEquality: Boolean,
      val verifyCommands: Boolean,
      val verifyEvents: Boolean,
      val verifyState: Boolean) {

    def withEnabled(value: Boolean): SerializationSettings =
      copy(enabled = value)

    def withVerifyEquality(value: Boolean): SerializationSettings =
      copy(verifyEquality = value)

    def withVerifyCommands(value: Boolean): SerializationSettings =
      copy(verifyCommands = value)

    def withVerifyEvents(value: Boolean): SerializationSettings =
      copy(verifyEvents = value)

    def withVerifyState(value: Boolean): SerializationSettings =
      copy(verifyState = value)

    private def copy(
        enabled: Boolean = this.enabled,
        verifyEquality: Boolean = this.verifyEquality,
        verifyCommands: Boolean = this.verifyCommands,
        verifyEvents: Boolean = this.verifyEvents,
        verifyState: Boolean = this.verifyState): SerializationSettings = {
      new SerializationSettings(enabled, verifyEquality, verifyCommands, verifyEvents, verifyState)
    }
  }

  /**
   * Factory method to create a new EventSourcedBehaviorTestKit.
   */
  def create[Command, Event, State](
      system: ActorSystem[_],
      behavior: Behavior[Command]): EventSourcedBehaviorTestKit[Command, Event, State] =
    create(system, behavior, enabledSerializationSettings)

  /**
   * Factory method to create a new EventSourcedBehaviorTestKit with custom [[SerializationSettings]].
   *
   * Note that `equals` must be implemented in the commands, events and state if `verifyEquality` is enabled.
   */
  def create[Command, Event, State](
      system: ActorSystem[_],
      behavior: Behavior[Command],
      serializationSettings: SerializationSettings): EventSourcedBehaviorTestKit[Command, Event, State] = {
    val scaladslSettings = new scaladsl.EventSourcedBehaviorTestKit.SerializationSettings(
      enabled = serializationSettings.enabled,
      verifyEquality = serializationSettings.verifyEquality,
      verifyCommands = serializationSettings.verifyCommands,
      verifyEvents = serializationSettings.verifyEvents,
      verifyState = serializationSettings.verifyState)
    new EventSourcedBehaviorTestKit(scaladsl.EventSourcedBehaviorTestKit(system, behavior, scaladslSettings))
  }

  /**
   * The result of running a command.
   */
  @DoNotInherit class CommandResult[Command, Event, State](
      delegate: scaladsl.EventSourcedBehaviorTestKit.CommandResult[Command, Event, State]) {

    /**
     * The command that was run.
     */
    def command: Command =
      delegate.command

    /**
     * The events that were emitted by the command, and persisted.
     * In many cases only one event is emitted and then it's more convenient to use [[CommandResult.event]]
     * or [[CommandResult.eventOfType]].
     */
    def events: JList[Event] =
      delegate.events.asJava

    /**
     * `true` if no events were emitted by the command.
     */
    def hasNoEvents: Boolean =
      delegate.hasNoEvents

    /**
     * The first event. It will throw `AssertionError` if there is no event.
     */
    def event: Event =
      delegate.event

    /**
     * The first event as a given expected type. It will throw `AssertionError` if there is no event or
     * if the event is of a different type.
     */
    def eventOfType[E <: Event](eventClass: Class[E]): E =
      delegate.eventOfType(ClassTag[E](eventClass))

    /**
     * The state after applying the events.
     */
    def state: State =
      delegate.state

    /**
     * The state as a given expected type. It will throw `AssertionError` if the state is of a different type.
     */
    def stateOfType[S <: State](stateClass: Class[S]): S =
      delegate.stateOfType(ClassTag[S](stateClass))
  }

  /**
   * The result of running a command with a `ActorRef<R> replyTo`, i.e. the `runCommand` with a
   * `Function<ActorRef<R>, Command>` parameter.
   */
  final class CommandResultWithReply[Command, Event, State, Reply](
      delegate: scaladsl.EventSourcedBehaviorTestKit.CommandResultWithReply[Command, Event, State, Reply])
      extends CommandResult[Command, Event, State](delegate) {

    /**
     * The reply. It will throw `AssertionError` if there was no reply.
     */
    def reply: Reply =
      delegate.reply

    /**
     * The reply as a given expected type.  It will throw `AssertionError` if there is no reply or
     * if the reply is of a different type.
     */
    def replyOfType[R <: Reply](replyClass: Class[R]): R =
      delegate.replyOfType(ClassTag[R](replyClass))

    /**
     * `true` if there is no reply.
     */
    def hasNoReply: Boolean = delegate.hasNoReply

  }

  /**
   * The result of restarting the behavior.
   */
  final class RestartResult[State](delegate: scaladsl.EventSourcedBehaviorTestKit.RestartResult[State]) {

    /**
     * The state after recovery.
     */
    def state: State =
      delegate.state
  }

}

@ApiMayChange
final class EventSourcedBehaviorTestKit[Command, Event, State](
    delegate: scaladsl.EventSourcedBehaviorTestKit[Command, Event, State]) {

  import EventSourcedBehaviorTestKit._

  private val _persistenceTestKit = new PersistenceTestKit(delegate.persistenceTestKit)
  private val _snapshotTestKit = {
    import scala.jdk.OptionConverters._
    delegate.snapshotTestKit.map(new SnapshotTestKit(_)).toJava
  }

  /**
   * Run one command through the behavior. The returned result contains emitted events and the state
   * after applying the events.
   */
  def runCommand(command: Command): CommandResult[Command, Event, State] =
    new CommandResult(delegate.runCommand(command))

  /**
   * Run one command  with a `replyTo: ActorRef` through the behavior. The returned result contains emitted events,
   * the state after applying the events, and the reply.
   */
  def runCommand[R](creator: JFunction[ActorRef[R], Command]): CommandResultWithReply[Command, Event, State, R] =
    new CommandResultWithReply(delegate.runCommand(replyTo => creator.apply(replyTo)))

  /**
   * Retrieve the current state of the Behavior.
   */
  def getState(): State =
    delegate.getState()

  /**
   * Restart the behavior, which will then recover from stored snapshot and events. Can be used for testing
   * that the recovery is correct.
   */
  def restart(): RestartResult[State] =
    new RestartResult(delegate.restart())

  /**
   * Clears the in-memory journal and snapshot storage and restarts the behavior.
   */
  def clear(): Unit =
    delegate.clear()

  /**
   * Initializes behavior from provided state and/or events.
   */
  @varargs
  def initialize(state: State, events: Event*): Unit = delegate.initialize(state, events: _*)
  @varargs
  def initialize(events: Event*): Unit = delegate.initialize(events: _*)

  /**
   * The underlying `PersistenceTestKit` for the in-memory journal.
   * Can be useful for advanced testing scenarios, such as simulating failures or
   * populating the journal with events that are used for replay.
   */
  def persistenceTestKit: PersistenceTestKit =
    _persistenceTestKit

  /**
   * The underlying `SnapshotTestKit` for snapshot storage. Present only if snapshots are enabled.
   * Can be useful for advanced testing scenarios, such as simulating failures or
   * populating the storage with snapshots that are used for replay.
   */
  def snapshotTestKit: Optional[SnapshotTestKit] = _snapshotTestKit
}
