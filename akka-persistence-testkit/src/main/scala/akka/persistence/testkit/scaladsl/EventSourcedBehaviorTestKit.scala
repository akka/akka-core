/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import scala.collection.immutable
import scala.reflect.ClassTag

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.testkit.internal.EventSourcedBehaviorTestKitImpl

/**
 * Testing of [[akka.persistence.typed.scaladsl.EventSourcedBehavior]] implementations.
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
   * constructor parameter to `ScalaTestWithActorTestKit`. The configuration enables the in-memory
   * journal and snapshot storage.
   */
  val config: Config = ConfigFactory.parseString("""
    akka.persistence.testkit.events.serialize = off
    akka.persistence.testkit.snapshots.serialize = off
    """).withFallback(PersistenceTestKitPlugin.config).withFallback(PersistenceTestKitSnapshotPlugin.config)

  object SerializationSettings {
    val enabled: SerializationSettings = new SerializationSettings(
      enabled = true,
      verifyEquality = false,
      verifyCommands = true,
      verifyEvents = true,
      verifyState = true)

    val disabled: SerializationSettings = new SerializationSettings(
      enabled = false,
      verifyEquality = false,
      verifyCommands = false,
      verifyEvents = false,
      verifyState = false)
  }

  /**
   * Customization of which serialization checks that are performed.
   * `equals` must be implemented (or using `case class`) when `verifyEquality` is enabled.
   */
  final class SerializationSettings private[akka] (
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
  def apply[Command, Event, State](
      system: ActorSystem[_],
      behavior: Behavior[Command]): EventSourcedBehaviorTestKit[Command, Event, State] =
    apply(system, behavior, SerializationSettings.enabled)

  /**
   * Factory method to create a new EventSourcedBehaviorTestKit with custom [[SerializationSettings]].
   *
   * Note that `equals` must be implemented (or using `case class`) in the commands, events and state if
   * `verifyEquality` is enabled.
   */
  def apply[Command, Event, State](
      system: ActorSystem[_],
      behavior: Behavior[Command],
      serializationSettings: SerializationSettings): EventSourcedBehaviorTestKit[Command, Event, State] =
    new EventSourcedBehaviorTestKitImpl(ActorTestKit(system), behavior, serializationSettings)

  /**
   * The result of running a command.
   */
  @DoNotInherit trait CommandResult[Command, Event, State] {

    /**
     * The command that was run.
     */
    def command: Command

    /**
     * The events that were emitted by the command, and persisted.
     * In many cases only one event is emitted and then it's more convenient to use [[CommandResult.event]]
     * or [[CommandResult.eventOfType]].
     */
    def events: immutable.Seq[Event]

    /**
     * `true` if no events were emitted by the command.
     */
    def hasNoEvents: Boolean

    /**
     * The first event. It will throw `AssertionError` if there is no event.
     */
    def event: Event

    /**
     * The first event as a given expected type. It will throw `AssertionError` if there is no event or
     * if the event is of a different type.
     */
    def eventOfType[E <: Event: ClassTag]: E

    /**
     * The state after applying the events.
     */
    def state: State

    /**
     * The state as a given expected type. It will throw `AssertionError` if the state is of a different type.
     */
    def stateOfType[S <: State: ClassTag]: S
  }

  /**
   * The result of running a command with a `replyTo: ActorRef[R]`, i.e. the `runCommand` with a
   * `ActorRef[R] => Command` parameter.
   */
  @DoNotInherit trait CommandResultWithReply[Command, Event, State, Reply]
      extends CommandResult[Command, Event, State] {

    /**
     * The reply. It will throw `AssertionError` if there was no reply.
     */
    def reply: Reply

    /**
     * The reply as a given expected type.  It will throw `AssertionError` if there is no reply or
     * if the reply is of a different type.
     */
    def replyOfType[R <: Reply: ClassTag]: R

    /**
     * `true` if there is no reply.
     */
    def hasNoReply: Boolean

  }

  /**
   * The result of restarting the behavior.
   */
  @DoNotInherit trait RestartResult[State] {

    /**
     * The state after recovery.
     */
    def state: State
  }

}

@ApiMayChange
@DoNotInherit trait EventSourcedBehaviorTestKit[Command, Event, State] {

  import EventSourcedBehaviorTestKit._

  /**
   * Run one command through the behavior. The returned result contains emitted events and the state
   * after applying the events.
   */
  def runCommand(command: Command): CommandResult[Command, Event, State]

  /**
   * Run one command  with a `replyTo: ActorRef[R]` through the behavior. The returned result contains emitted events,
   * the state after applying the events, and the reply.
   */
  def runCommand[R](creator: ActorRef[R] => Command): CommandResultWithReply[Command, Event, State, R]

  /**
   * Retrieve the current state of the Behavior.
   */
  def getState(): State

  /**
   * Restart the behavior, which will then recover from stored snapshot and events. Can be used for testing
   * that the recovery is correct.
   */
  def restart(): RestartResult[State]

  /**
   * Clears the in-memory journal and snapshot storage and restarts the behavior.
   */
  def clear(): Unit

  /**
   * The underlying `PersistenceTestKit` for the in-memory journal.
   * Can be useful for advanced testing scenarios, such as simulating failures or
   * populating the journal with events that are used for replay.
   */
  def persistenceTestKit: PersistenceTestKit

  /**
   * The underlying `SnapshotTestKit` for snapshot storage. Present only if snapshots are enabled.
   * Can be useful for advanced testing scenarios, such as simulating failures or
   * populating the storage with snapshots that are used for replay.
   */
  def snapshotTestKit: Option[SnapshotTestKit]

  /**
   * Initializes behavior from provided state and/or events.
   */
  def initialize(state: State, events: Event*): Unit
  def initialize(events: Event*): Unit
}
