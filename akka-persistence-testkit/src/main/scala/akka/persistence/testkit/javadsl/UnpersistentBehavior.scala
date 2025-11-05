/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import java.util.{ List, Set }

import scala.collection.immutable.{ Set => ScalaSet }

import akka.actor.testkit.typed.javadsl.BehaviorTestKit
import akka.actor.typed.Behavior
import akka.annotation.DoNotInherit
import akka.persistence.testkit.internal.{ PersistenceProbeImpl, Unpersistent }

object UnpersistentBehavior {

  /** Given an EventSourcedBehavior, produce a non-persistent Behavior which synchronously publishes events and snapshots
   *  for inspection.  State is updated as in the EventSourcedBehavior, and side effects are performed synchronously.  The
   *  resulting Behavior is, contingent on the command handling, event handling, and side effects being compatible with the
   *  BehaviorTestKit, testable with the BehaviorTestKit.
   *
   *  The returned Behavior does not intrinsically depend on configuration: it therefore does not serialize and
   *  assumes an unbounded stash for commands.
   *
   *  @param behavior a (possibly wrapped) EventSourcedBehavior to serve as the basis for the unpersistent behavior
   *  @param initialState start the unpersistent behavior with this state; if null, behavior's initialState will be used
   *  @param initialSequenceNr start the unpersistent behavior with this sequence number; only applies if initialState is non-null
   *  @return an UnpersistentBehavior based on an EventSourcedBehavior
   */
  def fromEventSourced[Command, Event, State](
      behavior: Behavior[Command],
      initialState: State,
      initialSequenceNr: Long): UnpersistentBehavior[Command, Event, State] = {
    require(initialSequenceNr >= 0, "initialSequenceNr must be at least zero")

    val initialStateAndSequenceNr = Option(initialState).map(_ -> initialSequenceNr)
    val eventProbe = new PersistenceProbeImpl[Event]
    val snapshotProbe = new PersistenceProbeImpl[State]

    val b =
      Unpersistent.eventSourced(behavior, initialStateAndSequenceNr) {
        (event: Event, sequenceNr: Long, tags: ScalaSet[String]) =>
          eventProbe.persist((event, sequenceNr, tags))
      } { (snapshot, sequenceNr) =>
        snapshotProbe.persist((snapshot, sequenceNr, ScalaSet.empty))
      }

    new UnpersistentBehavior(b, eventProbe.asJava, snapshotProbe.asJava)
  }

  def fromEventSourced[Command, Event, State](
      behavior: Behavior[Command]): UnpersistentBehavior[Command, Event, State] =
    fromEventSourced(behavior, null.asInstanceOf[State], 0)

  def fromDurableState[Command, State](
      behavior: Behavior[Command],
      initialState: State): UnpersistentBehavior[Command, Void, State] = {
    val probe = new PersistenceProbeImpl[State]
    val b =
      Unpersistent.durableState(behavior, Option(initialState)) { (state, version, tag) =>
        probe.persist((state, version, if (tag == "") ScalaSet.empty else ScalaSet(tag)))
      }

    new UnpersistentBehavior(b, noEventProbe, probe.asJava)
  }

  def fromDurableState[Command, State](behavior: Behavior[Command]): UnpersistentBehavior[Command, Void, State] =
    fromDurableState(behavior, null.asInstanceOf[State])

  private val noEventProbe: PersistenceProbe[Void] =
    new PersistenceProbe[Void] {
      def drain(): List[PersistenceEffect[Void]] =
        // could return an empty list, but the intent is that any use of this probe should fail the test
        boom()

      def extract(): PersistenceEffect[Void] = boom()
      def expectPersistedClass[S <: Void](clazz: Class[S]): PersistenceEffect[S] = boom()
      def hasEffects: Boolean = boom()
      def expectPersisted(obj: Void): PersistenceProbe[Void] = boom()
      def expectPersisted(obj: Void, tag: String): PersistenceProbe[Void] = boom()
      def expectPersisted(obj: Void, tags: Set[String]): PersistenceProbe[Void] = boom()

      private def boom() = throw new AssertionError("No events were persisted")
    }
}

final class UnpersistentBehavior[Command, Event, State] private (
    behavior: Behavior[Command],
    eventProbe: PersistenceProbe[Event],
    stateProbe: PersistenceProbe[State]) {
  def getBehavior(): Behavior[Command] = behavior
  def getBehaviorTestKit(): BehaviorTestKit[Command] = btk

  /** Note: durable state behaviors will not publish events to this probe */
  def getEventProbe(): PersistenceProbe[Event] = eventProbe

  def getStateProbe(): PersistenceProbe[State] = stateProbe
  def getSnapshotProbe(): PersistenceProbe[State] = stateProbe

  private lazy val btk = BehaviorTestKit.create(behavior)
}

final case class PersistenceEffect[T](persistedObject: T, sequenceNr: Long, tags: Set[String])

/**
 * Not for user extension
 */
@DoNotInherit
trait PersistenceProbe[T] {

  /** Collect all persistence effects from the probe and empty the probe */
  def drain(): List[PersistenceEffect[T]]

  /** Get and remove the oldest persistence effect from the probe */
  def extract(): PersistenceEffect[T]

  /** Get and remove the oldest persistence effect from the probe, failing if the
   *  persisted object is not of the requested type
   */
  def expectPersistedClass[S <: T](clazz: Class[S]): PersistenceEffect[S]

  /** Are there any persistence effects */
  def hasEffects: Boolean

  /** Assert that the given object was persisted in the oldest persistence effect and
   *  remove that persistence effect
   */
  def expectPersisted(obj: T): PersistenceProbe[T]

  /** Assert that the given object was persisted with the given tag in the oldest persistence
   *  effect and remove that persistence effect.  If the persistence effect has multiple tags,
   *  only one of them has to match in order for the assertion to succeed.
   */
  def expectPersisted(obj: T, tag: String): PersistenceProbe[T]

  /** Assert that the given object was persisted with the given tag in the oldest persistence
   *  effect and remove that persistence effect.  If the persistence effect has tags which are
   *  not given, the assertion fails.
   */
  def expectPersisted(obj: T, tags: Set[String]): PersistenceProbe[T]
}
