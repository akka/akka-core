/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import scala.reflect.ClassTag

import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.typed.Behavior
import akka.annotation.DoNotInherit
import akka.persistence.testkit.internal.{ PersistenceProbeImpl, Unpersistent }

sealed trait UnpersistentBehavior[Command, State] {
  val behavior: Behavior[Command]
  lazy val behaviorTestKit = BehaviorTestKit(behavior)

  def stateProbe: PersistenceProbe[State]
}

object UnpersistentBehavior {

  /** Given an EventSourcedBehavior, produce a non-persistent Behavior which synchronously publishes events and snapshots
   *  for inspection.  State is updated as in the EventSourcedBehavior, and side effects are performed synchronously.  The
   *  resulting Behavior is, contingent on the command handling, event handling, and side effects being compatible with the
   *  BehaviorTestKit, testable with the BehaviorTestKit.
   *
   *  The returned Behavior does not intrinsically depend on configuration: it therefore does not serialize and assumes an
   *  unbounded stash for commands.
   */
  def fromEventSourced[Command, Event, State](
      behavior: Behavior[Command],
      initialStateAndSequenceNr: Option[(State, Long)] = None): EventSourced[Command, Event, State] = {
    val eventProbe = new PersistenceProbeImpl[Event]
    val snapshotProbe = new PersistenceProbeImpl[State]
    val resultingBehavior =
      Unpersistent.eventSourced(behavior, initialStateAndSequenceNr) {
        (event: Event, sequenceNr: Long, tags: Set[String]) =>
          eventProbe.persist((event, sequenceNr, tags))
      } { (snapshot, sequenceNr) =>
        snapshotProbe.persist((snapshot, sequenceNr, Set.empty))
      }

    EventSourced(resultingBehavior, eventProbe.asScala, snapshotProbe.asScala)
  }

  def fromEventSourced[Command, Event, State](
      behavior: Behavior[Command],
      initialState: State): EventSourced[Command, Event, State] =
    fromEventSourced(behavior, Some(initialState -> 0L))

  def fromDurableState[Command, State](
      behavior: Behavior[Command],
      initialState: Option[State] = None): DurableState[Command, State] = {
    val probe = new PersistenceProbeImpl[State]

    val resultingBehavior =
      Unpersistent.durableState(behavior, initialState) { (state, version, tag) =>
        probe.persist((state, version, if (tag.isEmpty) Set.empty else Set(tag)))
      }

    DurableState(resultingBehavior, probe.asScala)
  }

  final case class EventSourced[Command, Event, State](
      override val behavior: Behavior[Command],
      val eventProbe: PersistenceProbe[Event],
      override val stateProbe: PersistenceProbe[State])
      extends UnpersistentBehavior[Command, State] {
    def apply(f: (BehaviorTestKit[Command], PersistenceProbe[Event], PersistenceProbe[State]) => Unit): Unit =
      f(behaviorTestKit, eventProbe, stateProbe)

    def snapshotProbe: PersistenceProbe[State] = stateProbe
  }

  final case class DurableState[Command, State](
      override val behavior: Behavior[Command],
      override val stateProbe: PersistenceProbe[State])
      extends UnpersistentBehavior[Command, State] {
    def apply(f: (BehaviorTestKit[Command], PersistenceProbe[State]) => Unit): Unit =
      f(behaviorTestKit, stateProbe)
  }
}

final case class PersistenceEffect[T](persistedObject: T, sequenceNr: Long, tags: Set[String])

/**
 * Not for user extension
 */
@DoNotInherit
trait PersistenceProbe[T] {

  /** Collect all persistence effects from the probe and empty the probe */
  def drain(): Seq[PersistenceEffect[T]]

  /** Get and remove the oldest persistence effect from the probe */
  def extract(): PersistenceEffect[T]

  /** Get and remove the oldest persistence effect from the probe, failing if the
   *  persisted object is not of the requested type
   */
  def expectPersistedType[S <: T: ClassTag](): PersistenceEffect[S]

  /** Are there any persistence effects? */
  def hasEffects: Boolean

  /** Assert that the given object was persisted in the oldest persistence effect and
   *  remove that persistence effect
   */
  def expectPersisted(obj: T): PersistenceProbe[T]

  /** Assert that the given object was persisted with the given tag in the oldest
   *  persistence effect and remove that persistence effect.  If the persistence
   *  effect has multiple tags, only one of them has to match in order for the
   *  assertion to succeed.
   */
  def expectPersisted(obj: T, tag: String): PersistenceProbe[T]

  /** Assert that the given object was persisted with the given tags in the oldest
   *  persistence effect and remove that persistence effect.  If the persistence
   *  effect has tags which are not given, the assertion fails.
   */
  def expectPersisted(obj: T, tags: Set[String]): PersistenceProbe[T]
}
