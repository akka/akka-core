/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import scala.collection.immutable
import scala.concurrent.Await
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.SerializationTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.InternalApi
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.testkit.SnapshotMeta
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit.CommandResult
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit.CommandResultWithReply
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit.RestartResult
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit.SerializationSettings
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.persistence.testkit.scaladsl.SnapshotTestKit
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.EventSourcedBehaviorImpl
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetStateReply
import akka.stream.scaladsl.Sink

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventSourcedBehaviorTestKitImpl {
  final case class CommandResultImpl[Command, Event, State, Reply](
      command: Command,
      events: immutable.Seq[Event],
      state: State,
      replyOption: Option[Reply])
      extends CommandResultWithReply[Command, Event, State, Reply] {

    override def hasNoEvents: Boolean = events.isEmpty

    override def event: Event = {
      if (events.nonEmpty) events.head else throw new AssertionError("No events")
    }

    override def eventOfType[E <: Event: ClassTag]: E =
      ofType(event, "event")

    override def stateOfType[S <: State: ClassTag]: S =
      ofType(state, "state")

    override def reply: Reply = replyOption.getOrElse(throw new AssertionError("No reply"))

    override def replyOfType[R <: Reply: ClassTag]: R =
      ofType(reply, "reply")

    // cast with nice error message
    private def ofType[A: ClassTag](obj: Any, errorParam: String): A = {
      obj match {
        case a: A => a
        case other =>
          val expectedClass = implicitly[ClassTag[A]].runtimeClass
          throw new AssertionError(
            s"Expected $errorParam class [${expectedClass.getName}], " +
            s"but was [${other.getClass.getName}]")
      }
    }

    override def hasNoReply: Boolean = replyOption.isEmpty
  }

  final case class RestartResultImpl[State](state: State) extends RestartResult[State]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventSourcedBehaviorTestKitImpl[Command, Event, State](
    actorTestKit: ActorTestKit,
    behavior: Behavior[Command],
    serializationSettings: SerializationSettings)
    extends EventSourcedBehaviorTestKit[Command, Event, State] {

  import EventSourcedBehaviorTestKitImpl._

  private def system: ActorSystem[_] = actorTestKit.system
  if (system.settings.config.getBoolean("akka.persistence.testkit.events.serialize") ||
      system.settings.config.getBoolean("akka.persistence.testkit.snapshots.serialize")) {
    system.log.warn(
      "Persistence TestKit serialization enabled when using EventSourcedBehaviorTestKit, this is not intended. " +
      "make sure you create the system used in the test with the config from EventSourcedBehaviorTestKit.config " +
      "as described in the docs https://doc.akka.io/libraries/akka-core/current/typed/persistence-testing.html#unit-testing")
  }

  override val persistenceTestKit: PersistenceTestKit = PersistenceTestKit(system)
  persistenceTestKit.clearAll()

  override val snapshotTestKit: Option[SnapshotTestKit] =
    if (system.settings.config.getString("akka.persistence.snapshot-store.plugin") != "")
      Some(SnapshotTestKit(system))
    else None
  snapshotTestKit.foreach(_.clearAll())

  private val queries =
    PersistenceQuery(system).readJournalFor[CurrentEventsByPersistenceIdQuery](PersistenceTestKitReadJournal.Identifier)

  private val probe = actorTestKit.createTestProbe[Any]()
  private val stateProbe = actorTestKit.createTestProbe[GetStateReply[State]]()
  private var actor: ActorRef[Command] = actorTestKit.spawn(behavior)
  private def internalActor = actor.unsafeUpcast[Any]
  private val persistenceId: PersistenceId = {
    internalActor ! EventSourcedBehaviorImpl.GetPersistenceId(probe.ref)
    try {
      probe.expectMessageType[PersistenceId]
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException("Only EventSourcedBehavior, or nested EventSourcedBehavior allowed.")
    }
  }
  private val serializationTestKit = new SerializationTestKit(system)

  private var emptyStateVerified = false

  override def runCommand(command: Command): CommandResult[Command, Event, State] = {
    preCommandCheck(command)
    val seqNrBefore = getHighestSeqNr()

    actor ! command

    val newState = getState()
    val newEvents = getEvents(seqNrBefore + 1)

    postCommandCheck(newEvents, newState, reply = None)

    CommandResultImpl[Command, Event, State, Nothing](command, newEvents, newState, None)
  }

  override def runCommand[R](creator: ActorRef[R] => Command): CommandResultWithReply[Command, Event, State, R] = {
    val replyProbe = actorTestKit.createTestProbe[R]()
    val command = creator(replyProbe.ref)
    preCommandCheck(command)
    val seqNrBefore = getHighestSeqNr()

    actor ! command

    val reply = try {
      replyProbe.receiveMessage()
    } catch {
      case NonFatal(_) =>
        throw new AssertionError(s"Missing expected reply for command [$command].")
    } finally {
      replyProbe.stop()
    }

    val newState = getState()
    val newEvents = getEvents(seqNrBefore + 1)

    postCommandCheck(newEvents, newState, Some(reply))

    CommandResultImpl[Command, Event, State, R](command, newEvents, newState, Some(reply))
  }

  private def getHighestSeqNr(): Long = {
    implicit val sys: ActorSystem[_] = system
    val result =
      queries.currentEventsByPersistenceId(persistenceId.id, 0L, toSequenceNr = Long.MaxValue).runWith(Sink.lastOption)

    Await.result(result, actorTestKit.testKitSettings.SingleExpectDefaultTimeout) match {
      case None      => 0L
      case Some(env) => env.sequenceNr
    }
  }

  private def getEvents(fromSeqNr: Long): immutable.Seq[Event] = {
    implicit val sys: ActorSystem[_] = system
    val result =
      queries.currentEventsByPersistenceId(persistenceId.id, fromSeqNr, toSequenceNr = Long.MaxValue).runWith(Sink.seq)

    Await.result(result, actorTestKit.testKitSettings.SingleExpectDefaultTimeout).map(_.event.asInstanceOf[Event])
  }

  override def getState(): State = {
    internalActor ! EventSourcedBehaviorImpl.GetState(stateProbe.ref)
    stateProbe.receiveMessage().currentState
  }

  private def preCommandCheck(command: Command): Unit = {
    if (serializationSettings.enabled) {
      if (serializationSettings.verifyCommands)
        verifySerializationAndThrow(command, "Command")

      if (serializationSettings.verifyState && !emptyStateVerified) {
        val emptyState = getState()
        verifySerializationAndThrow(emptyState, "Empty State")
        emptyStateVerified = true
      }
    }
  }

  private def postCommandCheck(newEvents: immutable.Seq[Event], newState: State, reply: Option[Any]): Unit = {
    if (serializationSettings.enabled) {
      if (serializationSettings.verifyEvents) {
        newEvents.foreach(verifySerializationAndThrow(_, "Event"))
      }

      if (serializationSettings.verifyState)
        verifySerializationAndThrow(newState, "State")

      if (serializationSettings.verifyCommands) {
        reply.foreach(verifySerializationAndThrow(_, "Reply"))
      }
    }
  }

  override def restart(): RestartResult[State] = {
    actorTestKit.stop(actor)
    actor = actorTestKit.spawn(behavior)
    internalActor ! EventSourcedBehaviorImpl.GetState(stateProbe.ref)
    try {
      val state = stateProbe.receiveMessage()
      RestartResultImpl(state.currentState)
    } catch {
      case NonFatal(_) =>
        throw new IllegalStateException("Could not restart. Maybe exception from event handler. See logs.")
    }
  }

  override def clear(): Unit = {
    persistenceTestKit.clearByPersistenceId(persistenceId.id)
    snapshotTestKit.foreach(_.clearByPersistenceId(persistenceId.id))
    restart()
  }

  private def verifySerializationAndThrow(obj: Any, errorMessagePrefix: String): Unit = {
    try {
      serializationTestKit.verifySerialization(obj, serializationSettings.verifyEquality)
    } catch {
      case NonFatal(exc) =>
        throw new IllegalArgumentException(s"$errorMessagePrefix [$obj] isn't serializable.", exc)
    }
  }

  override def initialize(state: State, events: Event*): Unit = internalInitialize(Some(state), events: _*)

  override def initialize(events: Event*): Unit = internalInitialize(None, events: _*)

  private def internalInitialize(stateOption: Option[State], events: Event*) = {
    clear()

    stateOption.foreach { state =>
      snapshotTestKit match {
        case Some(kit) => kit.persistForRecovery(persistenceId.id, (SnapshotMeta(0), state))
        case _         => throw new IllegalArgumentException("Cannot initialize from state when snapshots are not used.")
      }
    }
    persistenceTestKit.persistForRecovery(persistenceId.id, collection.immutable.Seq.empty ++ events)

    restart()
  }
}
