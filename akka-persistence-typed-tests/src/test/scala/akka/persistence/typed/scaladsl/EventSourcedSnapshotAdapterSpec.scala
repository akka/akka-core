/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.persistence.query.PersistenceQuery
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SnapshotAdapter
import akka.serialization.jackson.CborSerializable

object EventSourcedSnapshotAdapterSpec {

  case class State(s: String) extends CborSerializable
  case class Command(c: String) extends CborSerializable
  case class Event(e: String) extends CborSerializable
  case class PersistedState(s: String) extends CborSerializable
}

class EventSourcedSnapshotAdapterSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(PersistenceTestKitSnapshotPlugin.config))
    with AnyWordSpecLike
    with LogCapturing {
  import EventSourcedSnapshotAdapterSpec._

  import akka.actor.typed.scaladsl.adapter._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  val queries: PersistenceTestKitReadJournal =
    PersistenceQuery(system.toClassic)
      .readJournalFor[PersistenceTestKitReadJournal](PersistenceTestKitReadJournal.Identifier)

  private def behavior(pid: PersistenceId, probe: ActorRef[State]): EventSourcedBehavior[Command, Event, State] =
    EventSourcedBehavior[Command, Event, State](
      pid,
      State(""),
      commandHandler = { (state, command) =>
        command match {
          case Command(c) if c == "shutdown" =>
            Effect.stop()
          case Command(c) if c == "get" =>
            probe.tell(state)
            Effect.none
          case _ =>
            Effect.persist(Event(command.c)).thenRun(newState => probe ! newState)
        }
      },
      eventHandler = { (state, evt) =>
        state.copy(s = state.s + "|" + evt.e)
      })

  "Snapshot adapter" must {

    "adapt snapshots to any" in {
      val pid = nextPid()
      val stateProbe = TestProbe[State]()
      val snapshotFromJournal = TestProbe[PersistedState]()
      val snapshotToJournal = TestProbe[State]()
      val b = behavior(pid, stateProbe.ref)
        .snapshotAdapter(new SnapshotAdapter[State]() {
          override def toJournal(state: State): Any = {
            snapshotToJournal.ref.tell(state)
            PersistedState(state.s)
          }
          override def fromJournal(from: Any): State = from match {
            case ps: PersistedState =>
              snapshotFromJournal.ref.tell(ps)
              State(ps.s)
            case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
          }
        })
        .snapshotWhen { (_, event, _) =>
          event.e.contains("snapshot")
        }

      val ref = spawn(b)

      ref.tell(Command("one"))
      stateProbe.expectMessage(State("|one"))
      ref.tell(Command("snapshot now"))
      stateProbe.expectMessage(State("|one|snapshot now"))
      snapshotToJournal.expectMessage(State("|one|snapshot now"))
      ref.tell(Command("shutdown"))

      val ref2 = spawn(b)
      snapshotFromJournal.expectMessage(PersistedState("|one|snapshot now"))
      ref2.tell(Command("get"))
      stateProbe.expectMessage(State("|one|snapshot now"))
    }

  }
}
