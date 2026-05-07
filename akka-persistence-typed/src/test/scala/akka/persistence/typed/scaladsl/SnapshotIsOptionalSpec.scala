/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.JsonCreator
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.typed.PersistenceId
import akka.serialization.jackson.CborSerializable

object SnapshotIsOptionalSpec {
  private val conf: Config = ConfigFactory.parseString(s"""
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/typed-persistence-${UUID.randomUUID().toString}"
    akka.persistence.snapshot-store.local.snapshot-is-optional = true
  """)
  case class State1(field1: String) extends CborSerializable {
    @JsonCreator
    def this() = this(null)

    if (field1 == null)
      throw new RuntimeException("Deserialization error")
  }
  case class Command(c: String) extends CborSerializable
  case class Event(e: String) extends CborSerializable
}

class SnapshotIsOptionalSpec
    extends ScalaTestWithActorTestKit(SnapshotIsOptionalSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {
  import SnapshotIsOptionalSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()})")

  private def behavior(pid: PersistenceId, probe: ActorRef[State1]): EventSourcedBehavior[Command, Event, State1] =
    EventSourcedBehavior[Command, Event, State1](
      pid,
      State1(""),
      commandHandler = { (state, command) =>
        command match {
          case Command("get") =>
            probe.tell(state)
            Effect.none
          case _ =>
            Effect.persist(Event(command.c)).thenRun(newState => probe ! newState)
        }
      },
      eventHandler = { (state, evt) =>
        state.copy(field1 = state.field1 + "|" + evt.e)
      })

  "Snapshot recovery with snapshot-is-optional=true" must {

    "fall back to events when deserialization error" in {
      val pid = nextPid()

      val stateProbe1 = createTestProbe[State1]()
      val b1 = behavior(pid, stateProbe1.ref).snapshotWhen { (_, event, _) =>
        event.e.contains("snapshot")
      }
      val ref1 = spawn(b1)
      ref1.tell(Command("one"))
      stateProbe1.expectMessage(State1("|one"))
      ref1.tell(Command("snapshot now"))
      stateProbe1.expectMessage(State1("|one|snapshot now"))
      testKit.stop(ref1)

      val stateProbe2 = createTestProbe[State1]()
      val ref2 = spawn(behavior(pid, stateProbe2.ref))
      ref2.tell(Command("get"))
      stateProbe2.expectMessage(State1("|one|snapshot now"))
      testKit.stop(ref2)
    }

    "fail fast if used with retention criteria with delete events" in {
      val pid = nextPid()

      val stateProbe1 = createTestProbe[State1]()
      val ref = spawn(
        behavior(pid, stateProbe1.ref).withRetention(RetentionCriteria.snapshotEvery(10, 3).withDeleteEventsOnSnapshot))
      createTestProbe().expectTerminated(ref)
    }

  }
}
