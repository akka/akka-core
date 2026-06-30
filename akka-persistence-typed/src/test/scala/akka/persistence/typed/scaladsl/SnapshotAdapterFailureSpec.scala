/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.PostStop
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.RecoveryFailed
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.SnapshotCompleted
import akka.persistence.typed.internal.JournalFailureException
import akka.serialization.jackson.CborSerializable

object SnapshotAdapterFailureSpec {

  // a second snapshot store, identical to the local one but with snapshot-is-optional = true
  private val conf: Config = ConfigFactory.parseString(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/SnapshotAdapterFailureSpec-${UUID.randomUUID().toString}"
      optional-snapshot-store = $${akka.persistence.snapshot-store.local}
      optional-snapshot-store.snapshot-is-optional = true
      optional-snapshot-store.dir = "target/SnapshotAdapterFailureSpec-optional-${UUID.randomUUID().toString}"
    """).withFallback(ConfigFactory.defaultReference()).resolve()

  final case class State(value: String) extends CborSerializable

  // toJournal is identity, fromJournal always throws to simulate a failing migration/transformation
  private val failingAdapter: SnapshotAdapter[State] = new SnapshotAdapter[State] {
    override def toJournal(state: State): Any = state
    override def fromJournal(from: Any): State =
      throw new RuntimeException("snapshot adapter failure")
  }
}

class SnapshotAdapterFailureSpec
    extends ScalaTestWithActorTestKit(SnapshotAdapterFailureSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {
  import SnapshotAdapterFailureSpec._

  private val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"sa-${pidCounter.incrementAndGet()}")

  private def behavior(
      pid: PersistenceId,
      snapshotPluginId: String,
      probe: ActorRef[String],
      failureProbe: ActorRef[Throwable]): EventSourcedBehavior[String, String, State] =
    EventSourcedBehavior[String, String, State](
      pid,
      State(""),
      commandHandler = { (state, command) =>
        command match {
          case "get" =>
            probe.tell(s"state[${state.value}]")
            Effect.none
          case _ =>
            Effect.persist(command)
        }
      },
      eventHandler = { (state, event) =>
        State(state.value + "|" + event)
      })
      .snapshotAdapter(failingAdapter)
      .withSnapshotPluginId(snapshotPluginId)
      .snapshotWhen { (_, event, _) =>
        event == "snap"
      }
      .receiveSignal {
        case (state, RecoveryCompleted) => probe.tell(s"recovered[${state.value}]")
        case (_, _: SnapshotCompleted)  => probe.tell("snapshot-saved")
        case (_, RecoveryFailed(cause)) => failureProbe.tell(cause)
        case (_, PostStop)              => probe.tell("stopped")
      }

  "Snapshot recovery when the snapshot adapter fails" must {

    "fail recovery and signal RecoveryFailed when fromJournal throws" in {
      val pid = nextPid()
      val probe = createTestProbe[String]()
      val failureProbe = createTestProbe[Throwable]()

      // first incarnation saves a snapshot (fromJournal is never called since there is no snapshot yet)
      val ref1 = spawn(behavior(pid, "akka.persistence.snapshot-store.local", probe.ref, failureProbe.ref))
      probe.expectMessage("recovered[]")
      ref1 ! "a"
      ref1 ! "snap"
      probe.expectMessage("snapshot-saved")
      testKit.stop(ref1)
      probe.expectMessage("stopped")

      // second incarnation loads the snapshot, the adapter throws -> RecoveryFailed + JournalFailureException + stop.
      // PostStop is delivered to the user signal handler in the snapshot recovery phase too (consistent with
      // event recovery failures), so "stopped" is observed (and not "recovered[...]", since recovery failed).
      LoggingTestKit.error[JournalFailureException].expect {
        spawn(behavior(pid, "akka.persistence.snapshot-store.local", probe.ref, failureProbe.ref))
        failureProbe.expectMessageType[RuntimeException].getMessage shouldBe "snapshot adapter failure"
        probe.expectMessage("stopped")
      }
    }

    "replay all events when fromJournal throws and snapshot-is-optional=true" in {
      val pid = nextPid()
      val probe = createTestProbe[String]()
      val failureProbe = createTestProbe[Throwable]()

      // first incarnation saves a snapshot in the optional snapshot store
      val ref1 = spawn(behavior(pid, "optional-snapshot-store", probe.ref, failureProbe.ref))
      probe.expectMessage("recovered[]")
      ref1 ! "a"
      ref1 ! "b"
      ref1 ! "snap"
      probe.expectMessage("snapshot-saved")
      testKit.stop(ref1)
      probe.expectMessage("stopped")

      // second incarnation: the adapter throws, but since the snapshot is optional all events are replayed
      val ref2 = spawn(behavior(pid, "optional-snapshot-store", probe.ref, failureProbe.ref))
      probe.expectMessage("recovered[|a|b|snap]")
      failureProbe.expectNoMessage() // RecoveryFailed was not signalled
      ref2 ! "get"
      probe.expectMessage("state[|a|b|snap]")
      testKit.stop(ref2)
    }
  }
}
