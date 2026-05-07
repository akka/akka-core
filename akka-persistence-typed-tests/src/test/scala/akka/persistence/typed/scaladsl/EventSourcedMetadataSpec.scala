/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.SnapshotCompleted

object EventSourcedMetadataSpec {

  private val conf = ConfigFactory.parseString(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
      akka.persistence.snapshot-store.plugin = "slow-snapshot-store"
      slow-snapshot-store.class = "${classOf[SlowInMemorySnapshotStore].getName}"
    """)

}

class EventSourcedMetadataSpec
    extends ScalaTestWithActorTestKit(EventSourcedMetadataSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private def behavior(pid: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup(
      ctx =>
        EventSourcedBehavior[String, String, String](pid, "", {
          (state, command) =>
            state match {
              case "stashing" =>
                command match {
                  case "unstash" =>
                    probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} unstash"
                    Effect.persistWithMetadata(EventWithMetadata("normal", "meta-unstashing")).thenUnstashAll()
                  case _ =>
                    Effect.stash()
                }
              case _ =>
                command match {
                  case "cmd" =>
                    probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} onCommand"
                    Effect
                      .persistWithMetadata(EventWithMetadata("evt", "meta"))
                      .thenRun(_ => probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} thenRun")
                  case "cmd3" =>
                    probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} onCommand"
                    Effect
                      .persistWithMetadata(
                        List(
                          EventWithMetadata("evt1", "meta-1"),
                          EventWithMetadata("evt2", "meta-2"),
                          EventWithMetadata("evt3", "meta-3")))
                      .thenRun(_ => probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} thenRun")
                  case "stash" =>
                    probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} stash"
                    Effect.persistWithMetadata(EventWithMetadata("stashing", "meta-stashing"))
                  case "snapshot" =>
                    Effect.persistWithMetadata(EventWithMetadata("snapshot", "meta-snapshot"))
                }
            }
        }, { (_, evt) =>
          probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} eventHandler $evt"
          evt
        }).snapshotWhen((_, event, _) => event == "snapshot").receiveSignal {
          case (_, RecoveryCompleted) =>
            probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} RecoveryCompleted"
          case (_, SnapshotCompleted(_)) =>
            probe ! s"${EventSourcedBehavior.currentMetadata[String](ctx)} SnapshotCompleted"
        })

  "The metadata" must {

    "be accessible in the handlers" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("ess-1"), probe.ref))
      probe.expectMessage("None RecoveryCompleted")

      ref ! "cmd"
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")

      ref ! "cmd"
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")

      ref ! "cmd3"
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta-1) eventHandler evt1")
      probe.expectMessage("Some(meta-2) eventHandler evt2")
      probe.expectMessage("Some(meta-3) eventHandler evt3")
      probe.expectMessage("None thenRun")

      testKit.stop(ref)
      probe.expectTerminated(ref)

      // and during replay
      val ref2 = spawn(behavior(PersistenceId.ofUniqueId("ess-1"), probe.ref))

      // This command will be handled after the replay.
      // It will most likely be stashed during the replay and it should see seqNr 5 when handled.
      // Reproducer of issue #32651
      ref2 ! "cmd"

      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("Some(meta-1) eventHandler evt1")
      probe.expectMessage("Some(meta-2) eventHandler evt2")
      probe.expectMessage("Some(meta-3) eventHandler evt3")
      probe.expectMessage("Some(meta-3) RecoveryCompleted")

      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")
    }

    "be available while unstashing" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("ess-2"), probe.ref))
      probe.expectMessage("None RecoveryCompleted")

      ref ! "stash"
      ref ! "cmd"
      ref ! "cmd"
      ref ! "cmd3"
      ref ! "unstash"
      probe.expectMessage("None stash")
      probe.expectMessage("Some(meta-stashing) eventHandler stashing")
      probe.expectMessage("None unstash")
      probe.expectMessage("Some(meta-unstashing) eventHandler normal")
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")
      probe.expectMessage("None onCommand") // cmd3
      probe.expectMessage("Some(meta-1) eventHandler evt1")
      probe.expectMessage("Some(meta-2) eventHandler evt2")
      probe.expectMessage("Some(meta-3) eventHandler evt3")
      probe.expectMessage("None thenRun")
    }

    "recover from snapshot metadata" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("ess-3"), probe.ref))
      probe.expectMessage("None RecoveryCompleted")

      ref ! "cmd"
      ref ! "snapshot"

      probe.expectMessage("None onCommand") // first command
      probe.expectMessage("Some(meta) eventHandler evt")
      probe.expectMessage("None thenRun")
      probe.expectMessage("Some(meta-snapshot) eventHandler snapshot")
      probe.expectMessage("Some(meta-snapshot) SnapshotCompleted")
      probe.expectNoMessage()

      Thread.sleep(1000) // FIXME

      val ref2 = spawn(behavior(PersistenceId.ofUniqueId("ess-3"), probe.ref))
      probe.expectMessage("Some(meta-snapshot) RecoveryCompleted")
      // no replayed events
      probe.expectNoMessage()
      ref2 ! "cmd3"
      probe.expectMessage("None onCommand")
      probe.expectMessage("Some(meta-1) eventHandler evt1")
      probe.expectMessage("Some(meta-2) eventHandler evt2")
      probe.expectMessage("Some(meta-3) eventHandler evt3")
      probe.expectMessage("None thenRun")
    }
  }
}
