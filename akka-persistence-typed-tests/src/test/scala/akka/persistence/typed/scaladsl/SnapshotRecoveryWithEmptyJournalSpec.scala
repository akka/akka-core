/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.io.File
import java.util.UUID

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.serialization.Snapshot
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension

object SnapshotRecoveryWithEmptyJournalSpec {
  val survivingSnapshotPath = s"target/survivingSnapshotPath-${UUID.randomUUID().toString}"

  def conf: Config = PersistenceTestKitPlugin.config.withFallback(ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "${SnapshotRecoveryWithEmptyJournalSpec.survivingSnapshotPath}"
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """))

  object TestActor {
    def apply(name: String, probe: ActorRef[Any]): Behavior[String] = {
      Behaviors.setup { context =>
        EventSourcedBehavior[String, String, List[String]](
          PersistenceId.ofUniqueId(name),
          Nil,
          (state, cmd) =>
            cmd match {
              case "get" =>
                probe ! state.reverse
                Effect.none
              case _ =>
                Effect.persist(s"$cmd-${EventSourcedBehavior.lastSequenceNumber(context) + 1}")
            },
          (state, event) => event :: state)
      }
    }
  }

}

class SnapshotRecoveryWithEmptyJournalSpec
    extends ScalaTestWithActorTestKit(SnapshotRecoveryWithEmptyJournalSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {
  import SnapshotRecoveryWithEmptyJournalSpec._

  val snapshotsDir: File = new File(survivingSnapshotPath)

  val serializationExtension: Serialization = SerializationExtension(system)

  val persistenceId: String = system.name

  // Prepare a hand made snapshot file as basis for the recovery start point
  private def createSnapshotFile(sequenceNr: Long, ts: Long, data: Any): Unit = {
    val snapshotFile = new File(snapshotsDir, s"snapshot-$persistenceId-$sequenceNr-$ts")
    FileUtils.writeByteArrayToFile(snapshotFile, serializationExtension.serialize(Snapshot(data)).get)
  }

  val givenSnapshotSequenceNr: Long = 4711L
  val givenTimestamp: Long = 1000L

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    createSnapshotFile(givenSnapshotSequenceNr - 1, givenTimestamp - 1, List("a-1"))
    createSnapshotFile(givenSnapshotSequenceNr, givenTimestamp, List("b-2", "a-1"))
  }

  "A persistent actor in a system that only has snapshots and no previous journal activity" must {
    "recover its state and sequence number starting from the most recent snapshot and use subsequent sequence numbers to persist events to the journal" in {

      val probe = createTestProbe[Any]()
      val ref = spawn(TestActor(persistenceId, probe.ref))
      ref ! "c"
      ref ! "get"
      probe.expectMessage(List("a-1", "b-2", s"c-${givenSnapshotSequenceNr + 1}"))
    }
  }

}
