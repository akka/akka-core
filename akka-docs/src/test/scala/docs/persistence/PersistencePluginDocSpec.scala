/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence

import scala.collection.immutable
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config._
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.duration._

//#plugin-imports
import akka.persistence._
import akka.persistence.journal._
import akka.persistence.snapshot._

//#plugin-imports

object PersistencePluginDocSpec {
  val config =
    """
      # Path to the journal plugin to be used
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"

      //#local-snapshot-plugin-config
      # Path to the snapshot store plugin to be used
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      //#local-snapshot-plugin-config

      //#snapshot-config
      akka.persistence.snapshot-store.local.dir = "target/snapshots"
      //#snapshot-config
    """
}

class PersistencePluginDocSpec extends AnyWordSpec {
  {
    val providerConfig =
      """
        //#journal-plugin-config
        # Path to the journal plugin to be used
        akka.persistence.journal.plugin = "my-journal"

        # My custom journal plugin
        my-journal {
          # Class name of the plugin.
          class = "docs.persistence.MyJournal"
          # Dispatcher for the plugin actor.
          plugin-dispatcher = "akka.actor.default-dispatcher"
        }
        //#journal-plugin-config

        //#snapshot-store-plugin-config
        # Path to the snapshot store plugin to be used
        akka.persistence.snapshot-store.plugin = "my-snapshot-store"

        # My custom snapshot store plugin
        my-snapshot-store {
          # Class name of the plugin.
          class = "docs.persistence.MySnapshotStore"
          # Dispatcher for the plugin actor.
          plugin-dispatcher = "akka.actor.default-dispatcher"
        }
        //#snapshot-store-plugin-config
      """

    val system = ActorSystem(
      "PersistencePluginDocSpec",
      ConfigFactory
        .parseString(providerConfig)
        .withFallback(ConfigFactory.parseString(PersistencePluginDocSpec.config)))
    try {
      Persistence(system)
    } finally {
      TestKit.shutdownActorSystem(system, 10.seconds, false)
    }
  }
}

class MyJournal extends AsyncWriteJournal {
  //#sync-journal-plugin-api
  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    Future.fromTry(Try {
      // blocking call here
      ???
    })
  //#sync-journal-plugin-api

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = ???
  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit): Future[Unit] = ???
  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = ???

  // optionally override:
  override def receivePluginInternal: Receive = super.receivePluginInternal
}

class MySnapshotStore extends SnapshotStore {
  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = ???
  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = ???
  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = ???

  // optionally override:
  override def receivePluginInternal: Receive = super.receivePluginInternal
}

object PersistenceTCKDoc {
  object example1 {
    import akka.persistence.journal.JournalSpec

    //#journal-tck-scala
    class MyJournalSpec
        extends JournalSpec(
          config = ConfigFactory.parseString("""akka.persistence.journal.plugin = "my.journal.plugin"""")) {

      override def supportsRejectingNonSerializableObjects: CapabilityFlag =
        false // or CapabilityFlag.off

      override def supportsSerialization: CapabilityFlag =
        true // or CapabilityFlag.on
    }
    //#journal-tck-scala
  }
  object example2 {
    import akka.persistence.snapshot.SnapshotStoreSpec

    //#snapshot-store-tck-scala
    class MySnapshotStoreSpec
        extends SnapshotStoreSpec(
          config = ConfigFactory.parseString("""
        akka.persistence.snapshot-store.plugin = "my.snapshot-store.plugin"
        """)) {

      override def supportsSerialization: CapabilityFlag =
        true // or CapabilityFlag.on
    }
    //#snapshot-store-tck-scala
  }
  object example3 {
    import java.io.File

    import akka.persistence.journal.JournalSpec
    import org.apache.commons.io.FileUtils

    //#journal-tck-before-after-scala
    class MyJournalSpec
        extends JournalSpec(config = ConfigFactory.parseString("""
        akka.persistence.journal.plugin = "my.journal.plugin"
        """)) {

      override def supportsRejectingNonSerializableObjects: CapabilityFlag =
        true // or CapabilityFlag.on

      val storageLocations = List(
        new File(system.settings.config.getString("my.journal.plugin.dir")),
        new File(config.getString("akka.persistence.snapshot-store.local.dir")))

      override def beforeAll(): Unit = {
        super.beforeAll()
        storageLocations.foreach(FileUtils.deleteDirectory)
      }

      override def afterAll(): Unit = {
        storageLocations.foreach(FileUtils.deleteDirectory)
        super.afterAll()
      }

    }
    //#journal-tck-before-after-scala
  }
}
