/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.ActorLogging
import akka.annotation.InternalApi
import akka.persistence._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.testkit.internal.{ InMemStorageExtension, SnapshotStorageEmulatorExtension }
import akka.persistence.testkit.internal.CurrentTime
import akka.util.unused

/**
 * INTERNAL API
 *
 * Persistence testkit plugin for events.
 */
@InternalApi
class PersistenceTestKitPlugin(@unused cfg: Config, cfgPath: String) extends AsyncWriteJournal with ActorLogging {

  private final val storage = {
    log.debug("Using in memory storage [{}] for test kit journal", cfgPath)
    InMemStorageExtension(context.system).storageFor(cfgPath)
  }
  private val eventStream = context.system.eventStream

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    Future.fromTry(Try(messages.map(aw => {
      val timestamp = CurrentTime.now()
      val data = aw.payload.map(pl =>
        pl.payload match {
          case _ => pl.withTimestamp(timestamp)
        })

      val result: Try[Unit] = storage.tryAdd(data)
      result.foreach { _ =>
        messages.foreach { aw =>
          eventStream.publish(PersistenceTestKitPlugin.Write(aw.persistenceId, aw.highestSequenceNr))
        }
      }
      result
    })))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.fromTry(Try(storage.tryDelete(persistenceId, toSequenceNr)))

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] =
    Future.fromTry(
      Try(
        storage
          .tryRead(persistenceId, fromSequenceNr, toSequenceNr, max)
          .map { repr =>
            // we keep the tags in the repr, so remove those here
            repr.payload match {
              case Tagged(payload, _) => repr.withPayload(payload)
              case _                  => repr
            }

          }
          .foreach(recoveryCallback)))

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.fromTry(Try {
      val found = storage.tryReadSeqNumber(persistenceId)
      if (found < fromSequenceNr) fromSequenceNr else found
    })

}

object PersistenceTestKitPlugin {

  val PluginId = "akka.persistence.testkit.journal"

  import akka.util.ccompat.JavaConverters._

  def getInstance() = this

  val config: Config = ConfigFactory.parseMap(
    Map(
      "akka.persistence.journal.plugin" -> PluginId,
      s"$PluginId.class" -> s"${classOf[PersistenceTestKitPlugin].getName}").asJava)

  private[testkit] case class Write(persistenceId: String, toSequenceNr: Long)

}

/**
 * INTERNAL API
 *
 * Persistence testkit plugin for snapshots.
 */
@InternalApi
class PersistenceTestKitSnapshotPlugin extends SnapshotStore {

  private final val storage = SnapshotStorageEmulatorExtension(context.system)

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    Future.fromTry(Try(storage.tryRead(persistenceId, criteria)))

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    Future.fromTry(Try(storage.tryAdd(metadata, snapshot)))

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    Future.fromTry(Try(storage.tryDelete(metadata)))

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future.fromTry(Try(storage.tryDelete(persistenceId, criteria)))

}

object PersistenceTestKitSnapshotPlugin {

  val PluginId = "akka.persistence.testkit.snapshotstore.pluginid"

  import akka.util.ccompat.JavaConverters._

  def getInstance() = this

  val config: Config = ConfigFactory.parseMap(
    Map(
      "akka.persistence.snapshot-store.plugin" -> PluginId,
      s"$PluginId.class" -> classOf[PersistenceTestKitSnapshotPlugin].getName,
      s"$PluginId.snapshot-is-optional" -> false, // fallback isn't used by the testkit
      s"$PluginId.only-one-snapshot" -> false // fallback isn't used by the testkit
    ).asJava)

}

object PersistenceTestKitDurableStateStorePlugin {

  val PluginId = "akka.persistence.testkit.state"

  import akka.util.ccompat.JavaConverters._

  def getInstance() = this

  val config: Config = ConfigFactory.parseMap(Map("akka.persistence.state.plugin" -> PluginId).asJava)
}
