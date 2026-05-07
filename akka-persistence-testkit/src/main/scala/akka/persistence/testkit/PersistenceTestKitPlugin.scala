/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import scala.annotation.nowarn
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

/**
 * INTERNAL API
 *
 * Persistence testkit plugin for events.
 */
@InternalApi
class PersistenceTestKitPlugin(@nowarn("msg=never used") cfg: Config, cfgPath: String)
    extends AsyncWriteJournal
    with ActorLogging {

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
    Future.fromTry(Try {
      val highest = storage.tryReadSeqNumber(persistenceId)
      if (highest != 0L && max != 0L) {
        val to = math.min(toSequenceNr, highest)
        // read only last when fromSequenceNr is -1
        val from = if (fromSequenceNr == -1) to else fromSequenceNr

        storage
          .tryRead(persistenceId, from, to, max)
          .map { repr =>
            // we keep the tags in the repr, so remove those here
            repr.payload match {
              case Tagged(payload, _) => repr.withPayload(payload)
              case _                  => repr
            }

          }
          .foreach(recoveryCallback)
      } else {
        Future.successful(())
      }
    })

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.fromTry(Try {
      val found = storage.tryReadSeqNumber(persistenceId)
      if (found < fromSequenceNr) fromSequenceNr else found
    })

}

object PersistenceTestKitPlugin {

  val PluginId = "akka.persistence.testkit.journal"

  import scala.jdk.CollectionConverters._

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

  import scala.jdk.CollectionConverters._

  def getInstance() = this

  val config: Config = ConfigFactory.parseMap(
    Map[String, Any](
      "akka.persistence.snapshot-store.plugin" -> PluginId,
      s"$PluginId.class" -> classOf[PersistenceTestKitSnapshotPlugin].getName,
      s"$PluginId.snapshot-is-optional" -> false, // fallback isn't used by the testkit
      s"$PluginId.only-one-snapshot" -> false // fallback isn't used by the testkit
    ).asJava)

}

object PersistenceTestKitDurableStateStorePlugin {

  val PluginId = "akka.persistence.testkit.state"

  import scala.jdk.CollectionConverters._

  def getInstance() = this

  val config: Config = ConfigFactory.parseMap(Map("akka.persistence.state.plugin" -> PluginId).asJava)
}
