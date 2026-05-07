/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import java.io.File

import scala.util.Random

import com.typesafe.config.Config
import org.apache.commons.io.FileUtils

import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.persistence.{ DeleteMessagesSuccess, PersistenceSpec, PersistentActor }
import akka.persistence.journal.leveldb.JournalCompactionSpec.EventLogger._
import akka.persistence.journal.leveldb.JournalCompactionSpec.SpecComponentBuilder
import akka.testkit.TestProbe

class JournalNoCompactionSpec
    extends JournalCompactionSpecBase(SpecComponentBuilder("leveldb-JournalNoCompactionSpec")) {

  "A LevelDB-based persistent actor" must {
    "NOT compact the journal if compaction is not activated by configuration" in {
      val watcher = TestProbe("watcher")
      val logger = createLogger(watcher.ref)
      val totalMessages = 2000
      val deletionBatchSize = 500
      var oldJournalSize = 0L

      for (i <- 0L.until(totalMessages)) {
        logger ! Generate
        watcher.expectMsg(Generated(i))
      }

      var newJournalSize = calculateJournalSize()
      assert(oldJournalSize < newJournalSize)

      var deletionSeqNr = deletionBatchSize - 1

      while (deletionSeqNr < totalMessages) {
        logger ! Delete(deletionSeqNr)
        watcher.expectMsg(DeleteMessagesSuccess(deletionSeqNr))

        oldJournalSize = newJournalSize
        newJournalSize = calculateJournalSize()
        assert(oldJournalSize <= newJournalSize)

        deletionSeqNr += deletionBatchSize
      }
    }
  }
}

class JournalCompactionSpec
    extends JournalCompactionSpecBase(SpecComponentBuilder("leveldb-JournalCompactionSpec", 500)) {

  "A LevelDB-based persistent actor" must {
    "compact the journal upon message deletions of configured persistence ids" in {
      val watcher = TestProbe("watcher")
      val logger = createLogger(watcher.ref)
      val totalMessages = 4 * builder.compactionInterval // 2000
      val deletionBatchSize = builder.compactionInterval // 500
      var oldJournalSize = 0L

      for (i <- 0L.until(totalMessages)) {
        logger ! Generate
        watcher.expectMsg(Generated(i))
      }

      var newJournalSize = calculateJournalSize()
      assert(oldJournalSize < newJournalSize)

      var deletionSeqNr = deletionBatchSize - 1

      while (deletionSeqNr < totalMessages) {
        logger ! Delete(deletionSeqNr)
        watcher.expectMsg(DeleteMessagesSuccess(deletionSeqNr))

        oldJournalSize = newJournalSize
        newJournalSize = calculateJournalSize()
        assert(oldJournalSize > newJournalSize)

        deletionSeqNr += deletionBatchSize
      }
    }
  }
}

class JournalCompactionThresholdSpec
    extends JournalCompactionSpecBase(SpecComponentBuilder("leveldb-JournalCompactionThresholdSpec", 500)) {

  "A LevelDB-based persistent actor" must {
    "compact the journal only after the threshold implied by the configured compaction interval has been exceeded" in {
      val watcher = TestProbe("watcher")
      val logger = createLogger(watcher.ref)
      val totalMessages = 2 * builder.compactionInterval // 1000
      val deletionBatchSize = builder.compactionInterval // 500
      var oldJournalSize = 0L

      for (i <- 0L.until(totalMessages)) {
        logger ! Generate
        watcher.expectMsg(Generated(i))
      }

      var newJournalSize = calculateJournalSize()
      assert(oldJournalSize < newJournalSize)

      var deletionSeqNr = deletionBatchSize - 2

      while (deletionSeqNr < totalMessages) {
        logger ! Delete(deletionSeqNr)
        watcher.expectMsg(DeleteMessagesSuccess(deletionSeqNr))

        oldJournalSize = newJournalSize
        newJournalSize = calculateJournalSize()

        if (deletionSeqNr < builder.compactionInterval - 1) {
          assert(oldJournalSize <= newJournalSize)
        } else {
          assert(oldJournalSize > newJournalSize)
        }

        deletionSeqNr += deletionBatchSize
      }
    }
  }
}

abstract class JournalCompactionSpecBase(val builder: SpecComponentBuilder) extends PersistenceSpec(builder.config) {

  def createLogger(watcher: ActorRef): ActorRef = builder.createLogger(system, watcher)

  def calculateJournalSize(): Long = FileUtils.sizeOfDirectory(journalDir)

  def journalDir: File = {
    val relativePath = system.settings.config.getString("akka.persistence.journal.leveldb.dir")
    new File(relativePath).getAbsoluteFile
  }

}

object JournalCompactionSpec {

  class SpecComponentBuilder(val specId: String, val compactionInterval: Long) {

    def config: Config = {
      PersistenceSpec.config(
        "leveldb",
        specId,
        extraConfig = Some(s"""
           | akka.persistence.journal.leveldb.compaction-intervals.$specId = $compactionInterval
        """.stripMargin))
    }

    def createLogger(system: ActorSystem, watcher: ActorRef): ActorRef = {
      system.actorOf(EventLogger.props(specId, watcher), "logger")
    }

  }

  object SpecComponentBuilder {

    def apply(specId: String): SpecComponentBuilder = apply(specId, 0)

    def apply(specId: String, compactionInterval: Long): SpecComponentBuilder = {
      new SpecComponentBuilder(specId, compactionInterval)
    }
  }

  object EventLogger {

    case object Generate

    case class Generated(seqNr: Long)

    case class Delete(toSeqNr: Long)

    case class Event(seqNr: Long, payload: String)

    def props(specId: String, watcher: ActorRef): Props = Props(classOf[EventLogger], specId, watcher)
  }

  class EventLogger(specId: String, watcher: ActorRef) extends PersistentActor with ActorLogging {

    import EventLogger._

    override def receiveRecover: Receive = {
      case Event(seqNr, _) => log.info("Recovered event {}", seqNr)
    }

    override def receiveCommand: Receive = {
      case Generate =>
        persist(Event(lastSequenceNr, randomText()))(onEventPersisted)
      case Delete(toSeqNr) =>
        log.info("Deleting messages up to {}", toSeqNr)
        deleteMessages(toSeqNr)
      case evt: DeleteMessagesSuccess =>
        watcher ! evt
    }

    override def persistenceId: String = specId

    private def onEventPersisted(evt: Event): Unit = {
      watcher ! Generated(evt.seqNr)
    }

    private def randomText(): String = Random.nextString(1024)
  }

}
