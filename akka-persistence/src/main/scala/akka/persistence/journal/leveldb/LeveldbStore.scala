/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import java.io.File

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.util._
import scala.util.control.NonFatal

import com.typesafe.config.{ Config, ConfigFactory, ConfigObject }
import org.iq80.leveldb._

import akka.actor._
import akka.persistence._
import akka.persistence.journal.Tagged
import akka.persistence.journal.WriteJournalBase
import akka.serialization.SerializationExtension
import scala.jdk.CollectionConverters._

private[persistence] object LeveldbStore {
  val emptyConfig = ConfigFactory.empty()

  def toCompactionIntervalMap(obj: ConfigObject): Map[String, Long] = {
    obj.unwrapped().asScala.map(entry => (entry._1, java.lang.Long.parseLong(entry._2.toString))).toMap
  }
}

/**
 * INTERNAL API.
 */
private[persistence] trait LeveldbStore
    extends Actor
    with WriteJournalBase
    with LeveldbIdMapping
    with LeveldbRecovery
    with LeveldbCompaction {

  def prepareConfig: Config

  val config: Config = prepareConfig
  val nativeLeveldb = config.getBoolean("native")

  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.getBoolean("checksum"))
  val leveldbWriteOptions = new WriteOptions().sync(config.getBoolean("fsync")).snapshot(false)
  val leveldbDir = new File(config.getString("dir"))
  var leveldb: DB = _
  override val compactionIntervals: Map[String, Long] =
    LeveldbStore.toCompactionIntervalMap(config.getObject("compaction-intervals"))

  import scala.annotation.nowarn
  @nowarn("msg=deprecated")
  private val persistenceIdSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]]
    with mutable.MultiMap[String, ActorRef]
  @nowarn("msg=deprecated")
  private val tagSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]]
    with mutable.MultiMap[String, ActorRef]
  private var allPersistenceIdsSubscribers = Set.empty[ActorRef]

  private var tagSequenceNr = Map.empty[String, Long]
  private val tagPersistenceIdPrefix = "$$$"

  def leveldbFactory =
    if (nativeLeveldb) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

  val serialization = SerializationExtension(context.system)

  import Key._

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    var persistenceIds = Set.empty[String]
    var allTags = Set.empty[String]

    val result = Future.fromTry(Try {
      withBatch(batch =>
        messages.map {
          a =>
            Try {
              a.payload.foreach { p =>
                val (p2, tags) = p.payload match {
                  case Tagged(payload, tags) =>
                    (p.withPayload(payload), tags)
                  case _ => (p, Set.empty[String])
                }
                if (tags.nonEmpty && hasTagSubscribers)
                  allTags = allTags.union(tags)

                require(
                  !p2.persistenceId.startsWith(tagPersistenceIdPrefix),
                  s"persistenceId [${p.persistenceId}] must not start with $tagPersistenceIdPrefix")
                addToMessageBatch(p2, tags, batch)
              }
              if (hasPersistenceIdSubscribers)
                persistenceIds += a.persistenceId
            }
        })
    })

    if (hasPersistenceIdSubscribers) {
      persistenceIds.foreach { pid =>
        notifyPersistenceIdChange(pid)
      }
    }
    if (hasTagSubscribers && allTags.nonEmpty)
      allTags.foreach(notifyTagChange)
    result
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    try Future.successful {
      withBatch { batch =>
        val nid = numericId(persistenceId)

        // seek to first existing message
        val fromSequenceNr = withIterator { iter =>
          val startKey = Key(nid, 1L, 0)
          iter.seek(keyToBytes(startKey))
          if (iter.hasNext) keyFromBytes(iter.peekNext().getKey).sequenceNr else Long.MaxValue
        }

        if (fromSequenceNr != Long.MaxValue) {
          val toSeqNr = math.min(toSequenceNr, readHighestSequenceNr(nid))
          var sequenceNr = fromSequenceNr
          while (sequenceNr <= toSeqNr) {
            batch.delete(keyToBytes(Key(nid, sequenceNr, 0)))
            sequenceNr += 1
          }

          self ! LeveldbCompaction.TryCompactLeveldb(persistenceId, toSeqNr)
        }
      }
    } catch {
      case NonFatal(e) => Future.failed(e)
    }

  def leveldbSnapshot(): ReadOptions = leveldbReadOptions.snapshot(leveldb.getSnapshot)

  def withIterator[R](body: DBIterator => R): R = {
    val ro = leveldbSnapshot()
    val iterator = leveldb.iterator(ro)
    try {
      body(iterator)
    } finally {
      iterator.close()
      ro.snapshot().close()
    }
  }

  def withBatch[R](body: WriteBatch => R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, leveldbWriteOptions)
      r
    } finally {
      batch.close()
    }
  }

  def persistentToBytes(p: PersistentRepr): Array[Byte] = serialization.serialize(p).get
  def persistentFromBytes(a: Array[Byte]): PersistentRepr = serialization.deserialize(a, classOf[PersistentRepr]).get

  private def addToMessageBatch(persistent: PersistentRepr, tags: Set[String], batch: WriteBatch): Unit = {
    val persistentBytes = persistentToBytes(persistent.withTimestamp(System.currentTimeMillis()))
    val nid = numericId(persistent.persistenceId)
    batch.put(keyToBytes(counterKey(nid)), counterToBytes(persistent.sequenceNr))
    batch.put(keyToBytes(Key(nid, persistent.sequenceNr, 0)), persistentBytes)

    tags.foreach { tag =>
      val tagNid = tagNumericId(tag)
      val tagSeqNr = nextTagSequenceNr(tag)
      batch.put(keyToBytes(counterKey(tagNid)), counterToBytes(tagSeqNr))
      batch.put(keyToBytes(Key(tagNid, tagSeqNr, 0)), persistentBytes)
    }
  }

  private def nextTagSequenceNr(tag: String): Long = {
    val n = tagSequenceNr.get(tag) match {
      case Some(n) => n
      case None    => readHighestSequenceNr(tagNumericId(tag))
    }
    tagSequenceNr = tagSequenceNr.updated(tag, n + 1)
    n + 1
  }

  def tagNumericId(tag: String): Int =
    numericId(tagAsPersistenceId(tag))

  def tagAsPersistenceId(tag: String): String =
    tagPersistenceIdPrefix + tag

  override def preStart(): Unit = {
    leveldb = leveldbFactory.open(
      leveldbDir,
      if (nativeLeveldb) leveldbOptions else leveldbOptions.compressionType(CompressionType.NONE))
    super.preStart()
  }

  override def postStop(): Unit = {
    leveldb.close()
    super.postStop()
  }

  protected def hasPersistenceIdSubscribers: Boolean = persistenceIdSubscribers.nonEmpty

  protected def addPersistenceIdSubscriber(subscriber: ActorRef, persistenceId: String): Unit =
    persistenceIdSubscribers.addBinding(persistenceId, subscriber)

  protected def removeSubscriber(subscriber: ActorRef): Unit = {
    val keys = persistenceIdSubscribers.collect { case (k, s) if s.contains(subscriber) => k }
    keys.foreach { key =>
      persistenceIdSubscribers.removeBinding(key, subscriber)
    }

    val tagKeys = tagSubscribers.collect { case (k, s) if s.contains(subscriber) => k }
    tagKeys.foreach { key =>
      tagSubscribers.removeBinding(key, subscriber)
    }

    allPersistenceIdsSubscribers -= subscriber
  }

  protected def hasTagSubscribers: Boolean = tagSubscribers.nonEmpty

  protected def addTagSubscriber(subscriber: ActorRef, tag: String): Unit =
    tagSubscribers.addBinding(tag, subscriber)

  protected def hasAllPersistenceIdsSubscribers: Boolean = allPersistenceIdsSubscribers.nonEmpty

  protected def addAllPersistenceIdsSubscriber(subscriber: ActorRef): Unit = {
    allPersistenceIdsSubscribers += subscriber
    subscriber ! LeveldbJournal.CurrentPersistenceIds(allPersistenceIds)
  }

  private def notifyPersistenceIdChange(persistenceId: String): Unit =
    if (persistenceIdSubscribers.contains(persistenceId)) {
      val changed = LeveldbJournal.EventAppended(persistenceId)
      persistenceIdSubscribers(persistenceId).foreach(_ ! changed)
    }

  private def notifyTagChange(tag: String): Unit =
    if (tagSubscribers.contains(tag)) {
      val changed = LeveldbJournal.TaggedEventAppended(tag)
      tagSubscribers(tag).foreach(_ ! changed)
    }

  override protected def newPersistenceIdAdded(id: String): Unit = {
    if (hasAllPersistenceIdsSubscribers && !id.startsWith(tagPersistenceIdPrefix)) {
      val added = LeveldbJournal.PersistenceIdAdded(id)
      allPersistenceIdsSubscribers.foreach(_ ! added)
    }
  }

}
