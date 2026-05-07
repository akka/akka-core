/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import scala.concurrent.Future

import org.iq80.leveldb.DBIterator

import akka.persistence._
import akka.persistence.journal.AsyncRecovery
import akka.persistence.journal.leveldb.LeveldbJournal.ReplayedTaggedMessage

/**
 * INTERNAL API.
 *
 * LevelDB backed message replay and sequence number recovery.
 */
private[persistence] trait LeveldbRecovery extends AsyncRecovery { this: LeveldbStore =>
  import Key._

  private lazy val replayDispatcherId = config.getString("replay-dispatcher")
  private lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val nid = numericId(persistenceId)
    Future(readHighestSequenceNr(nid))(replayDispatcher)
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val nid = numericId(persistenceId)
    Future(replayMessages(nid, fromSequenceNr: Long, toSequenceNr, max: Long)(replayCallback))(replayDispatcher)
  }

  def replayMessages(persistenceId: Int, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit): Unit = {
    @scala.annotation.tailrec
    def go(iter: DBIterator, key: Key, ctr: Long, replayCallback: PersistentRepr => Unit): Unit = {
      if (iter.hasNext) {
        val nextEntry = iter.next()
        val nextKey = keyFromBytes(nextEntry.getKey)
        if (nextKey.sequenceNr > toSequenceNr) {
          // end iteration here
        } else if (isDeletionKey(nextKey)) {
          // this case is needed to discard old events with deletion marker
          go(iter, nextKey, ctr, replayCallback)
        } else if (key.persistenceId == nextKey.persistenceId) {
          val msg = persistentFromBytes(nextEntry.getValue)
          val del = deletion(iter, nextKey)
          if (ctr < max) {
            if (!del) replayCallback(msg)
            go(iter, nextKey, ctr + 1L, replayCallback)
          }
        }
      }
    }

    // need to have this to be able to read journal created with 2.3.x, which
    // supported deletion of individual events
    def deletion(iter: DBIterator, key: Key): Boolean = {
      if (iter.hasNext) {
        val nextEntry = iter.peekNext()
        val nextKey = keyFromBytes(nextEntry.getKey)
        if (key.persistenceId == nextKey.persistenceId && key.sequenceNr == nextKey.sequenceNr && isDeletionKey(
              nextKey)) {
          iter.next()
          true
        } else false
      } else false
    }

    withIterator { iter =>
      val startKey = Key(persistenceId, if (fromSequenceNr < 1L) 1L else fromSequenceNr, 0)
      iter.seek(keyToBytes(startKey))
      go(iter, startKey, 0L, replayCallback)
    }
  }

  def asyncReplayTaggedMessages(tag: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: ReplayedTaggedMessage => Unit): Future[Unit] = {
    val tagNid = tagNumericId(tag)
    Future(replayTaggedMessages(tag, tagNid, fromSequenceNr: Long, toSequenceNr, max: Long)(replayCallback))(
      replayDispatcher)
  }

  def replayTaggedMessages(tag: String, tagNid: Int, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: ReplayedTaggedMessage => Unit): Unit = {

    @scala.annotation.tailrec
    def go(iter: DBIterator, key: Key, ctr: Long, replayCallback: ReplayedTaggedMessage => Unit): Unit = {
      if (iter.hasNext) {
        val nextEntry = iter.next()
        val nextKey = keyFromBytes(nextEntry.getKey)
        if (nextKey.sequenceNr > toSequenceNr) {
          // end iteration here
        } else if (key.persistenceId == nextKey.persistenceId) {
          val msg = persistentFromBytes(nextEntry.getValue)
          if (ctr < max) {
            replayCallback(ReplayedTaggedMessage(msg, tag, nextKey.sequenceNr))
            go(iter, nextKey, ctr + 1L, replayCallback)
          }
        }
      }
    }

    withIterator { iter =>
      // fromSequenceNr is exclusive, i.e. start with +1
      val startKey = Key(tagNid, if (fromSequenceNr < 1L) 1L else fromSequenceNr + 1, 0)
      iter.seek(keyToBytes(startKey))
      go(iter, startKey, 0L, replayCallback)
    }
  }

  def readHighestSequenceNr(persistenceId: Int) = {
    val ro = leveldbSnapshot()
    try {
      leveldb.get(keyToBytes(counterKey(persistenceId)), ro) match {
        case null  => 0L
        case bytes => counterFromBytes(bytes)
      }
    } finally {
      ro.snapshot().close()
    }
  }
}
