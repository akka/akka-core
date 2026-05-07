/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import org.iq80.leveldb.DBIterator

import akka.actor.Actor
import akka.util.ByteString.UTF_8

/**
 * INTERNAL API.
 *
 * LevelDB backed persistent mapping of `String`-based persistent actor ids to numeric ids.
 */
private[persistence] trait LeveldbIdMapping extends Actor { this: LeveldbStore =>
  import Key._

  private val idOffset = 10
  private var idMap: Map[String, Int] = Map.empty
  private val idMapLock = new Object

  /**
   * Get the mapped numeric id for the specified persistent actor `id`. Creates and
   * stores a new mapping if necessary.
   *
   * This method is thread safe and it is allowed to call it from a another
   * thread than the actor's thread. That is necessary for Future composition,
   * e.g. `asyncReadHighestSequenceNr` followed by `asyncReplayMessages`.
   */
  def numericId(id: String): Int = idMapLock.synchronized {
    idMap.get(id) match {
      case None    => writeIdMapping(id, idMap.size + idOffset)
      case Some(v) => v
    }
  }

  def isNewPersistenceId(id: String): Boolean = idMapLock.synchronized {
    !idMap.contains(id)
  }

  def allPersistenceIds: Set[String] = idMapLock.synchronized {
    idMap.keySet
  }

  private def readIdMap(): Map[String, Int] = withIterator { iter =>
    iter.seek(keyToBytes(mappingKey(idOffset)))
    readIdMap(Map.empty, iter)
  }

  private def readIdMap(pathMap: Map[String, Int], iter: DBIterator): Map[String, Int] = {
    if (!iter.hasNext) pathMap
    else {
      val nextEntry = iter.next()
      val nextKey = keyFromBytes(nextEntry.getKey)
      if (!isMappingKey(nextKey)) pathMap
      else {
        val nextVal = new String(nextEntry.getValue, UTF_8)
        readIdMap(pathMap + (nextVal -> nextKey.mappingId), iter)
      }
    }
  }

  private def writeIdMapping(id: String, numericId: Int): Int = {
    idMap = idMap + (id -> numericId)
    leveldb.put(keyToBytes(mappingKey(numericId)), id.getBytes(UTF_8))
    this.newPersistenceIdAdded(id)
    numericId
  }

  override protected def newPersistenceIdAdded(id: String): Unit = ()

  override def preStart(): Unit = {
    idMap = readIdMap()
    super.preStart()
  }
}
