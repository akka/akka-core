/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import java.nio.ByteBuffer

/**
 * LevelDB key.
 */
private[leveldb] final case class Key(persistenceId: Int, sequenceNr: Long, mappingId: Int)

private[leveldb] object Key {
  def keyToBytes(key: Key): Array[Byte] = {
    val bb = ByteBuffer.allocate(20)
    bb.putInt(key.persistenceId)
    bb.putLong(key.sequenceNr)
    bb.putInt(key.mappingId)
    bb.array
  }

  def keyFromBytes(bytes: Array[Byte]): Key = {
    val bb = ByteBuffer.wrap(bytes)
    val aid = bb.getInt
    val snr = bb.getLong
    val mid = bb.getInt
    new Key(aid, snr, mid)
  }

  def counterKey(persistenceId: Int): Key = Key(persistenceId, 0L, 0)
  def counterToBytes(ctr: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(ctr).array
  def counterFromBytes(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  def mappingKey(id: Int) = Key(1, 0L, id)
  def isMappingKey(key: Key): Boolean = key.persistenceId == 1

  def deletionKey(persistenceId: Int, sequenceNr: Long): Key = Key(persistenceId, sequenceNr, 1)
  def isDeletionKey(key: Key): Boolean = key.mappingId == 1
}
