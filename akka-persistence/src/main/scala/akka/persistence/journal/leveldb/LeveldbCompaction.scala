/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import akka.actor.{ Actor, ActorLogging }

private[persistence] object LeveldbCompaction {

  case class TryCompactLeveldb(persistenceId: String, toSeqNr: Long)
}

/**
 * INTERNAL API.
 *
 * Exposure of LevelDB compaction capability to reduce journal size upon message deletions.
 */
private[persistence] trait LeveldbCompaction extends Actor with ActorLogging with CompactionSegmentManagement {
  this: LeveldbStore =>

  import Key._
  import LeveldbCompaction._

  def receiveCompactionInternal: Receive = {
    case TryCompactLeveldb(persistenceId, toSeqNr) =>
      tryCompactOnDelete(persistenceId, toSeqNr)
  }

  private def tryCompactOnDelete(persistenceId: String, toSeqNr: Long): Unit = {
    if (mustCompact(persistenceId, toSeqNr)) {
      val limit = compactionLimit(persistenceId, toSeqNr)
      log.info("Starting compaction for persistence id [{}] up to sequence number [{}]", persistenceId, limit)
      val start = keyToBytes(Key(numericId(persistenceId), 0, 0))
      val end = keyToBytes(Key(numericId(persistenceId), limit, 0))
      leveldb.compactRange(start, end)
      updateCompactionSegment(persistenceId, compactionSegment(persistenceId, limit))
      log.info("Compaction for persistence id [{}] up to sequence number [{}] is complete", persistenceId, limit)
    } else {
      log.debug("No compaction required yet for persistence id [{}] up to sequence number [{}]", persistenceId, toSeqNr)
    }
  }
}

/**
 * INTERNAL API.
 *
 * Calculates and stores info of compacted "segments" per persistence id.
 *
 * Assuming a compaction interval N for a given persistence id, then compaction is to be performed
 * for segments of message sequence numbers according to the following pattern:
 *
 * [0, N), [N, 2*N), ... , [m*N, (m + 1)*N)
 *
 * Once deletion is performed up to a sequence number 'toSeqNr', then 'toSeqNr' will be used to determine the
 * rightmost segment up to which compaction will be performed. Eligible segments for compaction are only
 * considered to be those which include sequence numbers up to 'toSeqNr' AND whose size is equal to N (the compaction
 * interval). This rule implies that if 'toSeqNr' spans an incomplete portion of a rightmost segment, then
 * that segment will be omitted from the pending compaction, and will be included into the next one.
 *
 */
private[persistence] trait CompactionSegmentManagement {

  import CompactionSegmentManagement._

  private[this] var latestCompactionSegments = Map.empty[String, Long]

  def compactionIntervals: Map[String, Long]

  def updateCompactionSegment(persistenceId: String, compactionSegment: Long): Unit = {
    latestCompactionSegments += persistenceId -> compactionSegment
  }

  def compactionLimit(persistenceId: String, toSeqNr: Long): Long = {
    val interval = compactionInterval(persistenceId)
    (toSeqNr + 1) / interval * interval - 1
  }

  def compactionSegment(persistenceId: String, toSeqNr: Long): Long = (toSeqNr + 1) / compactionInterval(persistenceId)

  def mustCompact(persistenceId: String, toSeqNr: Long): Boolean =
    isCompactionEnabled(persistenceId) && isCompactionRequired(persistenceId, toSeqNr)

  private def isCompactionEnabled(persistenceId: String): Boolean = compactionInterval(persistenceId) > 0L

  private def isCompactionRequired(persistenceId: String, toSeqNr: Long): Boolean =
    compactionSegment(persistenceId, toSeqNr) > latestCompactionSegment(persistenceId)

  private def latestCompactionSegment(persistenceId: String): Long =
    latestCompactionSegments.getOrElse(persistenceId, 0L)

  private def compactionInterval(persistenceId: String): Long =
    compactionIntervals.getOrElse(persistenceId, compactionIntervals.getOrElse(Wildcard, 0L))
}

private[persistence] object CompactionSegmentManagement {
  val Wildcard = "*"
}
