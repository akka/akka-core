/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import org.scalatest.wordspec.AnyWordSpec

class CompactionSegmentManagementSpec extends AnyWordSpec {

  "A CompactionSegmentManagement compatible object" must {
    "ignore persistence ids without declared compaction intervals" in {
      val intervals = Map("persistence_id-1" -> 1L, "persistence_id-2" -> 1L, "persistence_id-3" -> 1L)
      val compactionStub = new CompactionSegmentManagement {
        override def compactionIntervals: Map[String, Long] = intervals
      }
      assert(intervals.contains("persistence_id-1") && compactionStub.mustCompact("persistence_id-1", 1))
      assert(intervals.contains("persistence_id-2") && compactionStub.mustCompact("persistence_id-2", 1))
      assert(intervals.contains("persistence_id-3") && compactionStub.mustCompact("persistence_id-3", 1))
      assert(!intervals.contains("persistence_id-4") && !compactionStub.mustCompact("persistence_id-4", 1))
    }

    "ignore persistence ids whose compaction intervals are less or equal to zero" in {
      val intervals = Map("persistence_id-1" -> 1L, "persistence_id-2" -> 0L, "persistence_id-3" -> -1L)
      val compactionStub = new CompactionSegmentManagement {
        override def compactionIntervals: Map[String, Long] = intervals
      }
      assert(intervals("persistence_id-1") > 0 && compactionStub.mustCompact("persistence_id-1", 1))
      assert(intervals("persistence_id-2") <= 0 && !compactionStub.mustCompact("persistence_id-2", 1))
      assert(intervals("persistence_id-3") <= 0 && !compactionStub.mustCompact("persistence_id-3", 1))
    }

    "allow for wildcard configuration" in {
      val intervals = Map("persistence_id-1" -> 1L, "persistence_id-2" -> 1L, "*" -> 1L)
      val compactionStub = new CompactionSegmentManagement {
        override def compactionIntervals: Map[String, Long] = intervals
      }
      assert(intervals.contains("persistence_id-1") && compactionStub.mustCompact("persistence_id-1", 1))
      assert(intervals.contains("persistence_id-2") && compactionStub.mustCompact("persistence_id-2", 1))
      assert(!intervals.contains("persistence_id-3") && compactionStub.mustCompact("persistence_id-3", 1))
    }

    "not permit compaction before thresholds are exceeded" in {
      val namedIntervals = Map("persistence_id-1" -> 5L, "persistence_id-2" -> 4L)
      val intervals = namedIntervals + ("*" -> 3L)
      val compactionStub = new CompactionSegmentManagement {
        override def compactionIntervals: Map[String, Long] = intervals
      }
      val expectedIntervals = namedIntervals + ("persistence_id-3" -> 3L) + ("persistence_id-4" -> 3L)

      for ((id, interval) <- expectedIntervals) {
        var segment = 0

        for (i <- 0L.until(4 * interval)) {
          if ((i + 1) % interval == 0) {
            assert(compactionStub.mustCompact(id, i), s"must compact for [$id] when toSeqNr is [$i]")
            segment += 1
            compactionStub.updateCompactionSegment(id, segment)
          } else {
            assert(!compactionStub.mustCompact(id, i), s"must not compact for [$id] when toSeqNr is [$i]")
          }

          assert(compactionStub.compactionSegment(id, i) == segment, s"for [$id] when toSeqNr is [$i]")
        }
      }
    }

    "keep track of latest segment with respect to the limit imposed by the upper limit of deletion" in {
      val id = "persistence_id-1"
      val interval = 5L
      val compactionStub = new CompactionSegmentManagement {
        override def compactionIntervals: Map[String, Long] = Map(id -> interval)
      }
      val smallJump = interval / 2
      val midJump = interval
      val bigJump = interval * 2
      var toSeqNr = smallJump
      var segment = 0
      assert(!compactionStub.mustCompact(id, toSeqNr))
      assert(compactionStub.compactionSegment(id, toSeqNr) == segment)
      toSeqNr += midJump
      assert(compactionStub.mustCompact(id, toSeqNr))
      segment += 1
      assert(compactionStub.compactionSegment(id, toSeqNr) == segment)
      compactionStub.updateCompactionSegment(id, segment)
      toSeqNr += bigJump
      assert(compactionStub.mustCompact(id, toSeqNr))
      segment += 2
      assert(compactionStub.compactionSegment(id, toSeqNr) == segment)
    }
  }

}
