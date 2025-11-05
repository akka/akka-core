/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.persistence.typed.scaladsl.RetentionCriteria

class RetentionCriteriaSpec extends TestSuite with Matchers with AnyWordSpecLike with LogCapturing {

  "RetentionCriteria" must {

    "snapshotWhen the sequenceNr matches numberOfEvents" in {
      val criteria = RetentionCriteria.snapshotEvery(3, 2).asInstanceOf[SnapshotCountRetentionCriteriaImpl]
      criteria.snapshotWhen(1) should ===(false)
      criteria.snapshotWhen(2) should ===(false)
      criteria.snapshotWhen(3) should ===(true)
      criteria.snapshotWhen(4) should ===(false)
      criteria.snapshotWhen(6) should ===(true)
      criteria.snapshotWhen(21) should ===(true)
      criteria.snapshotWhen(31) should ===(false)
    }

    "have valid sequenceNr range based on keepNSnapshots" in {
      val criteria = RetentionCriteria.snapshotEvery(3, 2).asInstanceOf[SnapshotCountRetentionCriteriaImpl]
      val expected =
        List(1 -> 0, 3 -> 0, 4 -> 0, 6 -> 0, 7 -> 1, 9 -> 3, 10 -> 4, 12 -> 6, 13 -> 7, 15 -> 9, 18 -> 12, 20 -> 14)
      expected.foreach {
        case (seqNr, upper) =>
          withClue(s"seqNr=$seqNr:") {
            criteria.deleteUpperSequenceNr(seqNr) should ===(upper)
          }
      }
    }

    "require keepNSnapshots >= 1" in {
      RetentionCriteria.snapshotEvery(100, 1) // ok
      intercept[IllegalArgumentException] {
        RetentionCriteria.snapshotEvery(100, 0)
      }
      intercept[IllegalArgumentException] {
        RetentionCriteria.snapshotEvery(100, -1)
      }
    }

    "require numberOfEvents >= 1" in {
      RetentionCriteria.snapshotEvery(1, 2) // ok
      intercept[IllegalArgumentException] {
        RetentionCriteria.snapshotEvery(0, 0)
      }
      intercept[IllegalArgumentException] {
        RetentionCriteria.snapshotEvery(-1, -1)
      }
    }
  }
}
