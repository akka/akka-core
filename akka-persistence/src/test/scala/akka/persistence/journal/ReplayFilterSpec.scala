/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import akka.persistence.JournalProtocol
import akka.persistence.PersistentRepr
import akka.testkit._

class ReplayFilterSpec extends AkkaSpec with ImplicitSender {
  import JournalProtocol._
  import ReplayFilter.{ Fail, RepairByDiscardOld, Warn }

  val writerA = "writer-A"
  val writerB = "writer-B"
  val writerC = "writer-C"

  val n1 = ReplayedMessage(PersistentRepr("a", 13, "p1", "", writerUuid = PersistentRepr.Undefined))
  val n2 = ReplayedMessage(PersistentRepr("b", 14, "p1", "", writerUuid = PersistentRepr.Undefined))

  val m1 = ReplayedMessage(PersistentRepr("a", 13, "p1", "", writerUuid = writerA))
  val m2 = ReplayedMessage(PersistentRepr("b", 14, "p1", "", writerUuid = writerA))
  val m3 = ReplayedMessage(PersistentRepr("c", 15, "p1", "", writerUuid = writerA))
  val m4 = ReplayedMessage(PersistentRepr("d", 16, "p1", "", writerUuid = writerA))
  val successMsg = RecoverySuccess(15)

  "ReplayFilter in RepairByDiscardOld mode" must {
    "pass on all replayed messages and then stop" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 2, maxOldWriters = 10, debugEnabled = false))
      filter ! m1
      filter ! m2
      filter ! m3
      filter ! successMsg

      expectMsg(m1)
      expectMsg(m2)
      expectMsg(m3)
      expectMsg(successMsg)

      watch(filter)
      expectTerminated(filter)
    }

    "pass on all replayed messages (when previously no writer id was given, but now is) and then stop" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 2, maxOldWriters = 10, debugEnabled = true))
      filter ! n1
      filter ! n2
      filter ! m3
      filter ! successMsg

      expectMsg(n1)
      expectMsg(n2)
      expectMsg(m3)
      expectMsg(successMsg)

      watch(filter)
      expectTerminated(filter)
    }

    "pass on all replayed messages when switching writer" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      filter ! m1
      filter ! m2
      val m32 = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
      filter ! m32
      filter ! successMsg

      expectMsg(m1)
      expectMsg(m2)
      expectMsg(m32)
      expectMsg(successMsg)
    }

    "discard message with same seqNo from old overlapping writer" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 1).intercept {
        filter ! m1
        filter ! m2
        filter ! m3
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b // same seqNo as m3, but from writerB
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2)
        expectMsg(m3b) // discard m3, because same seqNo from new writer
        expectMsg(successMsg)
      }
    }

    "discard messages from old writer after switching writer" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 2).intercept {
        filter ! m1
        filter ! m2
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b
        filter ! m3
        filter ! m4
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2)
        expectMsg(m3b)
        // discard m3, m4
        expectMsg(successMsg)
      }
    }

    "discard messages from several old writers" in {
      val filter = system.actorOf(
        ReplayFilter
          .props(testActor, mode = RepairByDiscardOld, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 3).intercept {
        filter ! m1
        val m2b = m2.copy(persistent = m2.persistent.update(writerUuid = writerB))
        filter ! m2b
        val m3c = m3.copy(persistent = m3.persistent.update(writerUuid = writerC))
        filter ! m3c
        filter ! m2
        filter ! m3
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b
        val m4c = m4.copy(persistent = m4.persistent.update(writerUuid = writerC))
        filter ! m4c
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2b)
        expectMsg(m3c)
        // discard m2, m3, m3b
        expectMsg(m4c)
        expectMsg(successMsg)
      }
    }
  }

  "ReplayFilter in Fail mode" must {
    "fail when message with same seqNo from old overlapping writer" in {
      val filter = system.actorOf(
        ReplayFilter.props(testActor, mode = Fail, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.error(start = "Invalid replayed event", occurrences = 1).intercept {
        filter ! m1
        filter ! m2
        filter ! m3
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b // same seqNo as m3, but from writerB
        filter ! successMsg

        expectMsgType[ReplayMessagesFailure].cause.getClass should be(classOf[IllegalStateException])
      }
    }

    "fail when messages from old writer after switching writer" in {
      val filter = system.actorOf(
        ReplayFilter.props(testActor, mode = Fail, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.error(start = "Invalid replayed event", occurrences = 1).intercept {
        filter ! m1
        filter ! m2
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b
        filter ! m3
        filter ! m4
        filter ! successMsg

        expectMsgType[ReplayMessagesFailure].cause.getClass should be(classOf[IllegalStateException])
      }
    }
  }

  "ReplayFilter in Warn mode" must {
    "warn about message with same seqNo from old overlapping writer" in {
      val filter = system.actorOf(
        ReplayFilter.props(testActor, mode = Warn, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 1).intercept {
        filter ! m1
        filter ! m2
        filter ! m3
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b // same seqNo as m3, but from writerB
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2)
        expectMsg(m3)
        expectMsg(m3b)
        expectMsg(successMsg)
      }
    }

    "warn about messages from old writer after switching writer" in {
      val filter = system.actorOf(
        ReplayFilter.props(testActor, mode = Warn, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 2).intercept {
        filter ! m1
        filter ! m2
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b
        filter ! m3
        filter ! m4
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2)
        expectMsg(m3b)
        expectMsg(m3)
        expectMsg(m4)
        expectMsg(successMsg)
      }
    }

    "warn about messages from several old writers" in {
      val filter = system.actorOf(
        ReplayFilter.props(testActor, mode = Warn, windowSize = 100, maxOldWriters = 10, debugEnabled = false))
      EventFilter.warning(start = "Invalid replayed event", occurrences = 3).intercept {
        filter ! m1
        val m2b = m2.copy(persistent = m2.persistent.update(writerUuid = writerB))
        filter ! m2b
        val m3c = m3.copy(persistent = m3.persistent.update(writerUuid = writerC))
        filter ! m3c
        filter ! m2
        filter ! m3
        val m3b = m3.copy(persistent = m3.persistent.update(writerUuid = writerB))
        filter ! m3b
        val m4c = m4.copy(persistent = m4.persistent.update(writerUuid = writerC))
        filter ! m4c
        filter ! successMsg

        expectMsg(m1)
        expectMsg(m2b)
        expectMsg(m3c)
        expectMsg(m2)
        expectMsg(m3)
        expectMsg(m3b)
        expectMsg(m4c)
        expectMsg(successMsg)
      }
    }
  }
}
