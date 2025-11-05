/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import akka.persistence.AtomicWrite
import akka.persistence.JournalProtocol
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object EventWriterSpec {
  def config =
    ConfigFactory.parseString(s"""
      akka.persistence.journal.inmem.delay-writes=10ms
    """).withFallback(ConfigFactory.load()).resolve()
}

class EventWriterSpec extends ScalaTestWithActorTestKit(EventWriterSpec.config) with AnyWordSpecLike with LogCapturing {

  private val settings = EventWriter.EventWriterSettings(10, 5.seconds)
  implicit val ec: ExecutionContext = testKit.system.executionContext

  "The event writer" should {

    "handle duplicates" in new TestSetup {
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      // should also be ack:ed
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "handle batched duplicates" in new TestSetup {
      // first write
      sendWrite(1)
      // first batch
      for (n <- 2L to 10L) {
        sendWrite(n)
      }
      // 0 will be written directly
      journalAckWrite() should ===(1)
      clientExpectSuccess(1)

      // completing 1 triggers write of batch with 0-9
      // second
      for (n <- 1L to 10L) {
        sendWrite(n)
      }
      // batch 0-9 in flight, writes in the meanwhile go in a new batch
      journalAckWrite() should ===(9)
      journalFailWrite("duplicate") should ===(10)
      journalHighestSeqNr(10L)

      clientExpectSuccess(19)
    }

    "handle batches with half duplicates" in new TestSetup {
      for (n <- 1L to 10L) {
        sendWrite(n)
      }
      journalAckWrite() should ===(1)
      journalAckWrite() should ===(9)
      clientExpectSuccess(10)

      for (n <- 5L until 15L) {
        sendWrite(n)
      }
      journalFailWrite("duplicate") should ===(1) // seq nr 5
      journalHighestSeqNr(10L)
      journalFailWrite("duplicate") should ===(9) // batch of 6-15
      journalHighestSeqNr(10L)
      journalAckWrite() should ===(4) // new write of 11-15 (non duplicates)

      // all writes succeeded
      clientExpectSuccess(10)
    }

    "pass real errors from journal back" in new TestSetup {
      sendWrite(1L)
      journalFailWrite("error error")
      // duplicate handling will ask for highest seq nr, can't know it is an actual error
      journalHighestSeqNr(0L)
      val response = clientProbe.receiveMessage()
      response.isError should ===(true)
      response.getError.getMessage should ===("Journal write failed")
    }

    "ignores old failures when replay triggered" in new TestSetup {
      sendWrite(1L) // triggers write

      sendWrite(1L) // goes into batch
      sendWrite(2L)
      journalAckWrite()

      val firstWrite = fakeJournal.expectMessageType[JournalProtocol.WriteMessages]
      val payloads = firstWrite.messages.head.asInstanceOf[AtomicWrite].payload
      // signal first failure, triggers HighestSeq
      firstWrite.persistentActor ! JournalProtocol.WriteMessageFailure(
        payloads.head,
        new RuntimeException("duplicate"),
        firstWrite.actorInstanceId)

      // replay response triggers partial rewrite, seq nr 2
      val replayRequest = fakeJournal.expectMessageType[JournalProtocol.ReplayMessages]
      replayRequest.persistentActor ! JournalProtocol.RecoverySuccess(1L)

      // but then original write failure arrives for seq nr 2 after sequence number lookup succeeded
      firstWrite.persistentActor ! JournalProtocol.WriteMessageFailure(
        payloads.tail.head,
        new RuntimeException("duplicate"),
        firstWrite.actorInstanceId)
      firstWrite.persistentActor ! JournalProtocol.WriteMessagesFailed

      // ack the partial retry
      journalAckWrite() should ===(1)

      clientExpectSuccess(3)

    }

    "handle writes to many pids" in {
      val writer = spawn(EventWriter("akka.persistence.journal.inmem", settings))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      (0 to 1000).map { pidN =>
        Future {
          for (n <- 0 until 20) {
            writer ! EventWriter.Write(s"pid$pidN", n.toLong, n.toString, None, Set.empty, probe.ref)
          }
        }
      }
      probe.receiveMessages(20 * 1000, 20.seconds)
    }
  }

  trait TestSetup {
    def pid1 = "pid1"
    val fakeJournal = createTestProbe[JournalProtocol.Message]()
    val writer = spawn(EventWriter(fakeJournal.ref, settings))
    val clientProbe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
    def sendWrite(seqNr: Long, pid: String = pid1): Unit = {
      writer ! EventWriter.Write(pid, seqNr, seqNr.toString, None, Set.empty, clientProbe.ref)
    }
    def journalAckWrite(pid: String = pid1): Int = {
      val write = fakeJournal.expectMessageType[JournalProtocol.WriteMessages]
      write.messages should have size (1)
      val atomicWrite = write.messages.head.asInstanceOf[AtomicWrite]
      atomicWrite.payload.foreach { repr =>
        repr.persistenceId should ===(pid)
        write.persistentActor ! JournalProtocol.WriteMessageSuccess(repr, write.actorInstanceId)
      }
      write.persistentActor ! JournalProtocol.WriteMessagesSuccessful
      atomicWrite.payload.size
    }

    def journalFailWrite(reason: String, pid: String = pid1): Int = {
      val write = fakeJournal.expectMessageType[JournalProtocol.WriteMessages]
      write.messages should have size (1)
      val atomicWrite = write.messages.head.asInstanceOf[AtomicWrite]
      atomicWrite.payload.foreach { repr =>
        repr.persistenceId should ===(pid)
        write.persistentActor ! JournalProtocol.WriteMessageFailure(
          repr,
          new RuntimeException(reason),
          write.actorInstanceId)
      }
      write.persistentActor ! JournalProtocol.WriteMessagesFailed
      atomicWrite.payload.size
    }

    def journalHighestSeqNr(highestSeqNr: Long): Unit = {
      val replay = fakeJournal.expectMessageType[JournalProtocol.ReplayMessages]
      replay.persistentActor ! JournalProtocol.RecoverySuccess(highestSeqNr)
    }

    def clientExpectSuccess(n: Int) = {
      clientProbe.receiveMessages(n).foreach { reply =>
        reply.isSuccess should be(true)
      }
    }
  }

}
