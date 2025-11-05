/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.delivery

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.delivery.DurableProducerQueue.Confirmed
import akka.actor.typed.delivery.DurableProducerQueue.LoadState
import akka.actor.typed.delivery.DurableProducerQueue.MessageSent
import akka.actor.typed.delivery.DurableProducerQueue.NoQualifier
import akka.actor.typed.delivery.DurableProducerQueue.State
import akka.actor.typed.delivery.DurableProducerQueue.StoreMessageConfirmed
import akka.actor.typed.delivery.DurableProducerQueue.StoreMessageSent
import akka.actor.typed.delivery.DurableProducerQueue.StoreMessageSentAck
import akka.actor.typed.eventstream.EventStream
import akka.persistence.journal.inmem.InmemJournal
import akka.persistence.typed.PersistenceId

object EventSourcedProducerQueueSpec {
  def conf: Config =
    ConfigFactory.parseString(s"""
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.inmem.test-serialization = on
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/EventSourcedDurableProducerQueueSpec-${UUID
      .randomUUID()
      .toString}"
    """)
}

class EventSourcedProducerQueueSpec
    extends ScalaTestWithActorTestKit(ReliableDeliveryWithEventSourcedProducerQueueSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"pid-${pidCounter.incrementAndGet()})")

  private val journalOperations = createTestProbe[InmemJournal.Operation]()
  system.eventStream ! EventStream.Subscribe(journalOperations.ref)

  private val stateProbe = createTestProbe[State[String]]()

  "EventSourcedDurableProducerQueue" must {

    "persist MessageSent" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val queue = spawn(EventSourcedProducerQueue[String](pid))
      val timestamp = System.currentTimeMillis()

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      ackProbe.expectMessage(StoreMessageSentAck(storedSeqNr = 1))
      journalOperations.expectMessage(InmemJournal.Write(msg1, pid.id, 1))

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      ackProbe.expectMessage(StoreMessageSentAck(storedSeqNr = 2))
      journalOperations.expectMessage(InmemJournal.Write(msg2, pid.id, 2))

      queue ! LoadState(stateProbe.ref)
      val expectedState =
        State(currentSeqNr = 3, highestConfirmedSeqNr = 0, confirmedSeqNr = Map.empty, unconfirmed = Vector(msg1, msg2))
      stateProbe.expectMessage(expectedState)

      // replay
      testKit.stop(queue)
      val queue2 = spawn(EventSourcedProducerQueue[String](pid))
      queue2 ! LoadState(stateProbe.ref)
      stateProbe.expectMessage(expectedState)
    }

    "not persist MessageSent if lower seqNr than already stored" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val queue = spawn(EventSourcedProducerQueue[String](pid))
      val timestamp = System.currentTimeMillis()

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      ackProbe.expectMessage(StoreMessageSentAck(storedSeqNr = 1))
      journalOperations.expectMessage(InmemJournal.Write(msg1, pid.id, 1))

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      ackProbe.expectMessage(StoreMessageSentAck(storedSeqNr = 2))
      journalOperations.expectMessage(InmemJournal.Write(msg2, pid.id, 2))

      // duplicate msg2
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      ackProbe.expectMessage(StoreMessageSentAck(storedSeqNr = 2))
      journalOperations.expectNoMessage()

      // further back is ignored
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      ackProbe.expectNoMessage()
      journalOperations.expectNoMessage()
    }

    "persist Confirmed" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val queue = spawn(EventSourcedProducerQueue[String](pid))
      val timestamp = System.currentTimeMillis()

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg1, pid.id, 1))

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg2, pid.id, 2))

      val msg3 = MessageSent(seqNr = 3, "c", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg3, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg3, pid.id, 3))

      val timestamp2 = System.currentTimeMillis()
      queue ! StoreMessageConfirmed(seqNr = 2, NoQualifier, timestamp2)
      journalOperations.expectMessage(InmemJournal.Write(Confirmed(seqNr = 2, NoQualifier, timestamp2), pid.id, 4))

      queue ! LoadState(stateProbe.ref)
      // note that msg1 is also confirmed (removed) by the confirmation of msg2
      val expectedState =
        State(
          currentSeqNr = 4,
          highestConfirmedSeqNr = 2,
          confirmedSeqNr = Map(NoQualifier -> (2L -> timestamp2)),
          unconfirmed = Vector(msg3))
      stateProbe.expectMessage(expectedState)

      // replay
      testKit.stop(queue)
      val queue2 = spawn(EventSourcedProducerQueue[String](pid))
      queue2 ! LoadState(stateProbe.ref)
      stateProbe.expectMessage(expectedState)
    }

    "not persist Confirmed with lower seqNr than already confirmed" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val queue = spawn(EventSourcedProducerQueue[String](pid))
      val timestamp = System.currentTimeMillis()

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg1, pid.id, 1))

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, NoQualifier, timestamp)
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg2, pid.id, 2))

      val timestamp2 = System.currentTimeMillis()
      queue ! StoreMessageConfirmed(seqNr = 2, NoQualifier, timestamp2)
      journalOperations.expectMessage(InmemJournal.Write(Confirmed(seqNr = 2, NoQualifier, timestamp2), pid.id, 3))

      // lower
      queue ! StoreMessageConfirmed(seqNr = 1, NoQualifier, timestamp2)
      journalOperations.expectNoMessage()

      // duplicate
      queue ! StoreMessageConfirmed(seqNr = 2, NoQualifier, timestamp2)
      journalOperations.expectNoMessage()
    }

    "keep track of confirmations per confirmationQualifier" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val queue = spawn(EventSourcedProducerQueue[String](pid))
      val timestamp = System.currentTimeMillis()

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, confirmationQualifier = "q1", timestamp)
      queue ! StoreMessageSent(msg1, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg1, pid.id, 1))

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, confirmationQualifier = "q1", timestamp)
      queue ! StoreMessageSent(msg2, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg2, pid.id, 2))

      val msg3 = MessageSent(seqNr = 3, "c", ack = true, "q2", timestamp)
      queue ! StoreMessageSent(msg3, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg3, pid.id, 3))

      val msg4 = MessageSent(seqNr = 4, "d", ack = true, "q2", timestamp)
      queue ! StoreMessageSent(msg4, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg4, pid.id, 4))

      val msg5 = MessageSent(seqNr = 5, "e", ack = true, "q2", timestamp)
      queue ! StoreMessageSent(msg5, ackProbe.ref)
      journalOperations.expectMessage(InmemJournal.Write(msg5, pid.id, 5))

      val timestamp2 = System.currentTimeMillis()
      queue ! StoreMessageConfirmed(seqNr = 4, "q2", timestamp2)
      journalOperations.expectMessage(InmemJournal.Write(Confirmed(seqNr = 4, "q2", timestamp2), pid.id, 6))

      queue ! LoadState(stateProbe.ref)
      // note that msg3 is also confirmed (removed) by the confirmation of msg4, same qualifier
      // but msg1 and msg2 are still unconfirmed
      val expectedState =
        State(
          currentSeqNr = 6,
          highestConfirmedSeqNr = 4,
          confirmedSeqNr = Map("q2" -> (4L -> timestamp2)),
          unconfirmed = Vector(msg1, msg2, msg5))
      stateProbe.expectMessage(expectedState)

      // replay
      testKit.stop(queue)
      val queue2 = spawn(EventSourcedProducerQueue[String](pid))
      queue2 ! LoadState(stateProbe.ref)
      stateProbe.expectMessage(expectedState)
    }

    "cleanup old confirmationQualifier entries" in {
      val pid = nextPid()
      val ackProbe = createTestProbe[StoreMessageSentAck]()
      val settings = EventSourcedProducerQueue.Settings(system).withCleanupUnusedAfter(100.millis)
      val queue = spawn(EventSourcedProducerQueue[String](pid, settings))
      val now = System.currentTimeMillis()
      val timestamp0 = now - 70000

      val msg1 = MessageSent(seqNr = 1, "a", ack = true, confirmationQualifier = "q1", timestamp0)
      queue ! StoreMessageSent(msg1, ackProbe.ref)

      val msg2 = MessageSent(seqNr = 2, "b", ack = true, confirmationQualifier = "q1", timestamp0)
      queue ! StoreMessageSent(msg2, ackProbe.ref)

      val msg3 = MessageSent(seqNr = 3, "c", ack = true, "q2", timestamp0)
      queue ! StoreMessageSent(msg3, ackProbe.ref)

      val msg4 = MessageSent(seqNr = 4, "d", ack = true, "q2", timestamp0)
      queue ! StoreMessageSent(msg4, ackProbe.ref)

      val timestamp1 = now - 60000
      queue ! StoreMessageConfirmed(seqNr = 1, "q1", timestamp1)

      // cleanup tick
      Thread.sleep(1000)

      // q1, seqNr 2 is not confirmed yet, so q1 entries shouldn't be cleaned yet
      queue ! LoadState(stateProbe.ref)

      val expectedState1 =
        State(
          currentSeqNr = 5,
          highestConfirmedSeqNr = 1,
          confirmedSeqNr = Map("q1" -> (1L -> timestamp1)),
          unconfirmed = Vector(msg2, msg3, msg4))
      stateProbe.expectMessage(expectedState1)

      val timestamp2 = now - 50000
      queue ! StoreMessageConfirmed(seqNr = 2, "q1", timestamp2)

      val timestamp3 = now + 10000 // not old
      queue ! StoreMessageConfirmed(seqNr = 4, "q2", timestamp3)

      // cleanup tick
      Thread.sleep(1000)

      // all q1 confirmed and old timestamp, so q1 entries should be cleaned
      queue ! LoadState(stateProbe.ref)

      val expectedState2 =
        State[String](
          currentSeqNr = 5,
          highestConfirmedSeqNr = 4,
          confirmedSeqNr = Map("q2" -> (4L -> timestamp3)),
          unconfirmed = Vector.empty)
      stateProbe.expectMessage(expectedState2)

    }

  }

}
