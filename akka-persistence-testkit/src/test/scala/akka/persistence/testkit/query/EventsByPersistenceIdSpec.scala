/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.ActorRef
import akka.persistence.query.{ EventEnvelope, PersistenceQuery }
import akka.persistence.query.Sequence
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }
import akka.stream.testkit.scaladsl.TestSink

object EventsByPersistenceIdSpec {
  val config = PersistenceTestKitPlugin.config.withFallback(
    ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.persistence.testkit.events.serialize = off
      """))

  case class Command(evt: String, ack: ActorRef[Done])
  case class State()

  def testBehaviour(persistenceId: String) = {
    EventSourcedBehavior[Command, String, State](
      PersistenceId.ofUniqueId(persistenceId),
      State(),
      (_, command) =>
        Effect.persist(command.evt).thenRun { _ =>
          command.ack ! Done
        },
      (state, _) => state).withTagger(evt => if (evt.startsWith("tag-me-")) Set("tag") else Set.empty)
  }

}

class EventsByPersistenceIdSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdSpec.config)
    with LogCapturing
    with AnyWordSpecLike {
  import EventsByPersistenceIdSpec._

  implicit val classic: akka.actor.ActorSystem = system.classicSystem

  val queries =
    PersistenceQuery(system).readJournalFor[PersistenceTestKitReadJournal](PersistenceTestKitReadJournal.Identifier)

  def setup(persistenceId: String): ActorRef[Command] = {
    val probe = createTestProbe[Done]()
    val ref = setupEmpty(persistenceId)
    ref ! Command(s"$persistenceId-1", probe.ref)
    ref ! Command(s"$persistenceId-2", probe.ref)
    ref ! Command(s"$persistenceId-3", probe.ref)
    probe.expectMessage(Done)
    probe.expectMessage(Done)
    probe.expectMessage(Done)
    ref
  }

  def setupEmpty(persistenceId: String): ActorRef[Command] = {
    spawn(testBehaviour(persistenceId))
  }

  "Persistent test kit live query EventsByPersistenceId" must {
    "find new events" in {
      val ackProbe = createTestProbe[Done]()
      val ref = setup("c")
      val src = queries.eventsByPersistenceId("c", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink[Any]()).request(5).expectNext("c-1", "c-2", "c-3")

      ref ! Command("c-4", ackProbe.ref)
      ackProbe.expectMessage(Done)

      probe.expectNext("c-4")
    }

    "find new events up to a sequence number" in {
      val ackProbe = createTestProbe[Done]()
      val ref = setup("d")
      val src = queries.eventsByPersistenceId("d", 0L, 4L)
      val probe = src.map(_.event).runWith(TestSink[Any]()).request(5).expectNext("d-1", "d-2", "d-3")

      ref ! Command("d-4", ackProbe.ref)
      ackProbe.expectMessage(Done)

      probe.expectNext("d-4").expectComplete()
    }

    "find new events after demand request" in {
      val ackProbe = createTestProbe[Done]()
      val ref = setup("e")
      val src = queries.eventsByPersistenceId("e", 0L, Long.MaxValue)
      val probe =
        src.map(_.event).runWith(TestSink[Any]()).request(2).expectNext("e-1", "e-2").expectNoMessage(100.millis)

      ref ! Command("e-4", ackProbe.ref)
      ackProbe.expectMessage(Done)

      probe.expectNoMessage(100.millis).request(5).expectNext("e-3").expectNext("e-4")
    }

    "include timestamp in EventEnvelope" in {
      setup("n")

      val src = queries.eventsByPersistenceId("n", 0L, Long.MaxValue)
      val probe = src.runWith(TestSink[EventEnvelope]())

      probe.request(5)
      val envelope = probe.expectNext()
      envelope.timestamp should be > 0L
      envelope.offset shouldBe a[Sequence]
      probe.expectNext().timestamp should be > 0L
      probe.cancel()
    }

    "not complete for empty persistence id" in {
      val ackProbe = createTestProbe[Done]()
      val src = queries.eventsByPersistenceId("o", 0L, Long.MaxValue)
      val probe =
        src.map(_.event).runWith(TestSink[Any]()).request(2)

      probe.expectNoMessage(200.millis) // must not complete

      val ref = setupEmpty("o")
      ref ! Command("o-1", ackProbe.ref)
      ackProbe.expectMessage(Done)

      probe.cancel()
    }
  }

  "Persistent test kit query currentEventsByPersistenceId" must {
    "include timestamp in EventEnvelope" in {
      setup("n")

      val src = queries.currentEventsByPersistenceId("n", 0L, Long.MaxValue)
      val probe = src.runWith(TestSink[EventEnvelope]())

      probe.request(5)
      val envelope = probe.expectNext()
      envelope.timestamp should be > 0L
      envelope.offset shouldBe a[Sequence]
      probe.expectNext().timestamp should be > 0L
      probe.cancel()
    }
  }
}
