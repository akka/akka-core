/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.Persistence
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

object EventsByPersistenceIdTypedSpec {
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

class EventsByPersistenceIdTypedSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdTypedSpec.config)
    with LogCapturing
    with AnyWordSpecLike {
  import EventsByPersistenceIdTypedSpec._

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

  "Persistent test kit live query EventsByPersistenceIdTyped" must {
    "find new events" in {
      val ackProbe = createTestProbe[Done]()
      val ref = setup("d")
      val src = queries.eventsByPersistenceIdTyped[String]("d", 0L, Long.MaxValue)
      val probe = src.runWith(TestSink[EventEnvelope[String]]())
      probe.request(5)
      probe.expectNextN(3)

      ref ! Command("tag-me-d-4", ackProbe.ref)
      ackProbe.expectMessage(Done)

      val envelope = probe.expectNext()
      envelope.offset shouldBe a[TimestampOffset]
      envelope.event should ===("tag-me-d-4")
      envelope.tags should ===(Set("tag"))
      envelope.filtered should ===(false)
      envelope.source should ===("")
      envelope.slice should ===(Persistence(system).sliceForPersistenceId("d"))

      val currentResult =
        queries.currentEventsByPersistenceIdTyped[String]("d", 0L, Long.MaxValue).runWith(Sink.seq).futureValue
      currentResult should have size (4)
      currentResult.last should ===(envelope)
    }
  }
}
