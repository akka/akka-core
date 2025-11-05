/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.testkit.query.EventsByPersistenceIdSpec.Command
import akka.persistence.testkit.query.EventsByPersistenceIdSpec.testBehaviour
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.stream.scaladsl.Sink

class CurrentEventsBySlicesSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdSpec.config)
    with LogCapturing
    with AnyWordSpecLike {

  implicit val classic: akka.actor.ActorSystem = system.classicSystem

  val queries =
    PersistenceQuery(system).readJournalFor[PersistenceTestKitReadJournal](PersistenceTestKitReadJournal.Identifier)

  def setup(persistenceId: String): ActorRef[Command] = {
    val probe = createTestProbe[Done]()
    val ref = spawn(testBehaviour(persistenceId))
    ref ! Command(s"$persistenceId-1", probe.ref)
    ref ! Command(s"$persistenceId-2", probe.ref)
    ref ! Command(s"$persistenceId-3", probe.ref)
    probe.expectMessage(Done)
    probe.expectMessage(Done)
    probe.expectMessage(Done)
    ref
  }

  "Persistent test kit currentEventsByTag query" must {

    "find eventsBySlices ordered by insert time" in {
      val probe = createTestProbe[Done]()
      val ref1 = spawn(testBehaviour("Test|pid-1"))
      val ref2 = spawn(testBehaviour("Test|pid-2"))
      ref1 ! Command("evt-1", probe.ref)
      ref1 ! Command("evt-2", probe.ref)
      ref1 ! Command("evt-3", probe.ref)
      probe.receiveMessages(3)
      ref2 ! Command("evt-4", probe.ref)
      probe.receiveMessage()
      ref1 ! Command("evt-5", probe.ref)
      probe.receiveMessage()

      queries
        .currentEventsBySlices[String]("Test", 0, Persistence(system).numberOfSlices - 1, NoOffset)
        .runWith(Sink.seq)
        .futureValue
        .map(_.event) should ===(Seq("evt-1", "evt-2", "evt-3", "evt-4", "evt-5"))
    }

    "include tags in events by slices" in {
      val probe = createTestProbe[Done]()
      val ref1 = spawn(testBehaviour("TagTest|pid-1"))
      ref1 ! Command("tag-me-evt-1", probe.ref)
      ref1 ! Command("evt-2", probe.ref)
      probe.receiveMessages(2)
      val ref2 = spawn(testBehaviour("TagTest|pid-2"))
      ref2 ! Command("evt-3", probe.ref)
      ref2 ! Command("tag-me-evt-4", probe.ref)
      probe.receiveMessages(2)

      val result = queries
        .currentEventsBySlices[String]("TagTest", 0, Persistence(system).numberOfSlices - 1, NoOffset)
        .runWith(Sink.seq)
        .futureValue

      result.head.offset shouldBe a[TimestampOffset]
      result.map(e => (e.event, e.tags)) should ===(
        Seq(("tag-me-evt-1", Set("tag")), ("evt-2", Set.empty), ("evt-3", Set.empty), ("tag-me-evt-4", Set("tag"))))
    }
  }

}
