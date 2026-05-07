/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb

import scala.annotation.nowarn
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.persistence.query.EventEnvelope
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

object EventsByPersistenceIdSpec {
  val config = """
    akka.loglevel = INFO
    akka.persistence.journal.plugin = "akka.persistence.journal.leveldb"
    akka.persistence.journal.leveldb.dir = "target/journal-EventsByPersistenceIdSpec"
    akka.test.single-expect-default = 10s
    akka.persistence.query.journal.leveldb.refresh-interval = 1s
    # test is using Java serialization and not priority to rewrite
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """
}

class EventsByPersistenceIdSpec extends AkkaSpec(EventsByPersistenceIdSpec.config) with Cleanup with ImplicitSender {

  @nowarn("msg=deprecated")
  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)

  def setup(persistenceId: String): ActorRef = {
    val ref = setupEmpty(persistenceId)
    ref ! s"$persistenceId-1"
    ref ! s"$persistenceId-2"
    ref ! s"$persistenceId-3"
    expectMsg(s"$persistenceId-1-done")
    expectMsg(s"$persistenceId-2-done")
    expectMsg(s"$persistenceId-3-done")
    ref
  }

  def setupEmpty(persistenceId: String): ActorRef = {
    system.actorOf(TestActor.props(persistenceId))
  }

  "Leveldb query EventsByPersistenceId" must {

    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "find existing events" in {
      setup("a")

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink[Any]())
        .request(2)
        .expectNext("a-1", "a-2")
        .expectNoMessage(500.millis)
        .request(2)
        .expectNext("a-3")
        .expectComplete()
    }

    "find existing events up to a sequence number" in {
      setup("b")
      val src = queries.currentEventsByPersistenceId("b", 0L, 2L)
      src.map(_.event).runWith(TestSink[Any]()).request(5).expectNext("b-1", "b-2").expectComplete()
    }

    "not see new events after demand request" in {
      val ref = setup("f")
      val src = queries.currentEventsByPersistenceId("f", 0L, Long.MaxValue)
      val probe =
        src.map(_.event).runWith(TestSink[Any]()).request(2).expectNext("f-1", "f-2").expectNoMessage(100.millis)

      ref ! "f-4"
      expectMsg("f-4-done")

      probe.expectNoMessage(100.millis).request(5).expectNext("f-3").expectComplete() // f-4 not seen
    }

    "return empty stream for cleaned journal from 0 to MaxLong" in {
      val ref = setup("g1")

      ref ! TestActor.DeleteCmd(3L)
      expectMsg(s"${3L}-deleted")

      val src = queries.currentEventsByPersistenceId("g1", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "return empty stream for cleaned journal from 0 to 0" in {
      val ref = setup("g2")

      ref ! TestActor.DeleteCmd(3L)
      expectMsg(s"${3L}-deleted")

      val src = queries.currentEventsByPersistenceId("g2", 0L, 0L)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "return remaining values after partial journal cleanup" in {
      val ref = setup("h")

      ref ! TestActor.DeleteCmd(2L)
      expectMsg(s"${2L}-deleted")

      val src = queries.currentEventsByPersistenceId("h", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectNext("h-3").expectComplete()
    }

    "return empty stream for empty journal" in {
      setupEmpty("i")

      val src = queries.currentEventsByPersistenceId("i", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "return empty stream for journal from 0 to 0" in {
      setup("k1")

      val src = queries.currentEventsByPersistenceId("k1", 0L, 0L)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "return empty stream for empty journal from 0 to 0" in {
      setupEmpty("k2")

      val src = queries.currentEventsByPersistenceId("k2", 0L, 0L)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "return empty stream for journal from seqNo greater than highestSeqNo" in {
      setup("l")

      val src = queries.currentEventsByPersistenceId("l", 4L, 3L)
      src.map(_.event).runWith(TestSink[Any]()).request(1).expectComplete()
    }

    "include timestamp in EventEnvelope" in {
      setup("m")

      val src = queries.currentEventsByPersistenceId("m", 0L, Long.MaxValue)
      val probe = src.runWith(TestSink[EventEnvelope]())

      probe.request(5)
      probe.expectNext().timestamp should be > 0L
      probe.expectNext().timestamp should be > 0L
      probe.expectNext().timestamp should be > 0L
      probe.expectComplete()
    }

  }

  "Leveldb live query EventsByPersistenceId" must {
    "find new events" in {
      val ref = setup("c")
      val src = queries.eventsByPersistenceId("c", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink[Any]()).request(5).expectNext("c-1", "c-2", "c-3")

      ref ! "c-4"
      expectMsg("c-4-done")

      probe.expectNext("c-4")
    }

    "find new events up to a sequence number" in {
      val ref = setup("d")
      val src = queries.eventsByPersistenceId("d", 0L, 4L)
      val probe = src.map(_.event).runWith(TestSink[Any]()).request(5).expectNext("d-1", "d-2", "d-3")

      ref ! "d-4"
      expectMsg("d-4-done")

      probe.expectNext("d-4").expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("e")
      val src = queries.eventsByPersistenceId("e", 0L, Long.MaxValue)
      val probe =
        src.map(_.event).runWith(TestSink[Any]()).request(2).expectNext("e-1", "e-2").expectNoMessage(100.millis)

      ref ! "e-4"
      expectMsg("e-4-done")

      probe.expectNoMessage(100.millis).request(5).expectNext("e-3").expectNext("e-4")
    }

    "include timestamp in EventEnvelope" in {
      setup("n")

      val src = queries.eventsByPersistenceId("n", 0L, Long.MaxValue)
      val probe = src.runWith(TestSink[EventEnvelope]())

      probe.request(5)
      probe.expectNext().timestamp should be > 0L
      probe.expectNext().timestamp should be > 0L
      probe.cancel()
    }

    "not complete for empty persistence id" in {
      val src = queries.eventsByPersistenceId("o", 0L, Long.MaxValue)
      val probe =
        src.map(_.event).runWith(TestSink[Any]()).request(2)

      probe.expectNoMessage(200.millis) // must not complete

      val ref = setupEmpty("o")
      ref ! "o-1"
      expectMsg(s"o-1-done")

      probe.cancel()
    }
  }
}
