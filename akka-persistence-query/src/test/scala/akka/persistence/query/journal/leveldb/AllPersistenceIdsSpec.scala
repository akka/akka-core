/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb

import scala.annotation.nowarn
import scala.concurrent.duration._

import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.scaladsl.PersistenceIdsQuery
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

object AllPersistenceIdsSpec {
  val config = """
    akka.loglevel = INFO
    akka.persistence.journal.plugin = "akka.persistence.journal.leveldb"
    akka.persistence.journal.leveldb.dir = "target/journal-AllPersistenceIdsSpec"
    akka.test.single-expect-default = 10s
    # test is using Java serialization and not priority to rewrite
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """
}

class AllPersistenceIdsSpec extends AkkaSpec(AllPersistenceIdsSpec.config) with Cleanup with ImplicitSender {

  @nowarn("msg=deprecated")
  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)

  "Leveldb query AllPersistenceIds" must {

    "implement standard AllPersistenceIdsQuery" in {
      queries.isInstanceOf[PersistenceIdsQuery] should ===(true)
    }

    "find existing persistenceIds" in {
      system.actorOf(TestActor.props("a")) ! "a1"
      expectMsg("a1-done")
      system.actorOf(TestActor.props("b")) ! "b1"
      expectMsg("b1-done")
      system.actorOf(TestActor.props("c")) ! "c1"
      expectMsg("c1-done")

      val src = queries.currentPersistenceIds()
      val probe = src.runWith(TestSink[String]())
      probe.within(10.seconds) {
        probe.request(5).expectNextUnordered("a", "b", "c").expectComplete()
      }
    }

    "find new persistenceIds" in {
      // a, b, c created by previous step
      system.actorOf(TestActor.props("d")) ! "d1"
      expectMsg("d1-done")

      val src = queries.persistenceIds()
      val probe = src.runWith(TestSink[String]())
      probe.within(10.seconds) {
        probe.request(5).expectNextUnorderedN(List("a", "b", "c", "d"))

        system.actorOf(TestActor.props("e")) ! "e1"
        probe.expectNext("e")

        val more = (1 to 100).map("f" + _)
        more.foreach { p =>
          system.actorOf(TestActor.props(p)) ! p
        }

        probe.request(100)
        probe.expectNextUnorderedN(more)
      }

    }
  }

}
