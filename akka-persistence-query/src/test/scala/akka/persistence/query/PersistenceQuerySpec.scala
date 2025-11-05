/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorSystem
import akka.persistence.journal.{ EventSeq, ReadEventAdapter }

class PersistenceQuerySpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val eventAdaptersConfig =
    s"""
       |akka.persistence.query.journal.dummy {
       |  event-adapters {
       |    adapt = ${classOf[PrefixStringWithPAdapter].getCanonicalName}
       |  }
       |}
    """.stripMargin

  val customReadJournalPluginConfig =
    s"""
       |${DummyReadJournal.Identifier}5 {
       |  class = "${classOf[CustomDummyReadJournalProvider5].getCanonicalName}"
       |}
       |${DummyReadJournal.Identifier}6 {
       |  class = "${classOf[DummyReadJournalProvider].getCanonicalName}"
       |}
     """.stripMargin

  "ReadJournal" must {
    "be found by full config key" in {
      withActorSystem() { system =>
        val readJournalPluginConfig: Config = ConfigFactory.parseString(customReadJournalPluginConfig)
        PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier, readJournalPluginConfig)
        // other combinations of constructor parameters
        PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "2", readJournalPluginConfig)
        PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "3", readJournalPluginConfig)
        PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "4", readJournalPluginConfig)
        // config key existing within both the provided readJournalPluginConfig
        // and the actorSystem config. The journal must be created from the provided config then.
        val dummyReadJournal5 = PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "5", readJournalPluginConfig)
        dummyReadJournal5.dummyValue should equal("custom")
        // config key directly coming from the provided readJournalPluginConfig,
        // and does not exist within the actorSystem config
        PersistenceQuery
          .get(system)
          .readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "6", readJournalPluginConfig)
      }
    }

    "throw if unable to find query journal by config key" in {
      withActorSystem() { system =>
        intercept[IllegalArgumentException] {
          PersistenceQuery.get(system).readJournalFor[DummyReadJournal](DummyReadJournal.Identifier + "-unknown")
        }.getMessage should include("missing persistence plugin")
      }
    }

  }

  private val systemCounter = new AtomicInteger()

  private def withActorSystem(conf: String = "")(block: ActorSystem => Unit): Unit = {
    val config =
      DummyReadJournalProvider.config
        .withFallback(DummyJavaReadJournalProvider.config)
        .withFallback(ConfigFactory.parseString(conf))
        .withFallback(ConfigFactory.parseString(eventAdaptersConfig))
        .withFallback(ConfigFactory.load())

    val sys = ActorSystem(s"sys-${systemCounter.incrementAndGet()}", config)
    try block(sys)
    finally Await.ready(sys.terminate(), 10.seconds)
  }
}

object ExampleQueryModels {

  case class OldModel(value: String) {
    def promote = NewModel(value)
  }

  case class NewModel(value: String)

}

class PrefixStringWithPAdapter extends ReadEventAdapter {
  override def fromJournal(event: Any, manifest: String) = EventSeq.single("p-" + event)
}
