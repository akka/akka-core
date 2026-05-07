/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.annotation.nowarn

import com.typesafe.config.Config

import akka.actor.Actor
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit.ImplicitSender

object LoadPluginSpec {

  case object GetConfig

  class JournalWithConfig(val config: Config) extends InmemJournal {
    override def receivePluginInternal: Actor.Receive = {
      case GetConfig => sender() ! config
    }
  }

  object JournalWithStartupNotification {
    final case class Started(configPath: String)
  }
  class JournalWithStartupNotification(@nowarn("msg=never used") config: Config, configPath: String)
      extends InmemJournal {
    context.system.eventStream.publish(JournalWithStartupNotification.Started(configPath))
  }
}

class LoadPluginSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "LoadJournalSpec",
        extraConfig = Some("""
  akka.persistence.journal.inmem.class = "akka.persistence.LoadPluginSpec$JournalWithConfig"
  akka.persistence.journal.inmem.extra-property = 17
  
  test-plugin {
    class = "akka.persistence.LoadPluginSpec$JournalWithStartupNotification"
  }
  """)))
    with ImplicitSender {
  import LoadPluginSpec._

  "A journal" must {
    "be created with plugin config" in {
      val journalRef = Persistence(system).journalFor("akka.persistence.journal.inmem")
      journalRef ! GetConfig
      expectMsgType[Config].getInt("extra-property") should be(17)
    }

    "not be created via eventAdapter" in {
      system.eventStream.subscribe(testActor, classOf[JournalWithStartupNotification.Started])
      Persistence(system).adaptersFor("test-plugin")
      expectNoMessage()
      Persistence(system).journalFor("test-plugin")
      expectMsg(JournalWithStartupNotification.Started("test-plugin"))
    }
  }
}
