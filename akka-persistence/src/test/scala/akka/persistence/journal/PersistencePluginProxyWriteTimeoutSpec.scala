/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.persistence.AtomicWrite
import akka.persistence.JournalProtocol._
import akka.persistence.PersistentRepr
import akka.persistence.Persistence
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

object PersistencePluginProxyWriteTimeoutSpec {
  // drops writes for persistence ids starting with "lost", and replies only for the whole write for "partial"
  class LossyJournal(cfg: Config) extends akka.persistence.journal.inmem.InmemJournal(cfg) {
    override protected[akka] def aroundReceive(receive: Receive, msg: Any): Unit = msg match {
      case WriteMessages(messages, persistentActor, _, _) =>
        messages.collectFirst { case a: AtomicWrite => a.persistenceId } match {
          case Some(pid) if pid.startsWith("lost")    => // drop
          case Some(pid) if pid.startsWith("partial") => persistentActor ! WriteMessagesSuccessful
          case _                                      => super.aroundReceive(receive, msg)
        }
      case _ => super.aroundReceive(receive, msg)
    }
  }

  val config = ConfigFactory.parseString(
    """
    akka.persistence.journal.plugin = "akka.persistence.journal.proxy"
    akka.persistence.journal.proxy.start-target-journal = on
    akka.persistence.journal.proxy.target-journal-plugin = "lossy-journal"
    akka.persistence.journal.proxy.write-timeout = 500ms
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/snapshots-PersistencePluginProxyWriteTimeoutSpec"
    lossy-journal {
      class = "akka.persistence.journal.PersistencePluginProxyWriteTimeoutSpec$LossyJournal"
      plugin-dispatcher = "akka.actor.default-dispatcher"
    }
    """)
}

class PersistencePluginProxyWriteTimeoutSpec
    extends AkkaSpec(PersistencePluginProxyWriteTimeoutSpec.config)
    with ImplicitSender {

  private val journal: ActorRef = Persistence(system).journalFor(null)

  private def write(pid: String, probe: TestProbe): WriteMessages = {
    val repr = PersistentRepr("evt", 1L, pid, writerUuid = "w")
    WriteMessages(List(AtomicWrite(repr)), probe.ref, 1)
  }

  "PersistencePluginProxy with write-timeout" must {
    "relay all replies of a successful write" in {
      val probe = TestProbe()
      journal.tell(write("ok", probe), probe.ref)
      probe.expectMsg(WriteMessagesSuccessful)
      probe.expectMsgType[WriteMessageSuccess]
      probe.expectNoMessage(1.second)
    }

    "fail a write that gets no reply" in {
      val probe = TestProbe()
      journal.tell(write("lost-1", probe), probe.ref)
      probe.expectMsgType[WriteMessagesFailed](5.seconds)
      probe.expectMsgType[WriteMessageFailure]
    }

    "fail the messages that get no reply after the reply for the whole write" in {
      val probe = TestProbe()
      journal.tell(write("partial-1", probe), probe.ref)
      probe.expectMsg(WriteMessagesSuccessful)
      probe.expectMsgType[WriteMessageFailure](5.seconds)
    }
  }
}
