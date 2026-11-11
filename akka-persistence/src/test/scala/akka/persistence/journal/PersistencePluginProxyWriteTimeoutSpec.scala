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
import akka.persistence.NonPersistentRepr
import akka.persistence.PersistentRepr
import akka.persistence.Persistence
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

object PersistencePluginProxyWriteTimeoutSpec {
  // Drops writes for persistence ids starting with "lost". For "partial" it replies only for the whole write,
  // for "partial-first" also for the first event.
  class LossyJournal(cfg: Config) extends akka.persistence.journal.inmem.InmemJournal(cfg) {
    override protected[akka] def aroundReceive(receive: Receive, msg: Any): Unit = msg match {
      case WriteMessages(messages, persistentActor, actorInstanceId, _) =>
        messages.collectFirst { case a: AtomicWrite => a } match {
          case Some(a) if a.persistenceId.startsWith("lost") => // drop
          case Some(a) if a.persistenceId.startsWith("partial") =>
            persistentActor ! WriteMessagesSuccessful
            if (a.persistenceId.startsWith("partial-first"))
              persistentActor ! WriteMessageSuccess(a.payload.head, actorInstanceId)
          case _ => super.aroundReceive(receive, msg)
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

  private def event(pid: String, seqNr: Long) = PersistentRepr("evt", seqNr, pid, writerUuid = "w")

  private def write(pid: String, probe: TestProbe): WriteMessages =
    WriteMessages(List(AtomicWrite(event(pid, 1L))), probe.ref, 1)

  // two events in one atomic write, a deferred message, and one more event
  private def mixedWrite(pid: String, probe: TestProbe): WriteMessages =
    WriteMessages(
      List(
        AtomicWrite(List(event(pid, 1L), event(pid, 2L))),
        NonPersistentRepr("deferred", probe.ref),
        AtomicWrite(event(pid, 3L))),
      probe.ref,
      1)

  private def expectSuccess(probe: TestProbe, seqNr: Long): Unit =
    probe.expectMsgType[WriteMessageSuccess](5.seconds).persistent.sequenceNr should ===(seqNr)

  private def expectFailure(probe: TestProbe, seqNr: Long): Unit =
    probe.expectMsgType[WriteMessageFailure](5.seconds).message.sequenceNr should ===(seqNr)

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

    "relay all replies of a write with several events and a deferred message" in {
      val probe = TestProbe()
      journal.tell(mixedWrite("ok-mixed", probe), probe.ref)
      probe.expectMsg(WriteMessagesSuccessful)
      expectSuccess(probe, 1L)
      expectSuccess(probe, 2L)
      probe.expectMsg(LoopMessageSuccess("deferred", 1))
      expectSuccess(probe, 3L)
      probe.expectNoMessage(1.second)
    }

    "fail all events of a write with several events and a deferred message that gets no reply" in {
      val probe = TestProbe()
      journal.tell(mixedWrite("lost-mixed", probe), probe.ref)
      probe.expectMsgType[WriteMessagesFailed](5.seconds).writeCount should ===(2)
      expectFailure(probe, 1L)
      expectFailure(probe, 2L)
      probe.expectMsg(LoopMessageSuccess("deferred", 1))
      expectFailure(probe, 3L)
      probe.expectNoMessage(1.second)
    }

    "fail only the messages after the last reply, when some replies arrived" in {
      val probe = TestProbe()
      journal.tell(mixedWrite("partial-first-mixed", probe), probe.ref)
      probe.expectMsg(WriteMessagesSuccessful)
      expectSuccess(probe, 1L)
      expectFailure(probe, 2L)
      probe.expectMsg(LoopMessageSuccess("deferred", 1))
      expectFailure(probe, 3L)
      probe.expectNoMessage(1.second)
    }
  }
}
