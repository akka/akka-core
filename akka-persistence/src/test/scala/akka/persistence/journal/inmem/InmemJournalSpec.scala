/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.inmem

import akka.actor.Props
import akka.persistence.PersistenceSpec
import akka.persistence.PersistentActor
import akka.testkit._

object InmemJournalSpec {

  def testProps(name: String): Props =
    Props(new TestPersistentActor(name))

  final case class Cmd(s: String)
  final case class Delete(toSeqNr: Long)
  final case class Evt(s: String)

  class TestPersistentActor(name: String) extends PersistentActor {

    override def persistenceId: String = name

    override def receiveRecover: Receive = {
      case Evt(_) =>
    }
    override def receiveCommand: Receive = {
      case Cmd(s)          => persist(Evt(s))(_ => ())
      case Delete(toSeqNr) => deleteMessages(toSeqNr)
    }
  }

}

class InmemJournalSpec
    extends PersistenceSpec(PersistenceSpec.config("inmem", "InmemJournalSpec"))
    with ImplicitSender {
  import InmemJournalSpec._

  system.eventStream.subscribe(testActor, classOf[InmemJournal.Operation])

  "InmemJournal" must {
    "publish writes" in {
      val p1 = system.actorOf(testProps("p1"))
      p1 ! Cmd("A")
      p1 ! Cmd("B")
      expectMsg(InmemJournal.Write(Evt("A"), "p1", 1L))
      expectMsg(InmemJournal.Write(Evt("B"), "p1", 2L))
    }

    "publish deletes" in {
      val p1 = system.actorOf(testProps("p2"))
      p1 ! Cmd("A")
      p1 ! Cmd("B")
      p1 ! Cmd("C")
      p1 ! Delete(2)
      expectMsg(InmemJournal.Write(Evt("A"), "p2", 1L))
      expectMsg(InmemJournal.Write(Evt("B"), "p2", 2L))
      expectMsg(InmemJournal.Write(Evt("C"), "p2", 3L))
      expectMsg(InmemJournal.Delete("p2", 2L))
    }
  }

}
