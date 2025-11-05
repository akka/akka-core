/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.persistence.JournalProtocol._
import akka.testkit._

object PersistentActorJournalProtocolSpec {

  val config = ConfigFactory.parseString("""
puppet {
  class = "akka.persistence.JournalPuppet"
  max-message-batch-size = 10
}
akka.persistence.journal.plugin = puppet
akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
""")

  sealed trait Command
  case class Persist(id: Int, msgs: Any*) extends Command
  case class PersistAsync(id: Int, msgs: Any*) extends Command
  case class Multi(cmd: Command*) extends Command
  case class Echo(id: Int) extends Command
  case class Fail(ex: Throwable) extends Command
  case class Done(id: Int, sub: Int)

  case class PreStart(name: String)
  case class PreRestart(name: String)
  case class PostRestart(name: String)
  case class PostStop(name: String)

  class A(monitor: ActorRef) extends PersistentActor {

    def persistenceId = self.path.name

    override def preStart(): Unit = monitor ! PreStart(persistenceId)
    override def preRestart(reason: Throwable, msg: Option[Any]): Unit = monitor ! PreRestart(persistenceId)
    override def postRestart(reason: Throwable): Unit = monitor ! PostRestart(persistenceId)
    override def postStop(): Unit = monitor ! PostStop(persistenceId)

    def receiveRecover = {
      case x => monitor ! x
    }
    def receiveCommand = behavior.orElse {
      case m: Multi => m.cmd.foreach(behavior)
    }

    val behavior: Receive = {
      case p: Persist      => P(p)
      case p: PersistAsync => PA(p)
      case Echo(id)        => sender() ! Done(id, 0)
      case Fail(ex)        => throw ex
    }
    val doNothing = (_: Any) => ()

    def P(p: Persist): Unit = {
      var sub = 0
      persistAll(p.msgs.toList) { e =>
        sender() ! Done(p.id, { sub += 1; sub })
        behavior.applyOrElse(e, doNothing)
      }
    }
    def PA(p: PersistAsync): Unit = {
      var sub = 0
      persistAllAsync(p.msgs.toList) { e =>
        sender() ! Done(p.id, { sub += 1; sub })
        behavior.applyOrElse(e, doNothing)
      }
    }
  }
}

object JournalPuppet extends ExtensionId[JournalProbe] with ExtensionIdProvider {
  override def lookup = JournalPuppet

  override def createExtension(system: ExtendedActorSystem): JournalProbe =
    new JournalProbe()(system)

  override def get(system: ActorSystem): JournalProbe = super.get(system)
  override def get(system: ClassicActorSystemProvider): JournalProbe = super.get(system)
}
class JournalProbe(implicit private val system: ExtendedActorSystem) extends Extension {
  val probe = TestProbe()
  val ref = probe.ref
}

class JournalPuppet extends Actor {
  val ref = JournalPuppet(context.system).ref
  def receive = {
    case x => ref.forward(x)
  }
}

import PersistentActorJournalProtocolSpec._

class PersistentActorJournalProtocolSpec extends AkkaSpec(config) with ImplicitSender {

  val journal = JournalPuppet(system).probe

  case class Msgs(msg: Any*)

  def expectWrite(subject: ActorRef, msgs: Msgs*): WriteMessages = {
    val w = journal.expectMsgType[WriteMessages]
    withClue(s"$w: ") {
      w.persistentActor should ===(subject)
      w.messages.size should ===(msgs.size)
      w.messages.zip(msgs).foreach {
        case (AtomicWrite(writes), msg) =>
          writes.size should ===(msg.msg.size)
          writes.zip(msg.msg).foreach {
            case (PersistentRepr(evt, _), m) =>
              evt should ===(m)
            case _ =>
          }
        case x => fail(s"unexpected $x")
      }
    }
    w
  }

  def confirm(w: WriteMessages): Unit = {
    journal.send(w.persistentActor, WriteMessagesSuccessful)
    w.messages.foreach {
      case AtomicWrite(msgs) =>
        msgs.foreach(msg => w.persistentActor.tell(WriteMessageSuccess(msg, w.actorInstanceId), msg.sender))
      case NonPersistentRepr(msg, sender) => w.persistentActor.tell(msg, sender)
    }
  }

  def startActor(name: String): ActorRef = {
    val subject = system.actorOf(Props(new A(testActor)), name)
    subject ! Echo(0)
    expectMsg(PreStart(name))
    journal.expectMsgType[ReplayMessages]
    journal.reply(RecoverySuccess(0L))
    expectMsg(RecoveryCompleted)
    expectMsg(Done(0, 0))
    subject
  }

  "A PersistentActor’s journal protocol" must {

    "not send WriteMessages while a write is still outstanding" when {

      "using simple persist()" in {
        val subject = startActor("test-1")
        subject ! Persist(1, "a-1")
        val w1 = expectWrite(subject, Msgs("a-1"))
        subject ! Persist(2, "a-2")
        expectNoMessage(300.millis)
        journal.msgAvailable should ===(false)
        confirm(w1)
        expectMsg(Done(1, 1))
        val w2 = expectWrite(subject, Msgs("a-2"))
        confirm(w2)
        expectMsg(Done(2, 1))
        subject ! PoisonPill
        expectMsg(PostStop("test-1"))
        journal.msgAvailable should ===(false)
      }

      "using nested persist()" in {
        val subject = startActor("test-2")
        subject ! Persist(1, Persist(2, "a-1"))
        val w1 = expectWrite(subject, Msgs(Persist(2, "a-1")))
        subject ! Persist(3, "a-2")
        expectNoMessage(300.millis)
        journal.msgAvailable should ===(false)
        confirm(w1)
        expectMsg(Done(1, 1))
        val w2 = expectWrite(subject, Msgs("a-1"))
        confirm(w2)
        expectMsg(Done(2, 1))
        val w3 = expectWrite(subject, Msgs("a-2"))
        confirm(w3)
        expectMsg(Done(3, 1))
        subject ! PoisonPill
        expectMsg(PostStop("test-2"))
        journal.msgAvailable should ===(false)
      }

      "using nested multiple persist()" in {
        val subject = startActor("test-3")
        subject ! Multi(Persist(1, Persist(2, "a-1")), Persist(3, "a-2"))
        val w1 = expectWrite(subject, Msgs(Persist(2, "a-1")), Msgs("a-2"))
        confirm(w1)
        expectMsg(Done(1, 1))
        expectMsg(Done(3, 1))
        val w2 = expectWrite(subject, Msgs("a-1"))
        confirm(w2)
        expectMsg(Done(2, 1))
        subject ! PoisonPill
        expectMsg(PostStop("test-3"))
        journal.msgAvailable should ===(false)
      }

      "using large number of persist() calls" in {
        val subject = startActor("test-4")
        subject ! Multi(Vector.tabulate(30)(i => Persist(i, s"a-$i")): _*)
        val w1 = expectWrite(subject, Vector.tabulate(30)(i => Msgs(s"a-$i")): _*)
        confirm(w1)
        for (i <- 0 until 30) expectMsg(Done(i, 1))
        subject ! PoisonPill
        expectMsg(PostStop("test-4"))
        journal.msgAvailable should ===(false)
      }

      "using large number of persistAsync() calls" in {
        def msgs(start: Int, end: Int) = (start until end).map(i => Msgs(s"a-$i-1", s"a-$i-2"))
        def commands(start: Int, end: Int) = (start until end).map(i => PersistAsync(i, s"a-$i-1", s"a-$i-2"))
        def expectDone(start: Int, end: Int) = for (i <- start until end; j <- 1 to 2) expectMsg(Done(i, j))

        val subject = startActor("test-5")
        subject ! PersistAsync(-1, "a" +: commands(20, 30): _*)
        subject ! Multi(commands(0, 10): _*)
        subject ! Multi(commands(10, 20): _*)
        val w0 = expectWrite(subject, Msgs("a" +: commands(20, 30): _*))
        journal.expectNoMessage(300.millis)
        confirm(w0)
        (1 to 11).foreach(x => expectMsg(Done(-1, x)))
        val w1 = expectWrite(subject, msgs(0, 20): _*)
        journal.expectNoMessage(300.millis)
        confirm(w1)
        expectDone(0, 20)
        val w2 = expectWrite(subject, msgs(20, 30): _*)
        confirm(w2)
        expectDone(20, 30)
        subject ! PoisonPill
        expectMsg(PostStop("test-5"))
        journal.msgAvailable should ===(false)
      }

      "the actor fails with queued events" in {
        val subject = startActor("test-6")
        subject ! PersistAsync(1, "a-1")
        val w1 = expectWrite(subject, Msgs("a-1"))
        subject ! PersistAsync(2, "a-2")
        EventFilter[Exception](message = "K-BOOM!", occurrences = 1).intercept {
          subject ! Fail(new Exception("K-BOOM!"))
          expectMsg(PreRestart("test-6"))
          expectMsg(PostRestart("test-6"))
          journal.expectMsgType[ReplayMessages]
        }
        journal.reply(RecoverySuccess(0L))
        expectMsg(RecoveryCompleted)
        confirm(w1)
        subject ! PoisonPill
        expectMsg(PostStop("test-6"))
        journal.msgAvailable should ===(false)
      }

    }

  }
}
