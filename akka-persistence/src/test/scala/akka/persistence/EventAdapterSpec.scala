/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.collection.immutable

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor._
import akka.event.Logging
import akka.persistence.journal.{ EventAdapter, EventSeq, SingleEventSeq }
import akka.testkit.ImplicitSender

object EventAdapterSpec {

  final val JournalModelClassName = classOf[EventAdapterSpec].getCanonicalName + "$" + classOf[JournalModel].getSimpleName
  trait JournalModel {
    def payload: Any
    def tags: immutable.Set[String]
  }
  final case class Tagged(payload: Any, tags: immutable.Set[String]) extends JournalModel
  final case class NotTagged(payload: Any) extends JournalModel {
    override def tags = Set.empty
  }

  final val DomainEventClassName = classOf[EventAdapterSpec].getCanonicalName + "$" + classOf[DomainEvent].getSimpleName
  trait DomainEvent
  final case class TaggedDataChanged(tags: immutable.Set[String], value: Int) extends DomainEvent
  final case class UserDataChanged(countryCode: String, age: Int) extends DomainEvent

  class UserAgeTaggingAdapter extends EventAdapter {
    val Adult = Set("adult")
    val Minor = Set("minor")

    override def toJournal(event: Any): Any = event match {
      case e: UserDataChanged if e.age > 18 => Tagged(e, Adult)
      case e @ UserDataChanged(_, _)        => Tagged(e, Minor)
      case e                                => NotTagged(e)
    }
    override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single {
      event match {
        case m: JournalModel => m.payload
        case _               => throw new RuntimeException()
      }
    }

    override def manifest(event: Any): String = ""
  }

  class ReplayPassThroughAdapter extends UserAgeTaggingAdapter {
    override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single {
      event match {
        case m: JournalModel => m // don't unpack, just pass through the JournalModel
        case _               => throw new RuntimeException()
      }
    }
  }

  class LoggingAdapter(system: ExtendedActorSystem) extends EventAdapter {
    final val log = Logging(system, classOf[EventAdapterSpec])
    override def toJournal(event: Any): Any = {
      log.info("On its way to the journal: []: " + event)
      event
    }
    override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single {
      log.info("On its way out from the journal: []: " + event)
      event
    }

    override def manifest(event: Any): String = ""
  }

  class PersistAllIncomingActor(name: String, override val journalPluginId: String)
      extends NamedPersistentActor(name)
      with PersistentActor {

    var state: List[Any] = Nil

    val persistIncoming: Receive = {
      case GetState =>
        state.reverse.foreach { sender() ! _ }
      case in =>
        persist(in) { e =>
          state ::= e
          sender() ! e
        }
    }

    override def receiveRecover = {
      case RecoveryCompleted => // ignore
      case e                 => state ::= e
    }
    override def receiveCommand = persistIncoming
  }

}

class EventAdapterSpec(journalName: String, journalConfig: Config, adapterConfig: Config)
    extends PersistenceSpec(journalConfig.withFallback(adapterConfig))
    with ImplicitSender {

  import EventAdapterSpec._

  def this() =
    this(
      "inmem",
      PersistenceSpec.config("inmem", "InmemPersistentTaggingSpec"),
      ConfigFactory.parseString(s"""
         |akka.persistence.journal {
         |
         |  common-event-adapters {
         |    age                 = "${classOf[EventAdapterSpec].getCanonicalName}$$UserAgeTaggingAdapter"
         |    replay-pass-through = "${classOf[EventAdapterSpec].getCanonicalName}$$ReplayPassThroughAdapter"
         |  }
         |
         |  inmem {
         |    event-adapters = $${akka.persistence.journal.common-event-adapters}
         |    event-adapter-bindings {
         |      "${EventAdapterSpec.DomainEventClassName}"  = age
         |      "${EventAdapterSpec.JournalModelClassName}" = age
         |    }
         |  }
         |
         |  with-actor-system {
         |    class = $${akka.persistence.journal.inmem.class}
         |    dir = "journal-1"
         |
         |    event-adapters {
         |      logging = "${classOf[EventAdapterSpec].getCanonicalName}$$LoggingAdapter"
         |    }
         |    event-adapter-bindings {
         |      "java.lang.Object" = logging
         |    }
         |  }
         |
         |  replay-pass-through-adapter-journal {
         |    class = $${akka.persistence.journal.inmem.class}
         |    dir = "journal-2"
         |
         |    event-adapters = $${akka.persistence.journal.common-event-adapters}
         |    event-adapter-bindings {
         |      "${EventAdapterSpec.JournalModelClassName}" = replay-pass-through
         |      "${EventAdapterSpec.DomainEventClassName}"  = replay-pass-through
         |    }
         |  }
         |
         |  no-adapter {
         |    class = $${akka.persistence.journal.inmem.class}
         |    dir = "journal-3"
         |  }
         |}
      """.stripMargin))

  def persister(name: String, journalId: String = journalName) =
    system.actorOf(Props(classOf[PersistAllIncomingActor], name, "akka.persistence.journal." + journalId))

  def toJournal(in: Any, journalId: String = journalName) =
    Persistence(system).adaptersFor("akka.persistence.journal." + journalId).get(in.getClass).toJournal(in)

  def fromJournal(in: Any, journalId: String = journalName) =
    Persistence(system).adaptersFor("akka.persistence.journal." + journalId).get(in.getClass).fromJournal(in, "")

  "EventAdapter" must {

    "wrap with tags" in {
      val event = UserDataChanged("name", 42)
      toJournal(event) should equal(Tagged(event, Set("adult")))
    }

    "unwrap when reading" in {
      val event = UserDataChanged("name", 42)
      val tagged = Tagged(event, Set("adult"))

      toJournal(event) should equal(tagged)
      fromJournal(tagged) should equal(SingleEventSeq(event))
    }

    "create adapter requiring ActorSystem" in {
      val event = UserDataChanged("name", 42)
      toJournal(event, "with-actor-system") should equal(event)
      fromJournal(event, "with-actor-system") should equal(SingleEventSeq(event))
    }
    "store events after applying adapter" in {
      val replayPassThroughJournalId = "replay-pass-through-adapter-journal"

      val p1 = persister("p1", journalId = replayPassThroughJournalId)
      val m1 = UserDataChanged("name", 64)
      val m2 = "hello"
      p1 ! m1
      p1 ! m2
      expectMsg(m1)
      expectMsg(m2)

      watch(p1)
      p1 ! PoisonPill
      expectTerminated(p1)

      val p11 = persister("p1", journalId = replayPassThroughJournalId)
      p11 ! GetState
      expectMsg(Tagged(m1, Set("adult")))
      expectMsg(m2)
    }
    "work when plugin defines no adapter" in {
      val p2 = persister("p2", journalId = "no-adapter")
      val m1 = UserDataChanged("name", 64)
      val m2 = "hello"
      p2 ! m1
      p2 ! m2
      expectMsg(m1)
      expectMsg(m2)

      watch(p2)
      p2 ! PoisonPill
      expectTerminated(p2)

      val p22 = persister("p2", "no-adapter")
      p22 ! GetState
      expectMsg(m1)
      expectMsg(m2)
    }
  }
}
