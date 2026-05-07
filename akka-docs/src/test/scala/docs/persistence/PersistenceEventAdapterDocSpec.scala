/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence

import akka.actor.{ ExtendedActorSystem, Props }
import akka.persistence.journal.{ EventAdapter, EventSeq }
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import akka.testkit.{ AkkaSpec, TestProbe }
import com.google.gson.{ Gson, JsonElement }

import scala.annotation.nowarn

class PersistenceEventAdapterDocSpec(config: String) extends AkkaSpec(config) {

  def this() =
    this("""
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"

      //#event-adapters-config
      akka.persistence.journal {
        inmem {
          event-adapters {
            tagging        = "docs.persistence.MyTaggingEventAdapter"
            user-upcasting = "docs.persistence.UserUpcastingEventAdapter"
            item-upcasting = "docs.persistence.ItemUpcastingEventAdapter"
          }

          event-adapter-bindings {
            "docs.persistence.Item"        = tagging
            "docs.persistence.TaggedEvent" = tagging
            "docs.persistence.v1.Event"    = [user-upcasting, item-upcasting]
          }
        }
      }
      //#event-adapters-config


      akka.persistence.journal {
        auto-json-store {
          class = "akka.persistence.journal.inmem.InmemJournal" # reuse inmem, as an example

          event-adapters {
            auto-json = "docs.persistence.MyAutoJsonEventAdapter"
          }

          event-adapter-bindings {
            "docs.persistence.DomainEvent" = auto-json  # to journal
            "com.google.gson.JsonElement"  = auto-json  # from journal
          }
        }

        manual-json-store {
          class = "akka.persistence.journal.inmem.InmemJournal" # reuse inmem, as an example

          event-adapters {
            manual-json    = "docs.persistence.MyManualJsonEventAdapter"
          }

          event-adapter-bindings {
            "docs.persistence.DomainEvent" = manual-json  # to journal
            "com.google.gson.JsonElement"  = manual-json  # from journal
          }
        }
      }
    """)

  "MyAutomaticJsonEventAdapter" must {
    "demonstrate how to implement a JSON adapter" in {
      val p = TestProbe()

      val props = Props(new PersistentActor {
        override def persistenceId: String = "json-actor"
        override def journalPluginId: String = "akka.persistence.journal.auto-json-store"

        override def receiveRecover: Receive = {
          case RecoveryCompleted => // ignore...
          case e                 => p.ref ! e
        }

        override def receiveCommand: Receive = {
          case c =>
            persist(c) { e =>
              p.ref ! e
            }
        }
      })

      val p1 = system.actorOf(props)
      val m1 = Person("Caplin", 42)
      val m2 = Box(13)
      p1 ! m1
      p1 ! m2
      p.expectMsg(m1)
      p.expectMsg(m2)

      system.actorOf(props)
      p.expectMsg(m1)
      p.expectMsg(m2)
    }
  }

  "MyManualJsonEventAdapter" must {
    "demonstrate how to implement a JSON adapter" in {
      val p = TestProbe()

      val props = Props(new PersistentActor {
        override def persistenceId: String = "json-actor"
        override def journalPluginId: String = "akka.persistence.journal.manual-json-store"

        override def receiveRecover: Receive = {
          case RecoveryCompleted => // ignore...
          case e                 => p.ref ! e
        }

        override def receiveCommand: Receive = {
          case c =>
            persist(c) { e =>
              p.ref ! e
            }
        }
      })

      val p1 = system.actorOf(props)
      val m1 = Person("Caplin", 42)
      val m2 = Box(13)
      p1 ! m1
      p1 ! m2
      p.expectMsg(m1)
      p.expectMsg(m2)

      system.actorOf(props)
      p.expectMsg(m1)
      p.expectMsg(m2)
    }
  }
}

trait DomainEvent
case class Person(name: String, age: Int) extends DomainEvent
case class Box(length: Int) extends DomainEvent

case class MyTaggingJournalModel(payload: Any, tags: Set[String])

//#identity-event-adapter
@nowarn("msg=never used") // required constructor parameter
class MyEventAdapter(system: ExtendedActorSystem) extends EventAdapter {
  override def manifest(event: Any): String =
    "" // when no manifest needed, return ""

  override def toJournal(event: Any): Any =
    event // identity

  override def fromJournal(event: Any, manifest: String): EventSeq =
    EventSeq.single(event) // identity
}
//#identity-event-adapter

/**
 * This is an example adapter which completely takes care of domain<->json translation.
 * It allows the journal to take care of the manifest handling, which is the FQN of the serialized class.
 */
class MyAutoJsonEventAdapter(system: ExtendedActorSystem) extends EventAdapter {
  private val gson = new Gson

  override def manifest(event: Any): String =
    event.getClass.getCanonicalName

  override def toJournal(event: Any): Any = gson.toJsonTree(event)

  override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single {
    event match {
      case json: JsonElement =>
        val clazz = system.dynamicAccess.getClassFor[Any](manifest).get
        gson.fromJson(json, clazz)
    }
  }
}

@nowarn("msg=never used") // sample snippets
class MyUpcastingEventAdapter(system: ExtendedActorSystem) extends EventAdapter {
  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = ???

  override def fromJournal(event: Any, manifest: String): EventSeq = ???
}

/**
 * This is an example adapter which completely takes care of domain<->json translation.
 * It manually takes care of smuggling through the manifest even if the journal does not do anything in this regard,
 * which is the case in this example since we're persisting to the inmem journal, which does nothing in terms of manifest.
 */
class MyManualJsonEventAdapter(system: ExtendedActorSystem) extends EventAdapter {

  private val gson = new Gson

  override def manifest(event: Any): String = event.getClass.getCanonicalName

  override def toJournal(event: Any): Any = {
    val out = gson.toJsonTree(event).getAsJsonObject

    // optionally can include manifest in the adapted event.
    // some journals will store the manifest transparently
    out.addProperty("_manifest", manifest(event))

    out
  }

  override def fromJournal(event: Any, m: String): EventSeq = event match {
    case json: JsonElement =>
      val manifest = json.getAsJsonObject.get("_manifest").getAsString

      val clazz = system.dynamicAccess.getClassFor[Any](manifest).get
      EventSeq.single(gson.fromJson(json, clazz))
  }
}

@nowarn("msg=never used")
class MyTaggingEventAdapter(system: ExtendedActorSystem) extends EventAdapter {
  override def manifest(event: Any): String = ""

  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case j: MyTaggingJournalModel => EventSeq.single(j)
  }

  override def toJournal(event: Any): Any = {
    event match {
      case Person(_, age) if age >= 18 => MyTaggingJournalModel(event, tags = Set("adult"))
      case Person(_, age)              => MyTaggingJournalModel(event, tags = Set("minor"))
      case _                           => MyTaggingJournalModel(event, tags = Set.empty)
    }
  }
}

object v1 {
  trait Event
  trait UserEvent extends v1.Event
  trait ItemEvent extends v1.Event
}
