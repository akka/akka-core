/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import scala.annotation.nowarn

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.persistence._
import akka.testkit.{ AkkaSpec, TestProbe }

object SharedLeveldbJournalSpec {
  val config = ConfigFactory.parseString("""
      akka {
        actor {
          provider = remote
        }
        persistence {
          journal {
            plugin = "akka.persistence.journal.leveldb-shared"
            leveldb-shared.store.dir = target/journal-SharedLeveldbJournalSpec
          }
          snapshot-store {
            plugin = "akka.persistence.snapshot-store.local"
            local.dir = target/snapshots-SharedLeveldbJournalSpec
          }
        }
        remote {
          artery.canonical {
            hostname = "127.0.0.1"
            port = 0
          }
        }
        loglevel = ERROR
        log-dead-letters = 0
        log-dead-letters-during-shutdown = off
        test.single-expect-default = 10s
      }
    """).withFallback(SharedLeveldbJournal.configToEnableJavaSerializationForTest)

  class ExamplePersistentActor(probe: ActorRef, name: String) extends NamedPersistentActor(name) {
    override def receiveRecover = {
      case RecoveryCompleted => // ignore
      case payload           => probe ! payload
    }
    override def receiveCommand = {
      case payload =>
        persist(payload) { _ =>
          probe ! payload
        }
    }
  }

  class ExampleApp(probe: ActorRef, storePath: ActorPath) extends Actor {
    val p = context.actorOf(Props(classOf[ExamplePersistentActor], probe, context.system.name))

    def receive = {
      case ActorIdentity(1, Some(store)) => SharedLeveldbJournal.setStore(store, context.system)
      case m                             => p.forward(m)
    }

    override def preStart(): Unit =
      context.actorSelection(storePath) ! Identify(1)
  }

}

class SharedLeveldbJournalSpec extends AkkaSpec(SharedLeveldbJournalSpec.config) with Cleanup {
  import SharedLeveldbJournalSpec._

  val systemA = ActorSystem("SysA", system.settings.config)
  val systemB = ActorSystem("SysB", system.settings.config)

  override protected def afterTermination(): Unit = {
    shutdown(systemA)
    shutdown(systemB)
    super.afterTermination()
  }

  "A LevelDB store".can {
    "be shared by multiple actor systems" in {

      val probeA = new TestProbe(systemA)
      val probeB = new TestProbe(systemB)

      val storeConfig = system.settings.config.getConfig("akka.persistence.journal.leveldb-shared")
      @nowarn
      val sharedLeveldbStoreCls = classOf[SharedLeveldbStore]
      system.actorOf(Props(sharedLeveldbStoreCls, storeConfig), "store")
      val storePath = RootActorPath(system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress) / "user" / "store"

      val appA = systemA.actorOf(Props(classOf[ExampleApp], probeA.ref, storePath))
      val appB = systemB.actorOf(Props(classOf[ExampleApp], probeB.ref, storePath))

      appA ! "a1"
      appB ! "b1"

      probeA.expectMsg("a1")
      probeB.expectMsg("b1")

      val recoveredAppA = systemA.actorOf(Props(classOf[ExampleApp], probeA.ref, storePath))
      val recoveredAppB = systemB.actorOf(Props(classOf[ExampleApp], probeB.ref, storePath))

      recoveredAppA ! "a2"
      recoveredAppB ! "b2"

      probeA.expectMsg("a1")
      probeA.expectMsg("a2")

      probeB.expectMsg("b1")
      probeB.expectMsg("b2")
    }
  }
}
