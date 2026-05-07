/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, Props }
import akka.event.Logging
import akka.event.Logging.Warning
import akka.testkit.{ EventFilter, ImplicitSender, TestEvent }

object OptionalSnapshotStoreSpec {

  class AnyPersistentActor(name: String) extends PersistentActor {
    var lastSender = context.system.deadLetters

    override def persistenceId = name
    override def receiveCommand: Receive = {
      case s: String =>
        lastSender = sender()
        saveSnapshot(s)
      case f: SaveSnapshotFailure => lastSender ! f
      case s: SaveSnapshotSuccess => lastSender ! s
    }
    override def receiveRecover: Receive = Actor.emptyBehavior
  }

  class PickedSnapshotStorePersistentActor(name: String) extends AnyPersistentActor(name) {
    override def snapshotPluginId: String = "akka.persistence.snapshot-store.local"
  }
}

class OptionalSnapshotStoreSpec extends PersistenceSpec(ConfigFactory.parseString(s"""
    akka.persistence.publish-plugin-commands = on
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"

    akka.actor.warn-about-java-serializer-usage = off

    # snapshot store plugin is NOT defined, things should still work
    akka.persistence.snapshot-store.local.dir = "target/snapshots-${classOf[OptionalSnapshotStoreSpec].getName}/"
  """)) with ImplicitSender {
  import OptionalSnapshotStoreSpec._

  system.eventStream.publish(TestEvent.Mute(EventFilter[akka.pattern.AskTimeoutException]()))

  "Persistence extension" must {
    "initialize properly even in absence of configured snapshot store" in {
      system.eventStream.subscribe(testActor, classOf[Logging.Warning])
      system.actorOf(Props(classOf[AnyPersistentActor], name))
      val message = expectMsgType[Warning].message.toString
      message should include("No default snapshot store configured")
    }

    "fail if PersistentActor tries to saveSnapshot without snapshot-store available" in {
      val persistentActor = system.actorOf(Props(classOf[AnyPersistentActor], name))
      persistentActor ! "snap"
      expectMsgType[SaveSnapshotFailure].cause.getMessage should include("No snapshot store configured")
    }

    "successfully save a snapshot when no default snapshot-store configured, yet PersistentActor picked one explicitly" in {
      val persistentActor = system.actorOf(Props(classOf[PickedSnapshotStorePersistentActor], name))
      persistentActor ! "snap"
      expectMsgType[SaveSnapshotSuccess]
    }
  }
}
