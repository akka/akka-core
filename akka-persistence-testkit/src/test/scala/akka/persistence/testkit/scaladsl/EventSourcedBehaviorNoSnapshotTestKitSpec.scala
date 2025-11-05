/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKitSpec.TestCounter
import akka.persistence.typed.PersistenceId

class EventSourcedBehaviorNoSnapshotTestKitSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
    akka.persistence.testkit.events.serialize = off
    akka.persistence.testkit.snapshots.serialize = off
    """).withFallback(PersistenceTestKitPlugin.config))
    with AnyWordSpecLike
    with LogCapturing {

  private def createTestKit() = {
    EventSourcedBehaviorTestKit[TestCounter.Command, TestCounter.Event, TestCounter.State](
      system,
      TestCounter(PersistenceId.ofUniqueId("test")))
  }

  "EventSourcedBehaviorTestKit" when {
    "snapshots are not enabled" must {
      "not provide SnapshotTestKit" in {
        val eventSourcedTestKit = createTestKit()

        eventSourcedTestKit.snapshotTestKit shouldBe empty
      }

      "fail initializing from snapshot" in {
        val eventSourcedTestKit = createTestKit()

        val ex = intercept[IllegalArgumentException] {
          eventSourcedTestKit.initialize(TestCounter.RealState(1, Vector(0)))
        }
        ex.getMessage shouldEqual "Cannot initialize from state when snapshots are not used."
      }

      "initialize from event" in {
        val eventSourcedTestKit = createTestKit()
        eventSourcedTestKit.initialize(TestCounter.Incremented(1))

        val result = eventSourcedTestKit.runCommand[TestCounter.State](TestCounter.GetValue(_))
        result.reply shouldEqual TestCounter.RealState(1, Vector(0))
      }
    }
  }
}
