/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId

object NullEmptyStateSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))
}

class NullEmptyStateSpec
    extends ScalaTestWithActorTestKit(NullEmptyStateSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  implicit val testSettings: TestKitSettings = TestKitSettings(system)

  def nullState(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    DurableStateBehavior[String, String](
      persistenceId,
      emptyState = null,
      commandHandler = (state, command) => {
        if (command == "stop")
          Effect.stop()
        else if (state == null)
          Effect.persist(command).thenReply(probe)(newState => newState)
        else
          Effect.persist(s"$state:$command").thenReply(probe)(newState => newState)
      })

  "A typed persistent actor with null empty state" must {
    "persist and update state" in {
      val probe = TestProbe[String]()
      val b = nullState(PersistenceId.ofUniqueId("a"), probe.ref)
      val ref1 = spawn(b)
      ref1 ! "one"
      probe.expectMessage("one")
      ref1 ! "two"
      probe.expectMessage("one:two")
      ref1 ! "stop"
      probe.expectTerminated(ref1)

      val ref2 = spawn(b)
      ref2 ! "three"
      probe.expectMessage("one:two:three")
    }
  }
}
