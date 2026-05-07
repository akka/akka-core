/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId

object PrimitiveStateSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))
}

class PrimitiveStateSpec
    extends ScalaTestWithActorTestKit(PrimitiveStateSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  def primitiveState(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[Int] =
    DurableStateBehavior[Int, Int](
      persistenceId,
      emptyState = 0,
      commandHandler = (state, command) => {
        if (command < 0)
          Effect.stop()
        else
          Effect.persist(state + command).thenReply(probe)(newState => newState.toString)
      })

  "A typed persistent actor with primitive state" must {
    "persist primitive state and update" in {
      val probe = TestProbe[String]()
      val b = primitiveState(PersistenceId.ofUniqueId("a"), probe.ref)
      val ref1 = spawn(b)
      ref1 ! 1
      probe.expectMessage("1")
      ref1 ! 2
      probe.expectMessage("3")

      ref1 ! -1
      probe.expectTerminated(ref1)

      val ref2 = spawn(b)
      ref2 ! 3
      probe.expectMessage("6")
    }
  }
}
