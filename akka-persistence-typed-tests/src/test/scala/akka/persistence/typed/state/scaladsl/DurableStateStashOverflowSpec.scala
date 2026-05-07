/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId

// Reproducer for #29401
object DurableStateStashOverflowSpec {

  object Stasher {
    sealed trait Command
    final case class First(replyTo: ActorRef[Done]) extends Command
    final case class Hey(replyTo: ActorRef[Done]) extends Command
    case object UnstashThem extends Command
    final case class State(stashing: Boolean)

    def apply(persistenceId: PersistenceId): Behavior[Command] =
      DurableStateBehavior[Command, State](
        persistenceId,
        emptyState = State(stashing = true),
        commandHandler = { (state, command) =>
          command match {
            case First(replyTo) =>
              Effect.reply(replyTo)(Done)
            case Hey(replyTo) =>
              if (state.stashing)
                Effect.stash()
              else
                Effect.reply(replyTo)(Done)
            case UnstashThem =>
              Effect.persist(state.copy(stashing = false)).thenUnstashAll()
          }
        })
  }

  def conf: Config = PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString(s"""
    akka.persistence {
           typed {
             stash-capacity = 5000 # enough to fail on stack size
             stash-overflow-strategy = "drop"
           }
         }
    """))
}

class DurableStateStashOverflowSpec
    extends ScalaTestWithActorTestKit(DurableStateStashOverflowSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  import DurableStateStashOverflowSpec.Stasher

  "Stashing in a busy durable state behavior" must {

    "not cause stack overflow" in {
      val ref = spawn(Stasher(PersistenceId.ofUniqueId("id-1")))

      val probe = testKit.createTestProbe[Done]()

      ref.tell(Stasher.First(probe.ref))
      probe.expectMessage(Done)
      // now we know that recovery is completed, so that we don't use internal stash during recovery in this test

      val droppedMessageProbe = testKit.createDroppedMessageProbe()
      val stashCapacity = testKit.config.getInt("akka.persistence.typed.stash-capacity")

      for (_ <- 0 until (stashCapacity)) {
        ref.tell(Stasher.Hey(probe.ref))
      }

      droppedMessageProbe.expectNoMessage()
      ref.tell(Stasher.Hey(probe.ref))
      droppedMessageProbe.receiveMessage()

      ref.tell(Stasher.UnstashThem)
      probe.receiveMessages(stashCapacity)
    }

  }

}
