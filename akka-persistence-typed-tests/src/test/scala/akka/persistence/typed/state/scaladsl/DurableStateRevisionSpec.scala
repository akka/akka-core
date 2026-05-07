/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.Dropped
import akka.actor.testkit.typed.scaladsl.{ LogCapturing, LoggingTestKit, ScalaTestWithActorTestKit, TestProbe }
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.RecoveryCompleted

object DurableStateRevisionSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))

}

class DurableStateRevisionSpec
    extends ScalaTestWithActorTestKit(DurableStateRevisionSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private def durableState(ctx: ActorContext[String], pid: PersistenceId, probe: ActorRef[String]) = {
    DurableStateBehavior[String, String](
      pid,
      "",
      (state, command) =>
        state match {
          case "stashing" =>
            command match {
              case "unstash" =>
                probe ! s"${DurableStateBehavior.lastSequenceNumber(ctx)} unstash"
                Effect.persist("normal").thenUnstashAll()
              case _ =>
                Effect.stash()
            }
          case _ =>
            command match {
              case "cmd" =>
                probe ! s"${DurableStateBehavior.lastSequenceNumber(ctx)} onCommand"
                Effect.persist("state").thenRun(_ => probe ! s"${DurableStateBehavior.lastSequenceNumber(ctx)} thenRun")
              case "stash" =>
                probe ! s"${DurableStateBehavior.lastSequenceNumber(ctx)} stash"
                Effect.persist("stashing")
              case "snapshot" =>
                Effect.persist("snapshot")
            }
        }).receiveSignal {
      case (_, RecoveryCompleted) =>
        probe ! s"${DurableStateBehavior.lastSequenceNumber(ctx)} onRecoveryComplete"
    }
  }

  private def behavior(pid: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup(ctx => durableState(ctx, pid, probe))

  private def behaviorWithCustomStashSize(
      pid: PersistenceId,
      probe: ActorRef[String],
      customStashSize: Int): Behavior[String] =
    Behaviors.setup(ctx => durableState(ctx, pid, probe).withStashCapacity(customStashSize))

  "The revision number" must {

    "be accessible in the handlers" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("pid-1"), probe.ref))
      probe.expectMessage("0 onRecoveryComplete")

      ref ! "cmd"
      probe.expectMessage("0 onCommand")
      probe.expectMessage("1 thenRun")

      ref ! "cmd"
      probe.expectMessage("1 onCommand")
      probe.expectMessage("2 thenRun")

      testKit.stop(ref)
      probe.expectTerminated(ref)

      // and during recovery
      val ref2 = spawn(behavior(PersistenceId.ofUniqueId("pid-1"), probe.ref))
      probe.expectMessage("2 onRecoveryComplete")

      ref2 ! "cmd"
      probe.expectMessage("2 onCommand")
      probe.expectMessage("3 thenRun")
    }

    "be available while unstashing" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("pid-2"), probe.ref))
      probe.expectMessage("0 onRecoveryComplete")

      ref ! "stash"
      ref ! "cmd"
      ref ! "cmd"
      ref ! "cmd"
      ref ! "unstash"
      probe.expectMessage("0 stash")
      probe.expectMessage("1 unstash")
      probe.expectMessage("2 onCommand")
      probe.expectMessage("3 thenRun")
      probe.expectMessage("3 onCommand")
      probe.expectMessage("4 thenRun")
      probe.expectMessage("4 onCommand")
      probe.expectMessage("5 thenRun")
    }

    "not fail when snapshotting" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("pid-3"), probe.ref))
      probe.expectMessage("0 onRecoveryComplete")

      ref ! "cmd"
      ref ! "snapshot"
      ref ! "cmd"

      probe.expectMessage("0 onCommand") // first command
      probe.expectMessage("1 thenRun")
      probe.expectMessage("2 onCommand") // second command
      probe.expectMessage("3 thenRun")
    }

    "discard when custom stash has reached limit with default dropped setting" in {
      val customLimit = 100
      val probe = TestProbe[AnyRef]()
      system.toClassic.eventStream.subscribe(probe.ref.toClassic, classOf[Dropped])
      val durableState = spawn(behaviorWithCustomStashSize(PersistenceId.ofUniqueId("pid-4"), probe.ref, customLimit))
      probe.expectMessage("0 onRecoveryComplete")

      durableState ! "stash"

      probe.expectMessage("0 stash")

      LoggingTestKit.warn("Stash buffer is full, dropping message").expect {
        (0 to customLimit).foreach { _ =>
          durableState ! s"cmd"
        }
      }
      probe.expectMessageType[Dropped]

      durableState ! "unstash"
      probe.expectMessage("1 unstash")

      val lastSequenceId = 2
      (lastSequenceId until customLimit + lastSequenceId).foreach { n =>
        probe.expectMessage(s"$n onCommand")
        probe.expectMessage(s"${n + 1} thenRun") // after persisting
      }

      durableState ! s"cmd"
      probe.expectMessage(s"${102} onCommand")
      probe.expectMessage(s"${103} thenRun")

    }
  }
}
