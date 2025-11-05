/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.typed.PersistenceId

object EventSourcedBehaviorTimersSpec {

  val journalId = "event-sourced-behavior-timers-spec"

  def testBehavior(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup { _ =>
      Behaviors.withTimers { timers =>
        EventSourcedBehavior[String, String, String](
          persistenceId,
          emptyState = "",
          commandHandler = (_, command) =>
            command match {
              case "scheduled" =>
                probe ! "scheduled"
                Effect.none
              case "cmd-0" =>
                timers.startSingleTimer("key", "scheduled", Duration.Zero)
                Effect.none
              case _ =>
                timers.startSingleTimer("key", "scheduled", Duration.Zero)
                Effect.persist(command).thenRun(_ => probe ! command)
            },
          eventHandler = (state, evt) => state + evt)
      }
    }

  def testTimerFromSetupBehavior(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup { _ =>
      Behaviors.withTimers { timers =>
        timers.startSingleTimer("key", "scheduled", Duration.Zero)

        EventSourcedBehavior[String, String, String](
          persistenceId,
          emptyState = "",
          commandHandler = (_, command) =>
            command match {
              case "scheduled" =>
                probe ! "scheduled"
                Effect.none
              case _ =>
                Effect.persist(command).thenRun(_ => probe ! command)
            },
          eventHandler = (state, evt) => state + evt)
      }
    }

}

class EventSourcedBehaviorTimersSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorTimersSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  "EventSourcedBehavior withTimers" must {

    "be able to schedule message" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()
      val ref = spawn(testBehavior(pid, probe.ref))

      ref ! "cmd-0"
      probe.expectMessage("scheduled")
    }

    "not discard timer msg due to stashing" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()
      val ref = spawn(testBehavior(pid, probe.ref))

      ref ! "cmd-1"
      probe.expectMessage("cmd-1")
      probe.expectMessage("scheduled")
    }

    "be able to schedule message from setup" in {
      val probe = createTestProbe[String]()
      val pid = nextPid()
      val ref = spawn(testTimerFromSetupBehavior(pid, probe.ref))

      probe.expectMessage("scheduled")

      (1 to 20).foreach { n =>
        ref ! s"cmd-$n"
      }
      probe.receiveMessages(20)

      // start new instance that is likely to stash the timer message while replaying
      spawn(testTimerFromSetupBehavior(pid, probe.ref))
      probe.expectMessage("scheduled")
    }

  }
}
