/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.RecoveryTimedOut
import akka.persistence.journal.SteppingInmemJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryFailed
import akka.persistence.typed.internal.JournalFailureException

object EventSourcedBehaviorRecoveryTimeoutSpec {

  val journalId = "event-sourced-behavior-recovery-timeout-spec"

  def config: Config =
    SteppingInmemJournal
      .config(journalId)
      .withFallback(ConfigFactory.parseString("""
        akka.persistence.journal.stepping-inmem.recovery-event-timeout=1s
        """))
      .withFallback(ConfigFactory.parseString("""
        akka.loglevel = INFO
        """))

  def testBehavior(persistenceId: PersistenceId, probe: ActorRef[AnyRef]): Behavior[String] =
    Behaviors.setup { _ =>
      EventSourcedBehavior[String, String, String](
        persistenceId,
        emptyState = "",
        commandHandler = (_, command) => Effect.persist(command).thenRun(_ => probe ! command),
        eventHandler = (state, evt) => state + evt).receiveSignal {
        case (_, RecoveryFailed(cause)) =>
          probe ! cause
      }
    }

}

class EventSourcedBehaviorRecoveryTimeoutSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorRecoveryTimeoutSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import EventSourcedBehaviorRecoveryTimeoutSpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()}")

  import akka.actor.typed.scaladsl.adapter._
  // needed for SteppingInmemJournal.step
  private implicit val classicSystem: akka.actor.ActorSystem = system.toClassic

  "The recovery timeout" must {

    "fail recovery if timeout is not met when recovering" in {
      val probe = createTestProbe[AnyRef]()
      val pid = nextPid()
      val persisting = spawn(testBehavior(pid, probe.ref))

      probe.awaitAssert(SteppingInmemJournal.getRef(journalId), 3.seconds)
      val journal = SteppingInmemJournal.getRef(journalId)

      // initial read highest
      SteppingInmemJournal.step(journal)

      persisting ! "A"
      SteppingInmemJournal.step(journal)
      probe.expectMessage("A")

      testKit.stop(persisting)
      probe.expectTerminated(persisting)

      // now replay, but don't give the journal any tokens to replay events
      // so that we cause the timeout to trigger
      LoggingTestKit
        .error[JournalFailureException]
        .withMessageRegex("Exception during recovery.*Replay timed out")
        .expect {
          val replaying = spawn(testBehavior(pid, probe.ref))

          // initial read highest
          SteppingInmemJournal.step(journal)

          probe.expectMessageType[RecoveryTimedOut]
          probe.expectTerminated(replaying)
        }

      // avoid having it stuck in the next test from the
      // last read request above
      SteppingInmemJournal.step(journal)
    }

  }
}
