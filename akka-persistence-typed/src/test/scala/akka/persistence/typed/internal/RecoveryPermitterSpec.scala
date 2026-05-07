/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.{ TypedActorRefOps, TypedActorSystemOps }
import akka.persistence.Persistence
import akka.persistence.RecoveryPermitter.{ RecoveryPermitGranted, RequestRecoveryPermit, ReturnRecoveryPermit }
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler

object RecoveryPermitterSpec {

  class TE extends RuntimeException("Boom!") with NoStackTrace

  trait State

  object EmptyState extends State

  object EventState extends State

  trait Command

  case object StopActor extends Command

  trait Event

  case object Recovered extends Event

  def persistentBehavior(
      name: String,
      commandProbe: TestProbe[Any],
      eventProbe: TestProbe[Any],
      throwOnRecovery: Boolean = false): Behavior[Command] =
    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId.ofUniqueId(name),
      emptyState = EmptyState,
      commandHandler = CommandHandler.command {
        case StopActor => Effect.stop()
        case command   => commandProbe.ref ! command; Effect.none
      },
      eventHandler = { (state, event) =>
        eventProbe.ref ! event; state
      }).receiveSignal {
      case (_, RecoveryCompleted) =>
        eventProbe.ref ! Recovered
        if (throwOnRecovery) throw new TE
    }

  def forwardingBehavior(target: TestProbe[Any]): Behavior[Any] =
    Behaviors.receive[Any] { (_, any) =>
      target.ref ! any; Behaviors.same
    }
}

class RecoveryPermitterSpec
    extends ScalaTestWithActorTestKit("""
      akka.persistence.max-concurrent-recoveries = 3
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
      akka.loggers = ["akka.testkit.TestEventListener"]
      """)
    with AnyWordSpecLike
    with LogCapturing {

  import RecoveryPermitterSpec._

  implicit val classicSystem: ActorSystem = system.toClassic

  private val permitter = Persistence(classicSystem).recoveryPermitter

  def requestPermit(p: TestProbe[Any]): Unit = {
    permitter.tell(RequestRecoveryPermit, p.ref.toClassic)
    p.expectMessage(RecoveryPermitGranted)
  }

  val p1 = createTestProbe[Any]()
  val p2 = createTestProbe[Any]()
  val p3 = createTestProbe[Any]()
  val p4 = createTestProbe[Any]()
  val p5 = createTestProbe[Any]()

  "RecoveryPermitter" must {
    "grant permits up to the limit" in {
      requestPermit(p1)
      requestPermit(p2)
      requestPermit(p3)

      permitter.tell(RequestRecoveryPermit, p4.ref.toClassic)
      permitter.tell(RequestRecoveryPermit, p5.ref.toClassic)
      p4.expectNoMessage(100.millis)
      p5.expectNoMessage(10.millis)

      permitter.tell(ReturnRecoveryPermit, p2.ref.toClassic)
      p4.expectMessage(RecoveryPermitGranted)
      p5.expectNoMessage(100.millis)

      permitter.tell(ReturnRecoveryPermit, p1.ref.toClassic)
      p5.expectMessage(RecoveryPermitGranted)

      permitter.tell(ReturnRecoveryPermit, p3.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p4.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p5.ref.toClassic)
    }

    "grant recovery when all permits not used" in {
      requestPermit(p1)

      spawn(persistentBehavior("p2", p2, p2))
      p2.expectMessage(Recovered)
      permitter.tell(ReturnRecoveryPermit, p1.ref.toClassic)
    }

    "delay recovery when all permits used" in {
      requestPermit(p1)
      requestPermit(p2)
      requestPermit(p3)

      val persistentActor = spawn(persistentBehavior("p4", p4, p4))
      persistentActor ! StopActor
      p4.expectNoMessage(200.millis)

      permitter.tell(ReturnRecoveryPermit, p3.ref.toClassic)
      p4.expectMessage(Recovered)
      p4.expectTerminated(persistentActor, 1.second)

      permitter.tell(ReturnRecoveryPermit, p1.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p2.ref.toClassic)
    }

    "return permit when actor is pre-maturely terminated before holding permit" in {
      requestPermit(p1)
      requestPermit(p2)
      requestPermit(p3)

      val persistentActor = spawn(persistentBehavior("p4", p4, p4))
      p4.expectNoMessage(100.millis)

      permitter.tell(RequestRecoveryPermit, p5.ref.toClassic)
      p5.expectNoMessage(100.millis)

      // PoisonPill is not stashed
      persistentActor.toClassic ! PoisonPill

      // persistentActor didn't hold a permit so still
      p5.expectNoMessage(100.millis)

      permitter.tell(ReturnRecoveryPermit, p1.ref.toClassic)
      p5.expectMessage(RecoveryPermitGranted)

      permitter.tell(ReturnRecoveryPermit, p2.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p3.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p5.ref.toClassic)
    }

    "return permit when actor is pre-maturely terminated when holding permit" in {
      val actor = spawn(forwardingBehavior(p1))
      permitter.tell(RequestRecoveryPermit, actor.toClassic)
      p1.expectMessage(RecoveryPermitGranted)

      requestPermit(p2)
      requestPermit(p3)

      permitter.tell(RequestRecoveryPermit, p4.ref.toClassic)
      p4.expectNoMessage(100.millis)

      actor.toClassic ! PoisonPill
      p4.expectMessage(RecoveryPermitGranted)

      permitter.tell(ReturnRecoveryPermit, p2.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p3.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p4.ref.toClassic)
    }

    "return permit when actor throws from RecoveryCompleted" in {
      requestPermit(p1)
      requestPermit(p2)

      val stopProbe = createTestProbe[ActorRef[Command]]()
      val parent =
        LoggingTestKit.error("Exception during recovery.").expect {
          spawn(Behaviors.setup[Command](ctx => {
            val persistentActor =
              ctx.spawnAnonymous(persistentBehavior("p3", p3, p3, throwOnRecovery = true))
            Behaviors.receive[Command] {
              case (_, StopActor) =>
                stopProbe.ref ! persistentActor
                ctx.stop(persistentActor)
                Behaviors.same
              case (_, message) =>
                persistentActor ! message
                Behaviors.same
            }
          }))
        }
      p3.expectMessage(Recovered)
      // stop it
      parent ! StopActor
      val persistentActor = stopProbe.receiveMessage()
      stopProbe.expectTerminated(persistentActor, 1.second)

      requestPermit(p4)

      permitter.tell(ReturnRecoveryPermit, p1.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p2.ref.toClassic)
      permitter.tell(ReturnRecoveryPermit, p4.ref.toClassic)
    }
  }
}
