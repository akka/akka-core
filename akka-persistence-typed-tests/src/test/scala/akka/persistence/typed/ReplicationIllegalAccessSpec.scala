/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior, ReplicatedEventSourcing }
import akka.serialization.jackson.CborSerializable

object ReplicationIllegalAccessSpec {

  val R1 = ReplicaId("R1")
  val R2 = ReplicaId("R1")
  val AllReplicas = Set(R1, R2)

  sealed trait Command
  case class AccessInCommandHandler(replyTo: ActorRef[Thrown]) extends Command
  case class AccessInPersistCallback(replyTo: ActorRef[Thrown]) extends Command

  case class Thrown(exception: Option[Throwable])

  case class State(all: List[String]) extends CborSerializable

  def apply(entityId: String, replica: ReplicaId): Behavior[Command] = {
    ReplicatedEventSourcing.commonJournalConfig(
      ReplicationId("IllegalAccessSpec", entityId, replica),
      AllReplicas,
      PersistenceTestKitReadJournal.Identifier)(
      replicationContext =>
        EventSourcedBehavior[Command, String, State](
          replicationContext.persistenceId,
          State(Nil),
          (_, command) =>
            command match {
              case AccessInCommandHandler(replyTo) =>
                val exception = try {
                  replicationContext.origin
                  None
                } catch {
                  case t: Throwable =>
                    Some(t)
                }
                replyTo ! Thrown(exception)
                Effect.none
              case AccessInPersistCallback(replyTo) =>
                Effect.persist("cat").thenRun { _ =>
                  val exception = try {
                    replicationContext.concurrent
                    None
                  } catch {
                    case t: Throwable =>
                      Some(t)
                  }
                  replyTo ! Thrown(exception)
                }
            },
          (state, event) => state.copy(all = event :: state.all)))
  }

}

class ReplicationIllegalAccessSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing
    with Eventually {
  import ReplicationIllegalAccessSpec._
  "ReplicatedEventSourcing" should {
    "detect illegal access to context in command handler" in {
      val probe = createTestProbe[Thrown]()
      val ref = spawn(ReplicationIllegalAccessSpec("id1", R1))
      ref ! AccessInCommandHandler(probe.ref)
      val thrown: Throwable = probe.expectMessageType[Thrown].exception.get
      thrown.getMessage should include("from the event handler")
    }
    "detect illegal access to context in persist thenRun" in {
      val probe = createTestProbe[Thrown]()
      val ref = spawn(ReplicationIllegalAccessSpec("id1", R1))
      ref ! AccessInPersistCallback(probe.ref)
      val thrown: Throwable = probe.expectMessageType[Thrown].exception.get
      thrown.getMessage should include("from the event handler")
    }
    "detect illegal access in the factory" in {
      val exception = intercept[UnsupportedOperationException] {
        ReplicatedEventSourcing.commonJournalConfig(
          ReplicationId("IllegalAccessSpec", "id2", R1),
          AllReplicas,
          PersistenceTestKitReadJournal.Identifier) { replicationContext =>
          replicationContext.origin
          ???
        }
      }
      exception.getMessage should include("from the event handler")
    }
  }
}
