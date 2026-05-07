/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.crdt

import scala.util.Random

import ORSetSpec.ORSetEntity._

import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.{ ReplicaId, ReplicationBaseSpec }
import akka.persistence.typed.ReplicationBaseSpec.{ R1, R2 }
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.crdt.ORSetSpec.ORSetEntity
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior, ReplicatedEventSourcing }

object ORSetSpec {

  import ReplicationBaseSpec._

  object ORSetEntity {
    sealed trait Command
    final case class Get(replyTo: ActorRef[Set[String]]) extends Command
    final case class Add(elem: String) extends Command
    final case class AddAll(elems: Set[String]) extends Command
    final case class Remove(elem: String) extends Command

    def apply(entityId: String, replica: ReplicaId): Behavior[ORSetEntity.Command] = {

      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("ORSetSpec", entityId, replica),
        AllReplicas,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, ORSet.DeltaOp, ORSet[String]](
          replicationContext.persistenceId,
          ORSet(replica),
          (state, command) =>
            command match {
              case Add(elem) =>
                Effect.persist(state + elem)
              case AddAll(elems) =>
                Effect.persist(state.addAll(elems.toSet))
              case Remove(elem) =>
                Effect.persist(state - elem)
              case Get(replyTo) =>
                Effect.none.thenRun(state => replyTo ! state.elements)

            },
          (state, operation) => state.applyOperation(operation))
      }
    }
  }

}

class ORSetSpec extends ReplicationBaseSpec {

  class Setup {
    val entityId = nextEntityId
    val r1 = spawn(ORSetEntity.apply(entityId, R1))
    val r2 = spawn(ORSetEntity.apply(entityId, R2))
    val r1GetProbe = createTestProbe[Set[String]]()
    val r2GetProbe = createTestProbe[Set[String]]()

    def assertForAllReplicas(state: Set[String]): Unit = {
      eventually {
        r1 ! Get(r1GetProbe.ref)
        r1GetProbe.expectMessage(state)
        r2 ! Get(r2GetProbe.ref)
        r2GetProbe.expectMessage(state)
      }
    }
  }

  def randomDelay(): Unit = {
    // exercise different timing scenarios
    Thread.sleep(Random.nextInt(200).toLong)
  }

  "ORSet Replicated Entity" should {

    "support concurrent updates" in new Setup {
      r1 ! Add("a1")
      r2 ! Add("b1")
      assertForAllReplicas(Set("a1", "b1"))
      r2 ! Remove("b1")
      assertForAllReplicas(Set("a1"))
      r2 ! Add("b1")
      for (n <- 2 to 10) {
        r1 ! Add(s"a$n")
        if (n % 3 == 0)
          randomDelay()
        r2 ! Add(s"b$n")
      }
      r1 ! AddAll((11 to 13).map(n => s"a$n").toSet)
      r2 ! AddAll((11 to 13).map(n => s"b$n").toSet)
      val expected = (1 to 13).flatMap(n => List(s"a$n", s"b$n")).toSet
      assertForAllReplicas(expected)
    }
  }
}
