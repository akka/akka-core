/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import scala.annotation.nowarn

import akka.actor.typed.ActorSystem
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing

@nowarn("msg=never used")
object ReplicatedEventSourcingCompileOnlySpec {

  //#replicas
  val DCA = ReplicaId("DC-A")
  val DCB = ReplicaId("DC-B")
  val AllReplicas = Set(DCA, DCB)
  //#replicas

  val queryPluginId = ""

  trait Command
  trait State
  trait Event

  object Shared {
    //#factory-shared
    def apply(
        system: ActorSystem[_],
        entityId: String,
        replicaId: ReplicaId): EventSourcedBehavior[Command, State, Event] = {
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("MyReplicatedEntity", entityId, replicaId),
        AllReplicas,
        queryPluginId) { replicationContext =>
        EventSourcedBehavior[Command, State, Event](???, ???, ???, ???)
      }
    }
    //#factory-shared
  }

  object PerReplica {
    //#factory
    def apply(
        system: ActorSystem[_],
        entityId: String,
        replicaId: ReplicaId): EventSourcedBehavior[Command, State, Event] = {
      ReplicatedEventSourcing.perReplicaJournalConfig(
        ReplicationId("MyReplicatedEntity", entityId, replicaId),
        Map(DCA -> "journalForDCA", DCB -> "journalForDCB")) { replicationContext =>
        EventSourcedBehavior[Command, State, Event](???, ???, ???, ???)
      }
    }

    //#factory
  }

}
