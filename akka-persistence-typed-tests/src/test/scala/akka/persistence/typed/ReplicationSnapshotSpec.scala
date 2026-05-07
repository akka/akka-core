/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin }
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetSeenSequenceNr
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetVersion
import akka.persistence.typed.internal.{ ReplicatedPublishedEventMetaData, VersionVector }
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing

object ReplicationSnapshotSpec {

  import ReplicatedEventSourcingSpec._

  val EntityType = "SnapshotSpec"

  def behaviorWithSnapshotting(entityId: String, replicaId: ReplicaId): Behavior[Command] =
    behaviorWithSnapshotting(entityId, replicaId, None)

  def behaviorWithSnapshotting(
      entityId: String,
      replicaId: ReplicaId,
      eventProbe: ActorRef[EventAndContext]): Behavior[Command] =
    behaviorWithSnapshotting(entityId, replicaId, Some(eventProbe))

  def behaviorWithSnapshotting(
      entityId: String,
      replicaId: ReplicaId,
      probe: Option[ActorRef[EventAndContext]]): Behavior[Command] = {
    Behaviors.setup[Command] { context =>
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId(EntityType, entityId, replicaId),
        AllReplicas,
        PersistenceTestKitReadJournal.Identifier)(
        replicationContext =>
          eventSourcedBehavior(context, replicationContext, probe).snapshotWhen((_, _, sequenceNr) =>
            sequenceNr % 2 == 0))
    }

  }
}

class ReplicationSnapshotSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(PersistenceTestKitSnapshotPlugin.config))
    with AnyWordSpecLike
    with LogCapturing
    with Eventually {
  import ReplicatedEventSourcingSpec._
  import ReplicationSnapshotSpec._

  val ids = new AtomicInteger(0)
  def nextEntityId = s"e-${ids.getAndIncrement()}"

  val snapshotTestKit = SnapshotTestKit(system)
  val persistenceTestKit = PersistenceTestKit(system)

  val R1 = ReplicaId("R1")
  val R2 = ReplicaId("R2")

  "ReplicatedEventSourcing" should {
    "recover state from snapshots" in {
      val entityId = nextEntityId
      val persistenceIdR1 = s"$EntityType|$entityId|R1"
      val persistenceIdR2 = s"$EntityType|$entityId|R2"
      val probe = createTestProbe[Done]()
      val r2EventProbe = createTestProbe[EventAndContext]()

      {
        val r1 = spawn(behaviorWithSnapshotting(entityId, R1))
        val r2 = spawn(behaviorWithSnapshotting(entityId, R2, r2EventProbe.ref))
        r1 ! StoreMe("r1 1", probe.ref)
        r1 ! StoreMe("r1 2", probe.ref)
        r2EventProbe.expectMessageType[EventAndContext]
        r2EventProbe.expectMessageType[EventAndContext]

        snapshotTestKit.expectNextPersisted(persistenceIdR1, State(Vector("r1 1", "r1 2")))
        snapshotTestKit.expectNextPersisted(persistenceIdR2, State(Vector("r1 1", "r1 2")))

        r2.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
          ReplicationId(EntityType, entityId, R1).persistenceId,
          1L,
          "two-again",
          System.currentTimeMillis(),
          Some(new ReplicatedPublishedEventMetaData(R1, VersionVector.empty, None)),
          None)

        // r2 should now filter out that event if it receives it again
        r2EventProbe.expectNoMessage()
      }

      // restart r2 from a snapshot, the event should still be filtered
      {
        val r2 = spawn(behaviorWithSnapshotting(entityId, R2, r2EventProbe.ref))
        r2.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
          ReplicationId(EntityType, entityId, R1).persistenceId,
          1L,
          "two-again",
          System.currentTimeMillis(),
          Some(new ReplicatedPublishedEventMetaData(R1, VersionVector.empty, None)),
          None)
        r2EventProbe.expectNoMessage()

        val stateProbe = createTestProbe[State]()
        r2 ! GetState(stateProbe.ref)
        stateProbe.expectMessage(State(Vector("r1 1", "r1 2")))
      }
    }

    "recover version and seenSeqNrPerReplica" in {
      val entityId = nextEntityId
      val persistenceIdR1 = s"$EntityType|$entityId|R1"
      val persistenceIdR2 = s"$EntityType|$entityId|R2"
      val probe = createTestProbe[Done]()
      val r2EventProbe = createTestProbe[EventAndContext]()

      def assertFirst(r1: ActorRef[Command], r2: ActorRef[Command]): Unit = {
        val seenSeqNrProbe = createTestProbe[Long]()
        r1.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R1"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 0L // not tracking self
        r1.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R2"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 0L // no events from R2 yet

        r2.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R1"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 2L
        r2.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R2"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 0L // not tracking self

        val versionProbe = createTestProbe[VersionVector]()
        r1.unsafeUpcast[Any].tell(GetVersion(versionProbe.ref))
        val v1 = versionProbe.receiveMessage()
        v1.versionAt("R1") shouldEqual 2L
        v1.versionAt("R2") shouldEqual 0L

        r2.unsafeUpcast[Any].tell(GetVersion(versionProbe.ref))
        val v2 = versionProbe.receiveMessage()
        v2.versionAt("R1") shouldEqual 2L
        v2.versionAt("R2") shouldEqual 0L
      }

      {
        val r1 = spawn(behaviorWithSnapshotting(entityId, R1))
        val r2 = spawn(behaviorWithSnapshotting(entityId, R2, r2EventProbe.ref))
        r1 ! StoreMe("r1 1", probe.ref)
        r1 ! StoreMe("r1 2", probe.ref)
        r2EventProbe.expectMessageType[EventAndContext]
        r2EventProbe.expectMessageType[EventAndContext]

        snapshotTestKit.expectNextPersisted(persistenceIdR1, State(Vector("r1 1", "r1 2")))
        snapshotTestKit.expectNextPersisted(persistenceIdR2, State(Vector("r1 1", "r1 2")))

        assertFirst(r1, r2)

        r1 ! Stop
        r2 ! Stop
        probe.expectTerminated(r1)
        probe.expectTerminated(r2)
      }

      // restart r2 from a snapshot
      {
        val r1 = spawn(behaviorWithSnapshotting(entityId, R1))
        val r2 = spawn(behaviorWithSnapshotting(entityId, R2))

        // should recover to the same
        assertFirst(r1, r2)
      }
    }
  }
}
