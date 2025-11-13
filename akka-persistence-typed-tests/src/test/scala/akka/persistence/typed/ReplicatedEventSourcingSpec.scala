/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior, ReplicatedEventSourcing, ReplicationContext }
import akka.serialization.jackson.CborSerializable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetSeenSequenceNr
import akka.persistence.typed.internal.EventSourcedBehaviorImpl.GetVersion
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.VersionVector
import akka.persistence.typed.scaladsl.EventWithMetadata

object ReplicatedEventSourcingSpec {

  val AllReplicas = Set(ReplicaId("R1"), ReplicaId("R2"), ReplicaId("R3"))

  sealed trait Command
  final case class GetState(replyTo: ActorRef[State]) extends Command
  final case class StoreMe(description: String, replyTo: ActorRef[Done], latch: CountDownLatch = new CountDownLatch(1))
      extends Command
  final case class StoreUs(
      descriptions: List[String],
      replyTo: ActorRef[Done],
      latch: CountDownLatch = new CountDownLatch(1))
      extends Command
  final case class StoreMeWithMeta(description: String, replyTo: ActorRef[Done], meta: Meta) extends Command
  final case class GetReplica(replyTo: ActorRef[(ReplicaId, Set[ReplicaId])]) extends Command
  final case object Stop extends Command

  final case class State(all: Vector[String]) extends CborSerializable

  final case class Meta(value: String) extends CborSerializable

  final case class EventAndContext(
      event: Any,
      origin: ReplicaId,
      recoveryRunning: Boolean,
      concurrent: Boolean,
      meta: Option[Meta] = None)

  def testBehavior(entityId: String, replicaId: String, probe: ActorRef[EventAndContext]): Behavior[Command] =
    testBehavior(entityId, replicaId, Some(probe))

  def eventSourcedBehavior(
      context: ActorContext[Command],
      replicationContext: ReplicationContext,
      probe: Option[ActorRef[EventAndContext]]): EventSourcedBehavior[Command, String, State] = {
    EventSourcedBehavior[Command, String, State](
      replicationContext.persistenceId,
      State(Vector.empty),
      (state, command) =>
        command match {
          case GetState(replyTo) =>
            replyTo ! state
            Effect.none
          case GetReplica(replyTo) =>
            replyTo.tell((replicationContext.replicaId, replicationContext.allReplicas))
            Effect.none
          case StoreMe(evt, ack, latch) =>
            latch.countDown()
            latch.await(10, TimeUnit.SECONDS)
            Effect.persist(evt).thenRun(_ => ack ! Done)
          case StoreUs(evts, replyTo, latch) =>
            latch.countDown()
            latch.await(10, TimeUnit.SECONDS)
            Effect.persist(evts).thenRun(_ => replyTo ! Done)
          case StoreMeWithMeta(evt, ack, meta) =>
            Effect.persistWithMetadata(EventWithMetadata(evt, meta)).thenRun(_ => ack ! Done)
          case Stop =>
            Effect.stop()
        },
      (state, event) => {
        val meta = EventSourcedBehavior.currentMetadata[Meta](context)
        probe.foreach(
          _ ! EventAndContext(
            event,
            replicationContext.origin,
            replicationContext.recoveryRunning,
            replicationContext.concurrent,
            meta))
        state.copy(all = state.all :+ event)
      })
  }

  def testBehavior(
      entityId: String,
      replicaId: String,
      probe: Option[ActorRef[EventAndContext]] = None,
      allReplicas: Set[ReplicaId] = AllReplicas,
      modifyBehavior: EventSourcedBehavior[Command, String, State] => EventSourcedBehavior[Command, String, State] =
        identity): Behavior[Command] = {
    testBehaviorWithContext(entityId, replicaId, probe, allReplicas, (b, _) => modifyBehavior(b))
  }

  def testBehaviorWithContext(
      entityId: String,
      replicaId: String,
      probe: Option[ActorRef[EventAndContext]] = None,
      allReplicas: Set[ReplicaId] = AllReplicas,
      modifyBehavior: (
          EventSourcedBehavior[Command, String, State],
          ActorContext[Command]) => EventSourcedBehavior[Command, String, State]): Behavior[Command] = {
    Behaviors.setup[Command] { context =>
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("ReplicatedEventSourcingSpec", entityId, ReplicaId(replicaId)),
        allReplicas,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        val behv = eventSourcedBehavior(context, replicationContext, probe)
        modifyBehavior(behv, context)
      }
    }
  }

  def nonReplicatedEventSourcedBehavior(
      persistenceId: PersistenceId,
      probe: Option[ActorRef[String]]): EventSourcedBehavior[Command, String, State] = {
    EventSourcedBehavior[Command, String, State](
      persistenceId,
      State(Vector.empty),
      (state, command) =>
        command match {
          case GetState(replyTo) =>
            replyTo ! state
            Effect.none
          case GetReplica(_) =>
            Effect.unhandled
          case StoreMe(evt, ack, latch) =>
            latch.countDown()
            latch.await(10, TimeUnit.SECONDS)
            Effect.persist(evt).thenRun(_ => ack ! Done)
          case StoreUs(evts, replyTo, latch) =>
            latch.countDown()
            latch.await(10, TimeUnit.SECONDS)
            Effect.persist(evts).thenRun(_ => replyTo ! Done)
          case StoreMeWithMeta(evt, ack, meta) =>
            Effect.persistWithMetadata(EventWithMetadata(evt, meta)).thenRun(_ => ack ! Done)
          case Stop =>
            Effect.stop()
        },
      (state, event) => {
        probe.foreach(_ ! event)
        state.copy(all = state.all :+ event)
      })
  }

  def nonReplictedTestBehavior(entityId: String, probe: Option[ActorRef[String]] = None): Behavior[Command] =
    nonReplicatedEventSourcedBehavior(PersistenceId.of("ReplicatedEventSourcingSpec", entityId), probe)

}

class ReplicatedEventSourcingSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing
    with Eventually {
  import ReplicatedEventSourcingSpec._
  val ids = new AtomicInteger(0)
  def nextEntityId = s"e-${ids.getAndIncrement()}"
  "ReplicatedEventSourcing" should {
    "replicate events between two entities" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val r1 = spawn(testBehavior(entityId, "R1"))
      val r2 = spawn(testBehavior(entityId, "R2"))
      r1 ! StoreMe("from r1", probe.ref)
      r2 ! StoreMe("from r2", probe.ref)
      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
    }
    "get all events in recovery" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val r1 = spawn(testBehavior(entityId, "R1"))
      val r2 = spawn(testBehavior(entityId, "R2"))
      r1 ! StoreMe("from r1", probe.ref)
      r2 ! StoreMe("from r2", probe.ref)
      r1 ! StoreMe("from r1 again", probe.ref)

      val r3 = spawn(testBehavior(entityId, "R3"))
      eventually {
        val probe = createTestProbe[State]()
        r3 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2", "from r1 again")
      }
    }

    "continue after recovery" in {
      val entityId = nextEntityId
      val r1Behavior = testBehavior(entityId, "R1")
      val r2Behavior = testBehavior(entityId, "R2")
      val probe = createTestProbe[Done]()

      {
        // first incarnation
        val r1 = spawn(r1Behavior)
        val r2 = spawn(r2Behavior)
        r1 ! StoreMe("1 from r1", probe.ref)
        r2 ! StoreMe("1 from r2", probe.ref)
        r1 ! Stop
        r2 ! Stop
        probe.expectTerminated(r1)
        probe.expectTerminated(r2)
      }

      {
        // second incarnation
        val r1 = spawn(r1Behavior)
        val r2 = spawn(r2Behavior)

        r1 ! StoreMe("2 from r1", probe.ref)
        r2 ! StoreMe("2 from r2", probe.ref)
        r2 ! StoreMe("3 from r2", probe.ref)

        eventually {
          val probe = createTestProbe[State]()
          r1 ! GetState(probe.ref)
          val expected = Set("1 from r1", "1 from r2", "2 from r1", "2 from r2", "3 from r2")
          probe.expectMessageType[State].all.toSet shouldEqual expected
          r2 ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual expected
        }
      }
    }

    "recover version and seenSeqNrPerReplica" in {
      val entityId = nextEntityId
      val r1Behavior = testBehavior(entityId, "R1")
      val r2Behavior = testBehavior(entityId, "R2")
      val probe = createTestProbe[Done]()

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
        // first incarnation, only persist in R1
        val r1 = spawn(r1Behavior)
        val r2 = spawn(r2Behavior)
        r1 ! StoreMe("1 from r1", probe.ref)
        r1 ! StoreMe("2 from r1", probe.ref)

        eventually {
          val probe = createTestProbe[State]()
          r1 ! GetState(probe.ref)
          val expected = Set("1 from r1", "2 from r1")
          probe.expectMessageType[State].all.toSet shouldEqual expected
          r2 ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual expected
        }

        assertFirst(r1, r2)

        r1 ! Stop
        r2 ! Stop
        probe.expectTerminated(r1)
        probe.expectTerminated(r2)
      }

      def assertSecond(r1: ActorRef[Command], r2: ActorRef[Command]): Unit = {
        val seenSeqNrProbe = createTestProbe[Long]()
        r1.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R1"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 0L // not tracking self
        r1.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R2"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 5L

        r2.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R1"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 2L
        r2.unsafeUpcast[Any].tell(GetSeenSequenceNr(ReplicaId("R2"), seenSeqNrProbe.ref))
        seenSeqNrProbe.receiveMessage() shouldEqual 0L // not tracking self

        val versionProbe = createTestProbe[VersionVector]()
        r1.unsafeUpcast[Any].tell(GetVersion(versionProbe.ref))
        val v1 = versionProbe.receiveMessage()
        v1.versionAt("R1") shouldEqual 2L
        v1.versionAt("R2") shouldEqual 5L

        r2.unsafeUpcast[Any].tell(GetVersion(versionProbe.ref))
        val v2 = versionProbe.receiveMessage()
        v2.versionAt("R1") shouldEqual 2L
        v2.versionAt("R2") shouldEqual 5L
      }

      {
        // second incarnation, only persisting in R2
        val r1 = spawn(r1Behavior)
        val r2 = spawn(r2Behavior)

        // should recover to the same
        assertFirst(r1, r2)

        r2 ! StoreMe("1 from r2", probe.ref)
        r2 ! StoreMe("2 from r2", probe.ref)
        r2 ! StoreMe("3 from r2", probe.ref)

        eventually {
          val probe = createTestProbe[State]()
          r1 ! GetState(probe.ref)
          val expected = Set("1 from r1", "2 from r1", "1 from r2", "2 from r2", "3 from r2")
          probe.expectMessageType[State].all.toSet shouldEqual expected
          r2 ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual expected
        }

        assertSecond(r1, r2)

        r1 ! Stop
        r2 ! Stop
        probe.expectTerminated(r1)
        probe.expectTerminated(r2)
      }

      {
        // third incarnation
        val r1 = spawn(r1Behavior)
        val r2 = spawn(r2Behavior)

        // should recover to the same
        assertSecond(r1, r2)
      }
    }

    "have access to replica information" in {
      val entityId = nextEntityId
      val probe = createTestProbe[(ReplicaId, Set[ReplicaId])]()
      val r1 = spawn(testBehavior(entityId, "R1"))
      r1 ! GetReplica(probe.ref)
      probe.expectMessage((ReplicaId("R1"), Set(ReplicaId("R1"), ReplicaId("R2"), ReplicaId("R3"))))
    }

    "have access to event origin" in {
      val entityId = nextEntityId
      val replyProbe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val eventProbeR2 = createTestProbe[EventAndContext]()

      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2", eventProbeR2.ref))

      r1 ! StoreMe("from r1", replyProbe.ref)
      eventProbeR2.expectMessage(EventAndContext("from r1", ReplicaId("R1"), false, false))
      eventProbeR1.expectMessage(EventAndContext("from r1", ReplicaId("R1"), false, false))

      r2 ! StoreMe("from r2", replyProbe.ref)
      eventProbeR1.expectMessage(EventAndContext("from r2", ReplicaId("R2"), false, false))
      eventProbeR2.expectMessage(EventAndContext("from r2", ReplicaId("R2"), false, false))
    }

    "set recovery running" in {
      val entityId = nextEntityId
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val replyProbe = createTestProbe[Done]()
      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      r1 ! StoreMe("Event", replyProbe.ref)
      eventProbeR1.expectMessage(EventAndContext("Event", ReplicaId("R1"), recoveryRunning = false, false))
      replyProbe.expectMessage(Done)
      r1 ! Stop
      replyProbe.expectTerminated(r1)

      val recoveryProbe = createTestProbe[EventAndContext]()
      spawn(testBehavior(entityId, "R1", recoveryProbe.ref))
      recoveryProbe.expectMessage(EventAndContext("Event", ReplicaId("R1"), recoveryRunning = true, false))
    }

    "persist all" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()

      val latch = new CountDownLatch(3)

      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2"))
      r1 ! StoreUs("1 from r1" :: "2 from r1" :: Nil, probe.ref, latch)
      r2 ! StoreUs("1 from r2" :: "2 from r2" :: Nil, probe.ref, latch)

      // the commands have arrived in both actors, waiting for the latch,
      // so that the persist of the events will be concurrent
      latch.countDown()
      latch.await(10, TimeUnit.SECONDS)
      probe.receiveMessage()
      probe.receiveMessage()

      // events at r2 happened concurrently with events at r1
      eventProbeR1.expectMessage(EventAndContext("1 from r1", ReplicaId("R1"), false, concurrent = false))
      eventProbeR1.expectMessage(EventAndContext("2 from r1", ReplicaId("R1"), false, concurrent = false))
      eventProbeR1.expectMessage(EventAndContext("1 from r2", ReplicaId("R2"), false, concurrent = true))
      eventProbeR1.expectMessage(EventAndContext("2 from r2", ReplicaId("R2"), false, concurrent = true))

      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("1 from r1", "2 from r1", "1 from r2", "2 from r2")
      }
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("1 from r1", "2 from r1", "1 from r2", "2 from r2")
      }
    }

    "replicate alternate events" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val eventProbeR2 = createTestProbe[EventAndContext]()
      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2", eventProbeR2.ref))
      val latch = new CountDownLatch(3)
      r1 ! StoreMe("from r1", probe.ref, latch) // R1 0 R2 0 -> R1 1 R2 0
      r2 ! StoreMe("from r2", probe.ref, latch) // R2 0 R1 0 -> R2 1 R1 0

      // the commands have arrived in both actors, waiting for the latch,
      // so that the persist of the events will be concurrent
      latch.countDown()
      latch.await(10, TimeUnit.SECONDS)
      latch.countDown()

      // each gets its local event
      eventProbeR1.expectMessage(
        EventAndContext("from r1", ReplicaId("R1"), recoveryRunning = false, concurrent = false))
      eventProbeR2.expectMessage(
        EventAndContext("from r2", ReplicaId("R2"), recoveryRunning = false, concurrent = false))

      // then the replicated remote events, which will be concurrent
      eventProbeR1.expectMessage(
        EventAndContext("from r2", ReplicaId("R2"), recoveryRunning = false, concurrent = true))
      eventProbeR2.expectMessage(
        EventAndContext("from r1", ReplicaId("R1"), recoveryRunning = false, concurrent = true))

      // state is updated
      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }

      // Neither of these should be concurrent, nothing happening at r2
      r1 ! StoreMe("from r1 2", probe.ref) // R1 1 R2 1
      eventProbeR1.expectMessage(EventAndContext("from r1 2", ReplicaId("R1"), false, concurrent = false))
      eventProbeR2.expectMessage(EventAndContext("from r1 2", ReplicaId("R1"), false, concurrent = false))
      r1 ! StoreMe("from r1 3", probe.ref) // R2 2 R2 1
      eventProbeR1.expectMessage(EventAndContext("from r1 3", ReplicaId("R1"), false, concurrent = false))
      eventProbeR2.expectMessage(EventAndContext("from r1 3", ReplicaId("R1"), false, concurrent = false))
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2", "from r1 2", "from r1 3")
      }

      // not concurrent as the above asserts mean that all events are fully replicated
      r2 ! StoreMe("from r2 2", probe.ref)
      eventProbeR1.expectMessage(EventAndContext("from r2 2", ReplicaId("R2"), false, concurrent = false))
      eventProbeR2.expectMessage(EventAndContext("from r2 2", ReplicaId("R2"), false, concurrent = false))
      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set(
          "from r1",
          "from r2",
          "from r1 2",
          "from r1 3",
          "from r2 2")
      }
    }

    "receive each event only once" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val eventProbeR2 = createTestProbe[EventAndContext]()
      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2", eventProbeR2.ref))
      r1 ! StoreMe("from r1 1", probe.ref)
      probe.expectMessage(Done)
      r1 ! StoreMe("from r1 2", probe.ref)
      probe.expectMessage(Done)

      // r2, in order because we wrote them both in r1
      eventProbeR2.expectMessage(EventAndContext("from r1 1", ReplicaId("R1"), false, false))
      eventProbeR2.expectMessage(EventAndContext("from r1 2", ReplicaId("R1"), false, false))

      r2 ! StoreMe("from r2 1", probe.ref)
      probe.expectMessage(Done)
      r2 ! StoreMe("from r2 2", probe.ref)
      probe.expectMessage(Done)

      // r3 should only get the events 1, not R2s stored version of them, but we don't know the
      // order they will arrive
      val eventProbeR3 = createTestProbe[EventAndContext]()
      spawn(testBehavior(entityId, "R3", eventProbeR3.ref))
      val eventAndContexts = eventProbeR3.receiveMessages(4).toSet
      eventAndContexts should ===(
        Set(
          EventAndContext("from r1 1", ReplicaId("R1"), false, false),
          EventAndContext("from r1 2", ReplicaId("R1"), false, false),
          EventAndContext("from r2 1", ReplicaId("R2"), false, false),
          EventAndContext("from r2 2", ReplicaId("R2"), false, false)))
      eventProbeR3.expectNoMessage()
    }

    "set concurrent on replay of events" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2"))
      val latch = new CountDownLatch(3)
      r1 ! StoreMe("from r1", probe.ref, latch) // R1 0 R2 0 -> R1 1 R2 0
      r2 ! StoreMe("from r2", probe.ref, latch) // R2 0 R1 0 -> R2 1 R1 0

      // the commands have arrived in both actors, waiting for the latch,
      // so that the persist of the events will be concurrent
      latch.countDown()
      latch.await(10, TimeUnit.SECONDS)

      // local event isn't concurrent, remote event is
      eventProbeR1.expectMessage(
        EventAndContext("from r1", ReplicaId("R1"), recoveryRunning = false, concurrent = false))
      eventProbeR1.expectMessage(
        EventAndContext("from r2", ReplicaId("R2"), recoveryRunning = false, concurrent = true))

      r1 ! Stop
      r2 ! Stop
      probe.expectTerminated(r1)
      probe.expectTerminated(r2)

      // take 2
      val eventProbeR1Take2 = createTestProbe[EventAndContext]()
      spawn(testBehavior(entityId, "R1", eventProbeR1Take2.ref))
      eventProbeR1Take2.expectMessage(
        EventAndContext("from r1", ReplicaId("R1"), recoveryRunning = true, concurrent = false))
      eventProbeR1Take2.expectMessage(
        EventAndContext("from r2", ReplicaId("R2"), recoveryRunning = true, concurrent = true))
    }

    "replicate events between three entities" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      var r1 = spawn(testBehavior(entityId, "R1"))
      var r2 = spawn(testBehavior(entityId, "R2"))
      var r3 = spawn(testBehavior(entityId, "R3"))
      r1 ! StoreMe("1 from r1", probe.ref)
      r2 ! StoreMe("1 from r2", probe.ref)
      r3 ! StoreMe("1 from r3", probe.ref)
      probe.receiveMessages(3) // all writes acked

      (r1 :: r2 :: r3 :: Nil).foreach { replica =>
        eventually {
          val probe = createTestProbe[State]()
          replica ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual Set("1 from r1", "1 from r2", "1 from r3")
          replica ! Stop
          probe.expectTerminated(replica)
        }
      }

      // with all replicas stopped, start and write a bit to one of them
      r1 = spawn(testBehavior(entityId, "R1"))
      r1 ! StoreMe("2 from r1", probe.ref)
      r1 ! StoreMe("3 from r1", probe.ref)
      probe.receiveMessages(2) // both writes acked
      r1 ! Stop
      probe.expectTerminated(r1)

      // start the other two
      r1 = spawn(testBehavior(entityId, "R1"))
      r2 = spawn(testBehavior(entityId, "R2"))
      r3 = spawn(testBehavior(entityId, "R3"))

      (r1 :: r2 :: r3 :: Nil).foreach { replica =>
        eventually {
          val probe = createTestProbe[State]()
          replica ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual Set(
            "1 from r1",
            "2 from r1",
            "3 from r1",
            "1 from r2",
            "1 from r3")
          replica ! Stop
          probe.expectTerminated(replica)
        }
      }

    }

    "restart replication stream" in {
      val testkit = PersistenceTestKit(system)
      val entityId = nextEntityId
      val stateProbe = createTestProbe[State]()
      val probe = createTestProbe[Done]()
      val eventProbeR1 = createTestProbe[EventAndContext]()
      val r1 = spawn(testBehavior(entityId, "R1", eventProbeR1.ref))
      val r2 = spawn(testBehavior(entityId, "R2"))

      // ensure recovery is complete
      r1 ! GetState(stateProbe.ref)
      stateProbe.expectMessage(State(Vector.empty))
      r2 ! GetState(stateProbe.ref)
      stateProbe.expectMessage(State(Vector.empty))

      // make reads fail for the replication
      testkit.failNextNReads(s"$entityId|R2", 1)

      // should restart the replication stream
      r2 ! StoreMe("from r2", probe.ref)
      eventProbeR1.expectMessageType[EventAndContext].event shouldEqual "from r2"
    }

    "migrate from EventSourcedBehavior to ReplicatedEventSourcedBehavior" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      // first ordinary EventSourced
      val es1 = spawn(nonReplictedTestBehavior(entityId))
      es1 ! StoreMe("from es1", probe.ref)
      probe.expectMessage(Done)
      es1 ! Stop

      // then migrate to ReplicatedEventSourced, one replica must keep the same persistenceId
      // as the original EventSourced, i.e. without replicaId
      val allReplicas = Set(ReplicaId.empty, ReplicaId("R2"), ReplicaId("R3"))
      val r1 = spawn(testBehavior(entityId, "", allReplicas = allReplicas))
      val stateProbe = createTestProbe[State]()
      r1 ! GetState(stateProbe.ref)
      stateProbe.expectMessageType[State].all shouldEqual List("from es1")

      r1 ! StoreMe("from r1", probe.ref)
      probe.expectMessage(Done)
      val r2 = spawn(testBehavior(entityId, "R2", allReplicas = allReplicas))
      eventually {
        r2 ! GetState(stateProbe.ref)
        stateProbe.expectMessageType[State].all shouldEqual List("from es1", "from r1")
      }

      r2 ! StoreMe("from r2", probe.ref)
      probe.expectMessage(Done)
      eventually {
        r1 ! GetState(stateProbe.ref)
        stateProbe.expectMessageType[State].all shouldEqual List("from es1", "from r1", "from r2")
      }
    }

    "intercept replicated events between two entities" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val interceptProbe = createTestProbe[Intercepted]()
      val addInterceptor: EventSourcedBehavior[Command, String, State] => EventSourcedBehavior[Command, String, State] =
        _.withReplicatedEventInterceptor { (_, event, origin, seqNr) =>
          interceptProbe.ref ! Intercepted(origin, seqNr, event)
          Future.successful(Done)
        }
      val r1 = spawn(testBehavior(entityId, "R1", modifyBehavior = addInterceptor))
      val r2 = spawn(testBehavior(entityId, "R2", modifyBehavior = addInterceptor))
      r1 ! StoreMe("from r1", probe.ref)
      r2 ! StoreMe("from r2", probe.ref)
      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      val intercepted = interceptProbe.receiveMessages(2)
      intercepted.map(_.event).toSet shouldEqual Set("from r1", "from r2")
    }

    "intercept and delay replicated events between two entities" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val interceptProbe = createTestProbe[Intercepted]()
      implicit val ec: ExecutionContext = system.executionContext
      val addInterceptor: EventSourcedBehavior[Command, String, State] => EventSourcedBehavior[Command, String, State] =
        _.withReplicatedEventInterceptor { (_, event, origin, seqNr) =>
          interceptProbe.ref ! Intercepted(origin, seqNr, event)
          akka.pattern.after(50.millis)(Future { Done })
        }
      val r1 = spawn(testBehavior(entityId, "R1", modifyBehavior = addInterceptor))
      val r2 = spawn(testBehavior(entityId, "R2", modifyBehavior = addInterceptor))
      r1 ! StoreMe("from r1", probe.ref)
      r2 ! StoreMe("from r2", probe.ref)
      eventually {
        val probe = createTestProbe[State]()
        r1 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      eventually {
        val probe = createTestProbe[State]()
        r2 ! GetState(probe.ref)
        probe.expectMessageType[State].all.toSet shouldEqual Set("from r1", "from r2")
      }
      val intercepted = interceptProbe.receiveMessages(2)
      intercepted.map(_.event).toSet shouldEqual Set("from r1", "from r2")
    }

    "fail entity if replicated event interceptor fails" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val r1 = spawn(testBehavior(entityId, "R1", modifyBehavior = _.withReplicatedEventInterceptor { (_, _, _, _) =>
        throw TestException("immediate fail")
      }))
      val r2 = spawn(testBehavior(entityId, "R2"))
      r1 ! StoreMe("from r1", probe.ref)
      r2 ! StoreMe("from r2", probe.ref)
      probe.expectTerminated(r1)
    }

    "transform replicated events between two entities" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbe1 = createTestProbe[EventAndContext]()
      val eventProbe2 = createTestProbe[EventAndContext]()
      val addTransformation: (
          EventSourcedBehavior[Command, String, State],
          ActorContext[Command]) => EventSourcedBehavior[Command, String, State] = { (behv, context) =>
        behv.withReplicatedEventsTransformation { (_, eventWithMeta) =>
          val resMeta1 = EventSourcedBehavior.currentMetadata[ReplicatedEventMetadata](context)
          val resMeta2 = eventWithMeta.metadata[ReplicatedEventMetadata]
          if (resMeta1 != resMeta2)
            throw new IllegalStateException(s"Expected RES metadata to be the same, $resMeta1 != $resMeta2")

          val newMeta = eventWithMeta.metadata[Meta].map(m => m.copy(m.value.toUpperCase)).toList
          EventWithMetadata(eventWithMeta.event.toUpperCase, newMeta) :: Nil
        }
      }
      val r1 = spawn(
        testBehaviorWithContext(entityId, "R1", probe = Some(eventProbe1.ref), modifyBehavior = addTransformation))
      val r2 = spawn(
        testBehaviorWithContext(entityId, "R2", probe = Some(eventProbe2.ref), modifyBehavior = addTransformation))

      r1 ! StoreMeWithMeta("from r1", probe.ref, Meta("meta from r1"))
      eventProbe1.expectMessage(
        EventAndContext(
          "from r1",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          Some(Meta("meta from r1"))))
      // replicated to r2, and transformed
      eventProbe2.expectMessage(
        EventAndContext(
          "FROM R1",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("META FROM R1"))))

      r2 ! StoreMeWithMeta("from r2", probe.ref, Meta("meta from r2"))
      eventProbe2.expectMessage(
        EventAndContext(
          "from r2",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          Some(Meta("meta from r2"))))
      // replicated to r1, and transformed
      eventProbe1.expectMessage(
        EventAndContext(
          "FROM R2",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("META FROM R2"))))

      val stateProbe = createTestProbe[State]()
      r1 ! GetState(stateProbe.ref)
      stateProbe.expectMessage(State(Vector("from r1", "FROM R2")))

      r2 ! GetState(stateProbe.ref)
      stateProbe.expectMessage(State(Vector("FROM R1", "from r2")))
    }

    "transform replicated events and emit additional events" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val eventProbe1 = createTestProbe[EventAndContext]()
      val eventProbe2 = createTestProbe[EventAndContext]()
      val addTransformation: (
          EventSourcedBehavior[Command, String, State],
          ActorContext[Command]) => EventSourcedBehavior[Command, String, State] = { (behv, context) =>
        behv.withReplicatedEventsTransformation { (_, eventWithMeta) =>
          val resMeta1 = EventSourcedBehavior.currentMetadata[ReplicatedEventMetadata](context)
          val resMeta2 = eventWithMeta.metadata[ReplicatedEventMetadata]
          if (resMeta1 != resMeta2)
            throw new IllegalStateException(s"Expected RES metadata to be the same, $resMeta1 != $resMeta2")

          if (eventWithMeta.event.startsWith("transformed")) {
            // break the loop
            eventWithMeta :: Nil
          } else {
            EventWithMetadata("transformed-1: " + eventWithMeta.event.toUpperCase, Meta("meta-1")) ::
            EventWithMetadata("transformed-2: " + eventWithMeta.event.toUpperCase, Meta("meta-2")) ::
            EventWithMetadata("transformed-3: " + eventWithMeta.event.toUpperCase, Meta("meta-3")) ::
            Nil
          }
        }
      }
      val r1 = spawn(
        testBehaviorWithContext(entityId, "R1", probe = Some(eventProbe1.ref), modifyBehavior = addTransformation))
      val r2 = spawn(
        testBehaviorWithContext(entityId, "R2", probe = Some(eventProbe2.ref), modifyBehavior = addTransformation))

      r1 ! StoreMeWithMeta("from r1", probe.ref, Meta("meta from r1"))
      eventProbe1.expectMessage(
        EventAndContext(
          "from r1",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          Some(Meta("meta from r1"))))
      // replicated to r2, and transformed
      eventProbe2.expectMessage(
        EventAndContext(
          "transformed-1: FROM R1",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-1"))))
      eventProbe2.expectMessage(
        EventAndContext(
          "transformed-2: FROM R1",
          ReplicaId("R2"), // this is R2 because it was R2 that emitted it
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-2"))))
      eventProbe2.expectMessage(
        EventAndContext(
          "transformed-3: FROM R1",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-3"))))

      // R2 emitted two additional events, and those are replicated back to R1
      eventProbe1.expectMessage(
        EventAndContext(
          "transformed-2: FROM R1",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-2"))))

      eventProbe1.expectMessage(
        EventAndContext(
          "transformed-3: FROM R1",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-3"))))

      eventProbe1.expectNoMessage()
      eventProbe2.expectNoMessage()

      r2 ! StoreMeWithMeta("from r2", probe.ref, Meta("meta from r2"))
      eventProbe2.expectMessage(
        EventAndContext(
          "from r2",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          Some(Meta("meta from r2"))))
      // replicated to r1, and transformed
      eventProbe1.expectMessage(
        EventAndContext(
          "transformed-1: FROM R2",
          ReplicaId("R2"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-1"))))
      eventProbe1.expectMessage(
        EventAndContext(
          "transformed-2: FROM R2",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-2"))))
      eventProbe1.expectMessage(
        EventAndContext(
          "transformed-3: FROM R2",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-3"))))

      // R1 emitted two additional events, and those are replicated back to R2
      eventProbe2.expectMessage(
        EventAndContext(
          "transformed-2: FROM R2",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-2"))))

      eventProbe2.expectMessage(
        EventAndContext(
          "transformed-3: FROM R2",
          ReplicaId("R1"),
          recoveryRunning = false,
          concurrent = false,
          meta = Some(Meta("meta-3"))))

      eventProbe1.expectNoMessage()
      eventProbe2.expectNoMessage()

    }
  }

}
