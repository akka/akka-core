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
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior, ReplicatedEventSourcing, ReplicationContext }
import akka.serialization.jackson.CborSerializable

object ReplicatedEventSourcingSpec {

  val AllReplicas = Set(ReplicaId("R1"), ReplicaId("R2"), ReplicaId("R3"))

  sealed trait Command
  case class GetState(replyTo: ActorRef[State]) extends Command
  case class StoreMe(description: String, replyTo: ActorRef[Done], latch: CountDownLatch = new CountDownLatch(1))
      extends Command
  case class StoreUs(descriptions: List[String], replyTo: ActorRef[Done], latch: CountDownLatch = new CountDownLatch(1))
      extends Command
  case class GetReplica(replyTo: ActorRef[(ReplicaId, Set[ReplicaId])]) extends Command
  case object Stop extends Command

  case class State(all: List[String]) extends CborSerializable

  def testBehavior(entityId: String, replicaId: String, probe: ActorRef[EventAndContext]): Behavior[Command] =
    testBehavior(entityId, replicaId, Some(probe))

  def eventSourcedBehavior(
      replicationContext: ReplicationContext,
      probe: Option[ActorRef[EventAndContext]]): EventSourcedBehavior[Command, String, State] = {
    EventSourcedBehavior[Command, String, State](
      replicationContext.persistenceId,
      State(Nil),
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
          case Stop =>
            Effect.stop()
        },
      (state, event) => {
        probe.foreach(
          _ ! EventAndContext(
            event,
            replicationContext.origin,
            replicationContext.recoveryRunning,
            replicationContext.concurrent))
        state.copy(all = event :: state.all)
      })
  }

  def testBehavior(
      entityId: String,
      replicaId: String,
      probe: Option[ActorRef[EventAndContext]] = None): Behavior[Command] =
    ReplicatedEventSourcing.commonJournalConfig(
      ReplicationId("ReplicatedEventSourcingSpec", entityId, ReplicaId(replicaId)),
      AllReplicas,
      PersistenceTestKitReadJournal.Identifier)(replicationContext => eventSourcedBehavior(replicationContext, probe))

}

case class EventAndContext(event: Any, origin: ReplicaId, recoveryRunning: Boolean, concurrent: Boolean)

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

        eventually {
          val probe = createTestProbe[State]()
          r1 ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual Set("1 from r1", "1 from r2", "2 from r1", "2 from r2")
          r2 ! GetState(probe.ref)
          probe.expectMessageType[State].all.toSet shouldEqual Set("1 from r1", "1 from r2", "2 from r1", "2 from r2")
        }
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
      stateProbe.expectMessage(State(Nil))
      r2 ! GetState(stateProbe.ref)
      stateProbe.expectMessage(State(Nil))

      // make reads fail for the replication
      testkit.failNextNReads(s"$entityId|R2", 1)

      // should restart the replication stream
      r2 ! StoreMe("from r2", probe.ref)
      eventProbeR1.expectMessageType[EventAndContext].event shouldEqual "from r2"
    }
  }
}
