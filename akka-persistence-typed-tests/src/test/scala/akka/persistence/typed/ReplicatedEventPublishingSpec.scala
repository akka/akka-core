/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.internal.{ ReplicatedPublishedEventMetaData, VersionVector }
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.persistence.typed.scaladsl.EventWithMetadata

object ReplicatedEventPublishingSpec {

  val EntityType = "EventPublishingSpec"

  object MyReplicatedBehavior {
    trait Command
    case class Add(text: String, replyTo: ActorRef[Done]) extends Command
    case class Get(replyTo: ActorRef[Set[String]]) extends Command
    case object Stop extends Command

    def apply(
        entityId: String,
        replicaId: ReplicaId,
        allReplicas: Set[ReplicaId],
        modifyBehavior: EventSourcedBehavior[Command, String, Set[String]] => EventSourcedBehavior[
          Command,
          String,
          Set[String]] = identity): Behavior[Command] =
      Behaviors.setup { ctx =>
        ReplicatedEventSourcing.commonJournalConfig(
          ReplicationId(EntityType, entityId, replicaId),
          allReplicas,
          PersistenceTestKitReadJournal.Identifier)(
          replicationContext =>
            modifyBehavior(EventSourcedBehavior[Command, String, Set[String]](
              replicationContext.persistenceId,
              Set.empty,
              (state, command) =>
                command match {
                  case Add(string, replyTo) =>
                    ctx.log.debug("Persisting [{}]", string)
                    Effect.persist(string).thenRun { _ =>
                      ctx.log.debug("Ack:ing [{}]", string)
                      replyTo ! Done
                    }
                  case Get(replyTo) =>
                    replyTo ! state
                    Effect.none
                  case Stop =>
                    Effect.stop()
                  case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
                },
              (state, string) => state + string)))
      }

    def externalReplication(entityId: String, replicaId: ReplicaId, allReplicas: Set[ReplicaId]): Behavior[Command] =
      Behaviors.setup { ctx =>
        ReplicatedEventSourcing.externalReplication(ReplicationId(EntityType, entityId, replicaId), allReplicas)(
          replicationContext =>
            EventSourcedBehavior[Command, String, Set[String]](
              replicationContext.persistenceId,
              Set.empty,
              (state, command) =>
                command match {
                  case Add(string, replyTo) =>
                    ctx.log.debug("Persisting [{}]", string)
                    Effect.persist(string).thenRun { _ =>
                      ctx.log.debug("Ack:ing [{}]", string)
                      replyTo ! Done
                    }
                  case Get(replyTo) =>
                    replyTo ! state
                    Effect.none
                  case Stop =>
                    Effect.stop()
                  case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
                },
              (state, string) => state + string))
      }
  }
}

class ReplicatedEventPublishingSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {

  val DCA = ReplicaId("DC-A")
  val DCB = ReplicaId("DC-B")
  val DCC = ReplicaId("DC-C")

  private var idCounter = 0
  def nextEntityId(): String = {
    idCounter += 1
    s"myId$idCounter"
  }

  import ReplicatedEventPublishingSpec._

  "An Replicated Event Sourced actor" must {
    "move forward when a published event from a replica is received" in {
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
    }

    "reply with an ack for a published event if requested" in {
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior.externalReplication(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()

      val ackProbe = createTestProbe[Done]()
      val persistenceId = ReplicationId(EntityType, id, DCB).persistenceId
      // a published event from another replica
      val publishedEvent = internal.PublishedEventImpl(
        persistenceId,
        1L,
        "one",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        Some(ackProbe.ref))
      actor.asInstanceOf[ActorRef[Any]] ! publishedEvent
      ackProbe.receiveMessage()

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one"))

      // also if we publish it again, we ack since we have seen and persisted it
      // even if we deduplicate and don't write anything
      actor.asInstanceOf[ActorRef[Any]] ! publishedEvent
      ackProbe.receiveMessage()

      // nothing changed
      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one"))
    }

    "ignore a published event from a replica is received over a lossy transport when there is a gap in sequence numbers" in {
      // The event could be valid, but we cannot accept it since the lossy transport
      // means an event with seqnr 1 could have been lost, and writing 2 would mean we lose data
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        2L, // missing 1L
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "three"))
    }

    "accept a published event from a replica is received over a non-lossy transport when there is a gap in sequence numbers" in {
      // scenario:
      // DCB saw a replicated event from DCA first, so already used 1 as seq nr for that
      // then does a write of its own that is now replicating over to DCA - DCA has not seen
      // DCB -> 1 but should still accept the update since the transport is not lossy
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        2L, // missing 1L
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        Some(probe.ref))
      probe.expectMessage(Done)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
    }

    "accept a published event from an unknown replica" in {
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCC).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCC, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
    }

    "ignore an already seen event from a replica" in {
      val id = nextEntityId()
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB)))
      val probe = createTestProbe[Any]()
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, "myId4", DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      // simulate another published event from that replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two-again", // ofc this would be the same in the real world, different just so we can detect
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)

      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
    }

    "handle published events after replay" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      val replicatedBehavior = MyReplicatedBehavior(id, DCA, Set(DCA, DCB))
      val incarnation1 = spawn(replicatedBehavior)
      incarnation1 ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      incarnation1 ! MyReplicatedBehavior.Stop
      probe.expectTerminated(incarnation1)

      val incarnation2 = spawn(replicatedBehavior)

      incarnation2 ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one"))
      // replay completed

      // simulate a published event from another replica
      incarnation2.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)

      incarnation2 ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      incarnation2 ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
    }

    "handle published events before and after replay" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      val replicatedBehaviorA = MyReplicatedBehavior(id, DCA, Set(DCA, DCB))
      val incarnationA1 = spawn(replicatedBehaviorA)
      incarnationA1 ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      incarnationA1.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)

      incarnationA1 ! MyReplicatedBehavior.Stop
      probe.expectTerminated(incarnationA1)

      val incarnationA2 = spawn(replicatedBehaviorA)

      // simulate a published event from another replica
      incarnationA2.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        2L,
        "three",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)

      incarnationA2 ! MyReplicatedBehavior.Add("four", probe.ref)
      probe.expectMessage(Done)

      incarnationA2 ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three", "four"))
    }

    "intercept published replicated events between two entities" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val interceptProbe = createTestProbe[Intercepted]()
      val addInterceptor
          : EventSourcedBehavior[MyReplicatedBehavior.Command, String, Set[String]] => EventSourcedBehavior[
            MyReplicatedBehavior.Command,
            String,
            Set[String]] =
        _.withReplicatedEventInterceptor { (_, event, origin, seqNr) =>
          interceptProbe.ref ! Intercepted(origin, seqNr, event)
          Future.successful(Done)
        }
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB), modifyBehavior = addInterceptor))
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
      interceptProbe.receiveMessage() shouldEqual Intercepted(DCB, 2L, "two")
    }

    "intercept and delay published replicated events between two entities" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val interceptProbe = createTestProbe[Intercepted]()
      implicit val ec: ExecutionContext = system.executionContext
      val addInterceptor
          : EventSourcedBehavior[MyReplicatedBehavior.Command, String, Set[String]] => EventSourcedBehavior[
            MyReplicatedBehavior.Command,
            String,
            Set[String]] =
        _.withReplicatedEventInterceptor { (_, event, origin, seqNr) =>
          interceptProbe.ref ! Intercepted(origin, seqNr, event)
          akka.pattern.after(50.millis)(Future { Done })
        }
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB), modifyBehavior = addInterceptor))
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "two", "three"))
      interceptProbe.receiveMessage() shouldEqual Intercepted(DCB, 2L, "two")
    }

    "fail entity if replicated event interceptor fails" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      val actor = spawn(
        MyReplicatedBehavior(
          id,
          DCA,
          Set(DCA, DCB),
          modifyBehavior =
            _.withReplicatedEventInterceptor((_, _, _, _) => Future.failed(throw TestException("immediate fail")))))
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      probe.expectTerminated(actor)
    }

    "transform replicated events between two entities" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val addTransformation
          : EventSourcedBehavior[MyReplicatedBehavior.Command, String, Set[String]] => EventSourcedBehavior[
            MyReplicatedBehavior.Command,
            String,
            Set[String]] =
        _.withReplicatedEventsTransformation { (_, eventWithMeta) =>
          EventWithMetadata(eventWithMeta.event.toUpperCase, Nil) :: Nil
        }
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB), modifyBehavior = addTransformation))
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "TWO", "three"))
    }

    "transform replicated events and emit additional events" in {
      val id = nextEntityId()
      val probe = createTestProbe[Any]()
      case class Intercepted(origin: ReplicaId, seqNr: Long, event: String)
      val addTransformation
          : EventSourcedBehavior[MyReplicatedBehavior.Command, String, Set[String]] => EventSourcedBehavior[
            MyReplicatedBehavior.Command,
            String,
            Set[String]] =
        _.withReplicatedEventsTransformation { (_, eventWithMeta) =>
          EventWithMetadata(eventWithMeta.event.toUpperCase + "-1", Nil) ::
          EventWithMetadata(eventWithMeta.event.toUpperCase + "-2", Nil) ::
          EventWithMetadata(eventWithMeta.event.toUpperCase + "-3", Nil) ::
          Nil
        }
      val actor = spawn(MyReplicatedBehavior(id, DCA, Set(DCA, DCB), modifyBehavior = addTransformation))
      actor ! MyReplicatedBehavior.Add("one", probe.ref)
      probe.expectMessage(Done)

      // simulate a published event from another replica
      actor.asInstanceOf[ActorRef[Any]] ! internal.PublishedEventImpl(
        ReplicationId(EntityType, id, DCB).persistenceId,
        1L,
        "two",
        System.currentTimeMillis(),
        Some(new ReplicatedPublishedEventMetaData(DCB, VersionVector.empty, None)),
        None)
      actor ! MyReplicatedBehavior.Add("three", probe.ref)
      probe.expectMessage(Done)

      actor ! MyReplicatedBehavior.Get(probe.ref)
      probe.expectMessage(Set("one", "TWO-1", "TWO-2", "TWO-3", "three"))
    }

  }

}
