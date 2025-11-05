/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.serialization.jackson.CborSerializable
import akka.stream.scaladsl.Sink

object ReplicatedEventSourcingTaggingSpec {

  val ReplicaId1 = ReplicaId("R1")
  val ReplicaId2 = ReplicaId("R2")
  val AllReplicas = Set(ReplicaId1, ReplicaId2)
  val queryPluginId = PersistenceTestKitReadJournal.Identifier

  object ReplicatedStringSet {

    sealed trait Command
    case class Add(description: String, replyTo: ActorRef[Done]) extends Command
    case class GetStrings(replyTo: ActorRef[Set[String]]) extends Command
    case class State(strings: Set[String]) extends CborSerializable

    def apply(
        entityId: String,
        replica: ReplicaId,
        allReplicas: Set[ReplicaId]): EventSourcedBehavior[Command, String, State] = {
      // #tagging
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("TaggingSpec", entityId, replica),
        allReplicas,
        queryPluginId)(
        replicationContext =>
          EventSourcedBehavior[Command, String, State](
            replicationContext.persistenceId,
            State(Set.empty),
            (state, command) =>
              command match {
                case Add(string, ack) =>
                  if (state.strings.contains(string)) Effect.none.thenRun(_ => ack ! Done)
                  else Effect.persist(string).thenRun(_ => ack ! Done)
                case GetStrings(replyTo) =>
                  replyTo ! state.strings
                  Effect.none
              },
            (state, event) => state.copy(strings = state.strings + event))
          // use withTagger to define tagging logic
            .withTagger(
              event =>
                // don't apply tags if event was replicated here, it already will appear in queries by tag
                // as the origin replica would have tagged it already
                if (replicationContext.origin != replicationContext.replicaId) Set.empty
                else if (event.length > 10) Set("long-strings", "strings")
                else Set("strings")))
      // #tagging
    }
  }

}

class ReplicatedEventSourcingTaggingSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing
    with Eventually {
  import ReplicatedEventSourcingTaggingSpec._
  val ids = new AtomicInteger(0)
  def nextEntityId = s"e-${ids.getAndIncrement()}"
  "ReplicatedEventSourcing" should {
    "allow for tagging of events using the replication context" in {
      val entityId = nextEntityId
      val probe = createTestProbe[Done]()
      val r1 = spawn(ReplicatedStringSet(entityId, ReplicaId1, AllReplicas))
      val r2 = spawn(ReplicatedStringSet(entityId, ReplicaId2, AllReplicas))
      r1 ! ReplicatedStringSet.Add("from r1", probe.ref)
      r2 ! ReplicatedStringSet.Add("from r2", probe.ref)
      probe.receiveMessages(2)
      r1 ! ReplicatedStringSet.Add("a very long string from r1", probe.ref)
      probe.receiveMessages(1)

      val allEvents = Set("from r1", "from r2", "a very long string from r1")
      for (replica <- r1 :: r2 :: Nil) {
        eventually {
          val probe = testKit.createTestProbe[Set[String]]()
          replica ! ReplicatedStringSet.GetStrings(probe.ref)
          probe.receiveMessage() should ===(allEvents)
        }
      }

      val query =
        PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](PersistenceTestKitReadJournal.Identifier)

      val stringTaggedEvents = query.currentEventsByTag("strings", NoOffset).runWith(Sink.seq).futureValue
      stringTaggedEvents.map(_.event).toSet should equal(allEvents)

      val longStrings = query.currentEventsByTag("long-strings", NoOffset).runWith(Sink.seq).futureValue
      longStrings should have size (1)

    }
  }
}
