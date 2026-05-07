/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

object ChangeEventSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))

  sealed trait Command
  final case class Update(value: String, replyTo: ActorRef[Done]) extends Command
  final case class Delete(replyTo: ActorRef[Done]) extends Command

  def behaviorWithChangeEvent(persistenceId: PersistenceId, probe: ActorRef[String]): Behavior[Command] = {
    val changeEventHandler = ChangeEventHandler[Command, String, String](updateHandler = {
      (previousState, newState, _) =>
        val chgEvent = newState.replace(previousState, "")
        probe ! s"update: $previousState, $newState => $chgEvent"
        chgEvent
    }, deleteHandler = { (previousState, _) =>
      val chgEvent = "DEL"
      probe ! s"delete: $previousState => $chgEvent"
      chgEvent
    })

    DurableStateBehavior[Command, String](
      persistenceId,
      emptyState = "",
      commandHandler = (state, command) => {
        command match {
          case Update(value, replyTo) =>
            Effect.persist(state + value).thenReply(replyTo)(_ => Done)
          case Delete(replyTo) =>
            Effect.delete().thenReply(replyTo)((_: String) => Done)

        }
      }).withChangeEventHandler(changeEventHandler)
  }
}

class ChangeEventSpec extends ScalaTestWithActorTestKit(ChangeEventSpec.conf) with AnyWordSpecLike with LogCapturing {
  import ChangeEventSpec._

  "DurableStateBehavior with change event" must {
    "call the change event handler" in {
      val changeHandlerProbe = TestProbe[String]()
      val b = behaviorWithChangeEvent(PersistenceId("Test", "p1"), changeHandlerProbe.ref)
      val ref = spawn(b)
      ref ! Update("a", system.ignoreRef)
      changeHandlerProbe.expectMessage("update: , a => a")
      ref ! Update("b", system.ignoreRef)
      changeHandlerProbe.expectMessage("update: a, ab => b")
      ref ! Update("c", system.ignoreRef)
      changeHandlerProbe.expectMessage("update: ab, abc => c")
    }

    "call the delete change event handler" in {
      val probe = TestProbe[String]()
      val b = behaviorWithChangeEvent(PersistenceId("Test", "p2"), probe.ref)
      val ref = spawn(b)
      ref ! Update("a", system.ignoreRef)
      probe.expectMessage("update: , a => a")
      ref ! Update("b", system.ignoreRef)
      probe.expectMessage("update: a, ab => b")
      ref ! Delete(system.ignoreRef)
      probe.expectMessage("delete: ab => DEL")
    }

    "persist change event and retrieve with currentEventsBySlices" in {
      val replyProbe = TestProbe[Done]()
      val probe = TestProbe[String]()
      val pid = PersistenceId("Test", "p3")

      val b = behaviorWithChangeEvent(pid, probe.ref)
      val ref = spawn(b)
      ref ! Update("a", system.ignoreRef)
      ref ! Update("b", system.ignoreRef)
      ref ! Update("c", system.ignoreRef)
      ref ! Delete(replyProbe.ref)
      replyProbe.expectMessage(Done)

      val query = DurableStateStoreRegistry(system)
        .durableStateStoreFor(PersistenceTestKitDurableStateStorePlugin.PluginId)
        .asInstanceOf[CurrentEventsBySliceQuery]
      val sliceRange = query.sliceRanges(1).head
      val currentEventsBySlices =
        query
          .currentEventsBySlices[String](pid.entityTypeHint, sliceRange.min, sliceRange.max, Offset.noOffset)
          .filter(_.persistenceId == pid.id)
          .runWith(Sink.seq)
          .futureValue

      currentEventsBySlices.map(_.event) shouldBe Vector("a", "b", "c", "DEL")
    }

    "persist change event and retrieve with eventsBySlices" in {
      val replyProbe = TestProbe[Done]()
      val probe = TestProbe[String]()
      val pid = PersistenceId("Test", "p4")

      val query = DurableStateStoreRegistry(system)
        .durableStateStoreFor(PersistenceTestKitDurableStateStorePlugin.PluginId)
        .asInstanceOf[EventsBySliceQuery]
      val sliceRange = query.sliceRanges(1).head
      val eventsBySlices =
        query
          .eventsBySlices[String](pid.entityTypeHint, sliceRange.min, sliceRange.max, Offset.noOffset)
          .filter(_.persistenceId == pid.id)
          .runWith(TestSink[EventEnvelope[String]]())
      eventsBySlices.request(100)

      val b = behaviorWithChangeEvent(pid, probe.ref)
      val ref = spawn(b)
      ref ! Update("a", system.ignoreRef)
      ref ! Update("b", system.ignoreRef)
      ref ! Update("c", replyProbe.ref)
      replyProbe.expectMessage(Done)

      eventsBySlices.expectNext().event shouldBe "a"
      eventsBySlices.expectNext().event shouldBe "b"
      eventsBySlices.expectNext().event shouldBe "c"

      ref ! Update("d", system.ignoreRef)
      ref ! Delete(replyProbe.ref)
      eventsBySlices.expectNext().event shouldBe "d"
      eventsBySlices.expectNext().event shouldBe "DEL"
    }
  }
}
