/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId
import akka.serialization.jackson.CborSerializable

object DurableStateBehaviorSpec {

  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))

  sealed trait Command extends CborSerializable
  final case class IncrementWithConfirmation(replyTo: ActorRef[Done]) extends Command
  final case class GetValue(replyTo: ActorRef[State]) extends Command
  final case class DeleteWithConfirmation(replyTo: ActorRef[Done]) extends Command
  case class IncrementBy(by: Int) extends Command

  final case class State(value: Int) extends CborSerializable

  def counter(persistenceId: PersistenceId): DurableStateBehavior[Command, State] = {
    DurableStateBehavior(
      persistenceId,
      emptyState = State(0),
      commandHandler = (state, command) =>
        command match {

          case IncrementBy(by) =>
            Effect.persist(state.copy(value = state.value + by))

          case IncrementWithConfirmation(replyTo) =>
            Effect.persist(state.copy(value = state.value + 1)).thenRun(_ => replyTo ! Done)

          case GetValue(replyTo) =>
            replyTo ! state
            Effect.none

          case DeleteWithConfirmation(replyTo) =>
            Effect.delete[State]().thenRun(_ => replyTo ! Done)
        })
  }
}

class DurableStateBehaviorSpec
    extends ScalaTestWithActorTestKit(NullEmptyStateSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {
  import DurableStateBehaviorSpec._

  implicit val testSettings: TestKitSettings = TestKitSettings(system)

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()})")

  "A DurableStateBehavior actor" must {
    "persist and update state" in {
      val c = spawn(counter(nextPid()))
      val updateProbe = TestProbe[Done]()
      val queryProbe = TestProbe[State]()

      c ! IncrementWithConfirmation(updateProbe.ref)
      updateProbe.expectMessage(Done)

      c ! GetValue(queryProbe.ref)
      queryProbe.expectMessage(State(1))

      c ! IncrementBy(5)
      c ! IncrementWithConfirmation(updateProbe.ref)
      updateProbe.expectMessage(Done)

      c ! GetValue(queryProbe.ref)
      queryProbe.expectMessage(State(7))
    }

    "delete state" in {
      val c = spawn(counter(nextPid()))
      val updateProbe = TestProbe[Done]()
      c ! IncrementWithConfirmation(updateProbe.ref)
      updateProbe.expectMessage(Done)

      val deleteProbe = TestProbe[Done]()
      c ! DeleteWithConfirmation(deleteProbe.ref)
      deleteProbe.expectMessage(Done)

      val queryProbe = TestProbe[State]()
      c ! GetValue(queryProbe.ref)
      queryProbe.expectMessage(State(0))
    }

    "handle commands sequentially" in {
      val c = spawn(counter(nextPid()))
      val probe = TestProbe[Any]()

      c ! IncrementWithConfirmation(probe.ref)
      c ! IncrementWithConfirmation(probe.ref)
      c ! IncrementWithConfirmation(probe.ref)
      c ! GetValue(probe.ref)
      probe.expectMessage(Done)
      probe.expectMessage(Done)
      probe.expectMessage(Done)
      probe.expectMessage(State(3))

      c ! DeleteWithConfirmation(probe.ref)
      c ! GetValue(probe.ref)
      probe.expectMessage(Done)
      probe.expectMessage(State(0))
    }
  }
}
