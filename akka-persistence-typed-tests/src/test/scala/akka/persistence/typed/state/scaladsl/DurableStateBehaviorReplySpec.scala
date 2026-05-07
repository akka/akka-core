/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.persistence.typed.PersistenceId
import akka.serialization.jackson.CborSerializable

object DurableStateBehaviorReplySpec {
  def conf: Config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = INFO
    """))

  sealed trait Command[ReplyMessage] extends CborSerializable
  final case class IncrementWithConfirmation(replyTo: ActorRef[Done]) extends Command[Done]
  final case class IncrementReplyLater(replyTo: ActorRef[Done]) extends Command[Done]
  final case class ReplyNow(replyTo: ActorRef[Done]) extends Command[Done]
  final case class GetValue(replyTo: ActorRef[State]) extends Command[State]
  final case class DeleteWithConfirmation(replyTo: ActorRef[Done]) extends Command[Done]
  case object Increment extends Command[Nothing]
  case class IncrementBy(by: Int) extends Command[Nothing]

  final case class State(value: Int) extends CborSerializable

  def counter(persistenceId: PersistenceId): Behavior[Command[_]] =
    Behaviors.setup(ctx => counter(ctx, persistenceId))

  def counter(ctx: ActorContext[Command[_]], persistenceId: PersistenceId): DurableStateBehavior[Command[_], State] = {
    DurableStateBehavior.withEnforcedReplies[Command[_], State](
      persistenceId,
      emptyState = State(0),
      commandHandler = (state, command) =>
        command match {

          case IncrementWithConfirmation(replyTo) =>
            Effect.persist(state.copy(value = state.value + 1)).thenReply(replyTo)(_ => Done)

          case IncrementReplyLater(replyTo) =>
            Effect
              .persist(state.copy(value = state.value + 1))
              .thenRun((_: State) => ctx.self ! ReplyNow(replyTo))
              .thenNoReply()

          case ReplyNow(replyTo) =>
            Effect.reply(replyTo)(Done)

          case GetValue(replyTo) =>
            Effect.reply(replyTo)(state)

          case DeleteWithConfirmation(replyTo) =>
            Effect.delete[State]().thenReply(replyTo)(_ => Done)

          case _ => ???

        })
  }

}

class DurableStateBehaviorReplySpec
    extends ScalaTestWithActorTestKit(DurableStateBehaviorReplySpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  import DurableStateBehaviorReplySpec._

  val pidCounter = new AtomicInteger(0)
  private def nextPid(): PersistenceId = PersistenceId.ofUniqueId(s"c${pidCounter.incrementAndGet()})")

  "A DurableStateBehavior actor with commands that are expecting replies" must {

    "persist state thenReply" in {
      val c = spawn(counter(nextPid()))
      val probe = TestProbe[Done]()
      c ! IncrementWithConfirmation(probe.ref)
      probe.expectMessage(Done)

      c ! IncrementWithConfirmation(probe.ref)
      c ! IncrementWithConfirmation(probe.ref)
      probe.expectMessage(Done)
      probe.expectMessage(Done)
    }

    "persist state thenReply later" in {
      val c = spawn(counter(nextPid()))
      val probe = TestProbe[Done]()
      c ! IncrementReplyLater(probe.ref)
      probe.expectMessage(Done)
    }

    "reply to query command" in {
      val c = spawn(counter(nextPid()))
      val updateProbe = TestProbe[Done]()
      c ! IncrementWithConfirmation(updateProbe.ref)

      val queryProbe = TestProbe[State]()
      c ! GetValue(queryProbe.ref)
      queryProbe.expectMessage(State(1))
    }

    "delete state thenReply" in {
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

      c ! IncrementWithConfirmation(probe.ref)
      c ! IncrementWithConfirmation(probe.ref)
      c ! DeleteWithConfirmation(probe.ref)
      c ! GetValue(probe.ref)
      probe.expectMessage(Done)
      probe.expectMessage(Done)
      probe.expectMessage(Done)
      probe.expectMessage(State(0))
    }
  }
}
