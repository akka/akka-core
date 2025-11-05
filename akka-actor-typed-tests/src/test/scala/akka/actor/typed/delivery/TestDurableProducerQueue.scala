/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.testkit.typed.TestException
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

object TestDurableProducerQueue {
  import DurableProducerQueue._
  def apply[A](
      delay: FiniteDuration,
      stateHolder: AtomicReference[State[A]],
      failWhen: Command[A] => Boolean): Behavior[Command[A]] = {
    if (stateHolder.get() eq null)
      stateHolder.set(State(1L, 0L, Map.empty, Vector.empty))

    Behaviors
      .supervise {
        Behaviors.setup[Command[A]] { context =>
          context.setLoggerName("TestDurableProducerQueue")
          val state = stateHolder.get().cleanupPartialChunkedMessages()
          context.log.info("Starting with seqNr [{}], confirmedSeqNr [{}]", state.currentSeqNr, state.confirmedSeqNr)
          new TestDurableProducerQueue[A](context, delay, stateHolder, failWhen).active(state)
        }
      }
      .onFailure(SupervisorStrategy.restartWithBackoff(delay, delay, 0.0))
  }

  def apply[A](delay: FiniteDuration, state: State[A]): Behavior[Command[A]] = {
    apply(delay, new AtomicReference(state), _ => false)
  }

  // using a fixed timestamp to simplify tests, not using the timestamps in the commands
  val TestTimestamp: DurableProducerQueue.TimestampMillis = Long.MaxValue

}

class TestDurableProducerQueue[A](
    context: ActorContext[DurableProducerQueue.Command[A]],
    delay: FiniteDuration,
    stateHolder: AtomicReference[DurableProducerQueue.State[A]],
    failWhen: DurableProducerQueue.Command[A] => Boolean) {
  import DurableProducerQueue._
  import TestDurableProducerQueue.TestTimestamp

  private def active(state: State[A]): Behavior[Command[A]] = {
    stateHolder.set(state)
    Behaviors.receiveMessagePartial {
      case cmd: LoadState[A] @unchecked =>
        maybeFail(cmd)
        if (delay == Duration.Zero) cmd.replyTo ! state else context.scheduleOnce(delay, cmd.replyTo, state)
        Behaviors.same

      case cmd: StoreMessageSent[A] @unchecked =>
        if (cmd.sent.seqNr == state.currentSeqNr) {
          context.log.info(
            "StoreMessageSent seqNr [{}], confirmationQualifier [{}]",
            cmd.sent.seqNr,
            cmd.sent.confirmationQualifier)
          maybeFail(cmd)
          val reply = StoreMessageSentAck(cmd.sent.seqNr)
          if (delay == Duration.Zero) cmd.replyTo ! reply else context.scheduleOnce(delay, cmd.replyTo, reply)
          active(state.addMessageSent(cmd.sent.withTimestampMillis(TestTimestamp)))
        } else if (cmd.sent.seqNr == state.currentSeqNr - 1) {
          // already stored, could be a retry after timeout
          context.log.info("Duplicate seqNr [{}], currentSeqNr [{}]", cmd.sent.seqNr, state.currentSeqNr)
          val reply = StoreMessageSentAck(cmd.sent.seqNr)
          if (delay == Duration.Zero) cmd.replyTo ! reply else context.scheduleOnce(delay, cmd.replyTo, reply)
          Behaviors.same
        } else {
          // may happen after failure
          context.log.info("Ignoring unexpected seqNr [{}], currentSeqNr [{}]", cmd.sent.seqNr, state.currentSeqNr)
          Behaviors.unhandled // no reply, request will timeout
        }

      case cmd: StoreMessageConfirmed[A] @unchecked =>
        context.log.info(
          "StoreMessageConfirmed seqNr [{}], confirmationQualifier [{}]",
          cmd.seqNr,
          cmd.confirmationQualifier)
        maybeFail(cmd)
        active(state.confirmed(cmd.seqNr, cmd.confirmationQualifier, TestTimestamp))
    }
  }

  private def maybeFail(cmd: Command[A]): Unit = {
    if (failWhen(cmd))
      throw TestException(s"TestDurableProducerQueue failed at [$cmd]")
  }

}
