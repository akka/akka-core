/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object TestProducerWorkPulling {

  trait Command
  final case class RequestNext(sendTo: ActorRef[TestConsumer.Job]) extends Command
  private case object Tick extends Command

  def apply(
      delay: FiniteDuration,
      producerController: ActorRef[WorkPullingProducerController.Start[TestConsumer.Job]]): Behavior[Command] = {
    Behaviors.setup { context =>
      context.setLoggerName("TestProducerWorkPulling")
      val requestNextAdapter: ActorRef[WorkPullingProducerController.RequestNext[TestConsumer.Job]] =
        context.messageAdapter(req => RequestNext(req.sendNextTo))
      producerController ! WorkPullingProducerController.Start(requestNextAdapter)

      Behaviors.withTimers { timers =>
        timers.startTimerWithFixedDelay(Tick, Tick, delay)
        idle(0)
      }
    }
  }

  private def idle(n: Int): Behavior[Command] = {
    Behaviors.receiveMessagePartial {
      case Tick                => Behaviors.same
      case RequestNext(sendTo) => active(n + 1, sendTo)
    }
  }

  private def active(n: Int, sendTo: ActorRef[TestConsumer.Job]): Behavior[Command] = {
    Behaviors.receivePartial {
      case (ctx, Tick) =>
        val msg = s"msg-$n"
        ctx.log.info("sent {}", msg)
        sendTo ! TestConsumer.Job(msg)
        idle(n)

      case (_, RequestNext(_)) =>
        throw new IllegalStateException("Unexpected RequestNext, already got one.")
    }
  }

}
