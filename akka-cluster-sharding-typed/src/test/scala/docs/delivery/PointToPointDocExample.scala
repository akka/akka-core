/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.delivery

import java.util.UUID

import akka.actor.typed.ActorSystem

//#imports
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.scaladsl.Behaviors

//#imports

object PointToPointDocExample {

  //#producer
  object FibonacciProducer {
    sealed trait Command

    private case class WrappedRequestNext(r: ProducerController.RequestNext[FibonacciConsumer.Command]) extends Command

    def apply(
        producerController: ActorRef[ProducerController.Command[FibonacciConsumer.Command]]): Behavior[Command] = {
      Behaviors.setup { context =>
        val requestNextAdapter =
          context.messageAdapter[ProducerController.RequestNext[FibonacciConsumer.Command]](WrappedRequestNext(_))
        producerController ! ProducerController.Start(requestNextAdapter)

        fibonacci(0, 1, 0)
      }
    }

    private def fibonacci(n: Long, b: BigInt, a: BigInt): Behavior[Command] = {
      Behaviors.receive {
        case (context, WrappedRequestNext(next)) =>
          context.log.info("Generated fibonacci {}: {}", n, a)
          next.sendNextTo ! FibonacciConsumer.FibonacciNumber(n, a)

          if (n == 1000)
            Behaviors.stopped
          else
            fibonacci(n + 1, a + b, b)
      }
    }
  }
  //#producer

  //#consumer
  import akka.actor.typed.delivery.ConsumerController

  object FibonacciConsumer {
    sealed trait Command

    final case class FibonacciNumber(n: Long, value: BigInt) extends Command

    private case class WrappedDelivery(d: ConsumerController.Delivery[Command]) extends Command

    def apply(
        consumerController: ActorRef[ConsumerController.Command[FibonacciConsumer.Command]]): Behavior[Command] = {
      Behaviors.setup { context =>
        val deliveryAdapter =
          context.messageAdapter[ConsumerController.Delivery[FibonacciConsumer.Command]](WrappedDelivery(_))
        consumerController ! ConsumerController.Start(deliveryAdapter)

        Behaviors.receiveMessagePartial {
          case WrappedDelivery(ConsumerController.Delivery(FibonacciNumber(n, value), confirmTo)) =>
            context.log.info("Processed fibonacci {}: {}", n, value)
            confirmTo ! ConsumerController.Confirmed
            Behaviors.same
        }
      }
    }
  }
  //#consumer

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        //#connect
        val consumerController = context.spawn(ConsumerController[FibonacciConsumer.Command](), "consumerController")
        context.spawn(FibonacciConsumer(consumerController), "consumer")

        val producerId = s"fibonacci-${UUID.randomUUID()}"
        val producerController = context.spawn(
          ProducerController[FibonacciConsumer.Command](producerId, durableQueueBehavior = None),
          "producerController")
        context.spawn(FibonacciProducer(producerController), "producer")

        consumerController ! ConsumerController.RegisterToProducerController(producerController)
        //#connect

        Behaviors.empty
      }
    }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Guardian(), "FibonacciExample")
  }
}
