/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.ProducerController.MessageWithConfirmation
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors

object Producer {
  trait Command

  case object Run extends Command
  private case class WrappedRequestNext(r: ProducerController.RequestNext[Consumer.Command]) extends Command
  private case object AskReply extends Command

  private implicit val askTimeout: akka.util.Timeout = 5.seconds

  def apply(
      numberOfMessages: Int,
      useAsk: Boolean,
      producerController: ActorRef[ProducerController.Command[Consumer.Command]]): Behavior[Command] = {
    Behaviors.setup { context =>
      val requestNextAdapter =
        context.messageAdapter[ProducerController.RequestNext[Consumer.Command]](WrappedRequestNext(_))

      Behaviors.receiveMessagePartial {
        case WrappedRequestNext(next) =>
          if (next.confirmedSeqNr >= numberOfMessages) {
            context.log.info("Completed {} messages", numberOfMessages)
            Behaviors.stopped
          } else if (useAsk) {
            context.ask[MessageWithConfirmation[Consumer.Command], ProducerController.SeqNr](
              next.askNextTo,
              askReplyTo => MessageWithConfirmation(Consumer.TheMessage, askReplyTo)) {
              case Success(_) => AskReply
              case Failure(e) => throw e
            }
            Behaviors.same
          } else {
            next.sendNextTo ! Consumer.TheMessage
            Behaviors.same
          }

        case Run =>
          context.log.info("Starting {} messages", numberOfMessages)
          producerController ! ProducerController.Start(requestNextAdapter)
          Behaviors.same

        case AskReply =>
          Behaviors.same
      }
    }
  }
}

object Consumer {
  trait Command

  case object TheMessage extends Command

  private case class WrappedDelivery(d: ConsumerController.Delivery[Command]) extends Command

  def serviceKey(testName: String): ServiceKey[ConsumerController.Command[Command]] =
    ServiceKey[ConsumerController.Command[Consumer.Command]](testName)

  def apply(consumerController: ActorRef[ConsumerController.Command[Command]]): Behavior[Command] = {
    Behaviors.setup { context =>
      val traceEnabled = context.log.isTraceEnabled
      val deliveryAdapter =
        context.messageAdapter[ConsumerController.Delivery[Command]](WrappedDelivery(_))
      consumerController ! ConsumerController.Start(deliveryAdapter)

      Behaviors.receiveMessagePartial {
        case WrappedDelivery(d @ ConsumerController.Delivery(_, confirmTo)) =>
          if (traceEnabled)
            context.log.trace("Processed {}", d.seqNr)
          confirmTo ! ConsumerController.Confirmed
          Behaviors.same
      }
    }
  }
}

object WorkPullingProducer {
  trait Command

  case object Run extends Command
  private case class WrappedRequestNext(r: WorkPullingProducerController.RequestNext[Consumer.Command]) extends Command

  def apply(
      numberOfMessages: Int,
      producerController: ActorRef[WorkPullingProducerController.Command[Consumer.Command]]): Behavior[Command] = {
    Behaviors.setup { context =>
      val requestNextAdapter =
        context.messageAdapter[WorkPullingProducerController.RequestNext[Consumer.Command]](WrappedRequestNext(_))
      var remaining = numberOfMessages + context.system.settings.config
          .getInt("akka.reliable-delivery.consumer-controller.flow-control-window")

      Behaviors.receiveMessagePartial {
        case WrappedRequestNext(next) =>
          remaining -= 1
          if (remaining == 0) {
            context.log.info("Completed {} messages", numberOfMessages)
            Behaviors.stopped
          } else {
            next.sendNextTo ! Consumer.TheMessage
            Behaviors.same
          }

        case Run =>
          context.log.info("Starting {} messages", numberOfMessages)
          producerController ! WorkPullingProducerController.Start(requestNextAdapter)
          Behaviors.same
      }
    }
  }
}

object Guardian {

  trait Command
  final case class RunPointToPoint(id: String, numberOfMessages: Int, useAsk: Boolean, replyTo: ActorRef[Done])
      extends Command
  final case class RunWorkPulling(id: String, numberOfMessages: Int, workers: Int, replyTo: ActorRef[Done])
      extends Command
  final case class ProducerTerminated(consumers: List[ActorRef[Consumer.Command]], replyTo: ActorRef[Done])
      extends Command

  def apply(): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case RunPointToPoint(id, numberOfMessages, useAsk, replyTo) =>
          // point-to-point
          val consumerController =
            context.spawn(ConsumerController[Consumer.Command](), s"consumerController-$id")
          val consumers = List(context.spawn(Consumer(consumerController), s"consumer-$id"))

          val producerController = context.spawn(
            ProducerController[Consumer.Command](id, durableQueueBehavior = None),
            s"producerController-$id")
          val producer = context.spawn(Producer(numberOfMessages, useAsk, producerController), s"producer-$id")
          consumerController ! ConsumerController.RegisterToProducerController(producerController)
          context.watchWith(producer, ProducerTerminated(consumers, replyTo))
          producer ! Producer.Run
          Behaviors.same

        case RunWorkPulling(id, numberOfMessages, workers, replyTo) =>
          // workPulling
          val sKey = Consumer.serviceKey(id)
          val consumerController =
            context.spawn(ConsumerController[Consumer.Command](sKey), s"consumerController-$id")
          val consumers = (1 to workers).map { n =>
            context.spawn(Consumer(consumerController), s"consumer-$n-$id")
          }.toList

          val producerController = context.spawn(
            WorkPullingProducerController[Consumer.Command](id, sKey, durableQueueBehavior = None),
            s"producerController-$id")
          val producer = context.spawn(WorkPullingProducer(numberOfMessages, producerController), s"producer-$id")
          context.watchWith(producer, ProducerTerminated(consumers, replyTo))
          producer ! WorkPullingProducer.Run
          Behaviors.same

        case ProducerTerminated(consumers, replyTo) =>
          consumers.foreach(context.stop)
          replyTo ! Done
          Behaviors.same

        case msg =>
          throw new RuntimeException(s"Unexpected message $msg")
      }
    }
  }
}

object ReliableDeliveryBenchmark {
  final val messagesPerOperation = 100000
  final val timeout = 30.seconds
}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS, batchSize = 1)
class ReliableDeliveryBenchmark {
  import ReliableDeliveryBenchmark._

  @Param(Array("50"))
  var window = 0

  implicit var system: ActorSystem[Guardian.Command] = _

  implicit val askTimeout: akka.util.Timeout = akka.util.Timeout(timeout)

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem(
      Guardian(),
      "ReliableDeliveryBenchmark",
      ConfigFactory.parseString(s"""
        akka.loglevel = INFO
        akka.reliable-delivery {
          consumer-controller.flow-control-window = $window
        }
      """))
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(messagesPerOperation)
  def pointToPoint(): Unit = {
    Await.result(
      system.ask(
        Guardian.RunPointToPoint(s"point-to-point-${UUID.randomUUID()}", messagesPerOperation, useAsk = false, _)),
      timeout)
  }

  @Benchmark
  @OperationsPerInvocation(messagesPerOperation)
  def pointToPointAsk(): Unit = {
    Await.result(
      system.ask(
        Guardian.RunPointToPoint(s"point-to-point-${UUID.randomUUID()}", messagesPerOperation, useAsk = true, _)),
      timeout)
  }

  @Benchmark
  @OperationsPerInvocation(messagesPerOperation)
  def workPulling1(): Unit = {
    Await.result(
      system.ask(Guardian.RunWorkPulling(s"work-pulling-${UUID.randomUUID()}", messagesPerOperation, workers = 1, _)),
      timeout)
  }

  @Benchmark
  @OperationsPerInvocation(messagesPerOperation)
  def workPulling2(): Unit = {
    Await.result(
      system.ask(Guardian.RunWorkPulling(s"work-pulling-${UUID.randomUUID()}", messagesPerOperation, workers = 2, _)),
      timeout)
  }

}
