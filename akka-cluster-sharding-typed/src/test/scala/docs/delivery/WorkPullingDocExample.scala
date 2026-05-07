/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.delivery

import java.util.UUID

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorRef
import scala.annotation.nowarn

@nowarn("msg=never used")
object WorkPullingDocExample {

  //#imports
  import akka.actor.typed.scaladsl.Behaviors
  import akka.actor.typed.Behavior
  //#imports

  //#consumer
  import akka.actor.typed.delivery.ConsumerController
  import akka.actor.typed.receptionist.ServiceKey

  object ImageConverter {
    sealed trait Command
    final case class ConversionJob(resultId: UUID, fromFormat: String, toFormat: String, image: Array[Byte])
    private case class WrappedDelivery(d: ConsumerController.Delivery[ConversionJob]) extends Command

    val serviceKey = ServiceKey[ConsumerController.Command[ConversionJob]]("ImageConverter")

    def apply(): Behavior[Command] = {
      Behaviors.setup { context =>
        val deliveryAdapter =
          context.messageAdapter[ConsumerController.Delivery[ConversionJob]](WrappedDelivery(_))
        val consumerController =
          context.spawn(ConsumerController(serviceKey), "consumerController")
        consumerController ! ConsumerController.Start(deliveryAdapter)

        Behaviors.receiveMessage {
          case WrappedDelivery(delivery) =>
            val image = delivery.message.image
            val fromFormat = delivery.message.fromFormat
            val toFormat = delivery.message.toFormat
            // convert image...
            // store result with resultId key for later retrieval

            // and when completed confirm
            delivery.confirmTo ! ConsumerController.Confirmed

            Behaviors.same
        }

      }
    }

  }
  //#consumer

  //#producer
  import akka.actor.typed.delivery.WorkPullingProducerController
  import akka.actor.typed.scaladsl.ActorContext
  import akka.actor.typed.scaladsl.StashBuffer

  object ImageWorkManager {
    sealed trait Command
    final case class Convert(fromFormat: String, toFormat: String, image: Array[Byte]) extends Command
    private case class WrappedRequestNext(r: WorkPullingProducerController.RequestNext[ImageConverter.ConversionJob])
        extends Command

    final case class GetResult(resultId: UUID, replyTo: ActorRef[Option[Array[Byte]]]) extends Command

    //#producer

    //#ask
    final case class ConvertRequest(
        fromFormat: String,
        toFormat: String,
        image: Array[Byte],
        replyTo: ActorRef[ConvertResponse])
        extends Command

    sealed trait ConvertResponse
    final case class ConvertAccepted(resultId: UUID) extends ConvertResponse
    case object ConvertRejected extends ConvertResponse
    final case class ConvertTimedOut(resultId: UUID) extends ConvertResponse

    private final case class AskReply(resultId: UUID, originalReplyTo: ActorRef[ConvertResponse], timeout: Boolean)
        extends Command
    //#ask

    //#producer
    def apply(): Behavior[Command] = {
      Behaviors.setup { context =>
        val requestNextAdapter =
          context.messageAdapter[WorkPullingProducerController.RequestNext[ImageConverter.ConversionJob]](
            WrappedRequestNext(_))
        val producerController = context.spawn(
          WorkPullingProducerController(
            producerId = "workManager",
            workerServiceKey = ImageConverter.serviceKey,
            durableQueueBehavior = None),
          "producerController")
        //#producer
        //#durable-queue
        import akka.persistence.typed.delivery.EventSourcedProducerQueue
        import akka.persistence.typed.PersistenceId

        val durableQueue =
          EventSourcedProducerQueue[ImageConverter.ConversionJob](PersistenceId.ofUniqueId("ImageWorkManager"))
        val durableProducerController = context.spawn(
          WorkPullingProducerController(
            producerId = "workManager",
            workerServiceKey = ImageConverter.serviceKey,
            durableQueueBehavior = Some(durableQueue)),
          "producerController")
        //#durable-queue
        //#producer
        producerController ! WorkPullingProducerController.Start(requestNextAdapter)

        Behaviors.withStash(1000) { stashBuffer =>
          new ImageWorkManager(context, stashBuffer).waitForNext()
        }
      }
    }

  }

  //#ask

  final class ImageWorkManager(
      context: ActorContext[ImageWorkManager.Command],
      stashBuffer: StashBuffer[ImageWorkManager.Command]) {
    //#ask
    import ImageWorkManager._

    private def waitForNext(): Behavior[Command] = {
      Behaviors.receiveMessagePartial {
        case WrappedRequestNext(next) =>
          stashBuffer.unstashAll(active(next))
        case c: Convert =>
          if (stashBuffer.isFull) {
            context.log.warn("Too many Convert requests.")
            Behaviors.same
          } else {
            stashBuffer.stash(c)
            Behaviors.same
          }
        case GetResult(resultId, replyTo) =>
          // TODO retrieve the stored result and reply
          Behaviors.same
      }
    }

    private def active(
        next: WorkPullingProducerController.RequestNext[ImageConverter.ConversionJob]): Behavior[Command] = {
      Behaviors.receiveMessagePartial {
        case Convert(from, to, image) =>
          val resultId = UUID.randomUUID()
          next.sendNextTo ! ImageConverter.ConversionJob(resultId, from, to, image)
          waitForNext()
        case GetResult(resultId, replyTo) =>
          // TODO retrieve the stored result and reply
          Behaviors.same
        case _: WrappedRequestNext =>
          throw new IllegalStateException("Unexpected RequestNext")
      }
    }
    //#producer
    object askScope {
      //#ask

      import WorkPullingProducerController.MessageWithConfirmation
      import akka.util.Timeout

      implicit val askTimeout: Timeout = 5.seconds

      private def waitForNext(): Behavior[Command] = {
        Behaviors.receiveMessagePartial {
          case WrappedRequestNext(next) =>
            stashBuffer.unstashAll(active(next))
          case c: ConvertRequest =>
            if (stashBuffer.isFull) {
              c.replyTo ! ConvertRejected
              Behaviors.same
            } else {
              stashBuffer.stash(c)
              Behaviors.same
            }
          case AskReply(resultId, originalReplyTo, timeout) =>
            val response = if (timeout) ConvertTimedOut(resultId) else ConvertAccepted(resultId)
            originalReplyTo ! response
            Behaviors.same
          case GetResult(resultId, replyTo) =>
            // TODO retrieve the stored result and reply
            Behaviors.same
        }
      }

      private def active(
          next: WorkPullingProducerController.RequestNext[ImageConverter.ConversionJob]): Behavior[Command] = {
        Behaviors.receiveMessagePartial {
          case ConvertRequest(from, to, image, originalReplyTo) =>
            val resultId = UUID.randomUUID()
            context.ask[MessageWithConfirmation[ImageConverter.ConversionJob], Done](
              next.askNextTo,
              askReplyTo =>
                MessageWithConfirmation(ImageConverter.ConversionJob(resultId, from, to, image), askReplyTo)) {
              case Success(done) => AskReply(resultId, originalReplyTo, timeout = false)
              case Failure(_)    => AskReply(resultId, originalReplyTo, timeout = true)
            }
            waitForNext()
          case AskReply(resultId, originalReplyTo, timeout) =>
            val response = if (timeout) ConvertTimedOut(resultId) else ConvertAccepted(resultId)
            originalReplyTo ! response
            Behaviors.same
          case GetResult(resultId, replyTo) =>
            // TODO retrieve the stored result and reply
            Behaviors.same
          case _: WrappedRequestNext =>
            throw new IllegalStateException("Unexpected RequestNext")
        }
      }

      //#ask
    }
    //#producer
    //#ask
  }
  //#ask
  //#producer

}
