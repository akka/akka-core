/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.time.{ Duration => JavaDuration }

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.DeadLetterSuppression
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.actor.typed.delivery.internal.ConsumerControllerImpl
import akka.actor.typed.delivery.internal.DeliverySerializable
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

/**
 * `ConsumerController` and [[ProducerController]] or [[WorkPullingProducerController]] are used
 * together. See the descriptions in those classes or the Akka reference documentation for
 * how they are intended to be used.
 *
 * The destination consumer actor will start the flow by sending an initial [[ConsumerController.Start]]
 * message to the `ConsumerController`. The `ActorRef` in the `Start` message is typically constructed
 * as a message adapter to map the [[ConsumerController.Delivery]] to the protocol of the consumer actor.
 *
 * Received messages from the producer are wrapped in [[ConsumerController.Delivery]] when sent to the consumer,
 * which is supposed to reply with [[ConsumerController.Confirmed]] when it has processed the message.
 * Next message is not delivered until the previous is confirmed.
 * More messages from the producer that arrive while waiting for the confirmation are stashed by
 * the `ConsumerController` and delivered when previous message was confirmed.
 *
 * The consumer and the `ConsumerController` actors are supposed to be local so that these messages are fast
 * and not lost. This is enforced by a runtime check.
 *
 * The `ConsumerController` is automatically stopped when the consumer that registered with the `Start`
 * message is terminated.
 */
@ApiMayChange // TODO #28719 when removing ApiMayChange consider removing `case class` for some of the messages
object ConsumerController {
  import ConsumerControllerImpl.UnsealedInternalCommand

  type SeqNr = Long

  sealed trait Command[+A] extends UnsealedInternalCommand

  /**
   * Initial message from the consumer actor. The `deliverTo` is typically constructed
   * as a message adapter to map the [[Delivery]] to the protocol of the consumer actor.
   *
   * If the consumer is restarted it should send a new `Start` message to the
   * `ConsumerController`.
   */
  final case class Start[A](deliverTo: ActorRef[Delivery[A]]) extends Command[A]

  object Delivery {
    def apply[A](message: A, confirmTo: ActorRef[Confirmed], producerId: String, seqNr: SeqNr): Delivery[A] =
      new Delivery(message, confirmTo, producerId, seqNr)

    def unapply[A](delivery: Delivery[A]): Option[(A, ActorRef[Confirmed])] =
      Option((delivery.message, delivery.confirmTo))
  }

  /**
   * Received messages from the producer are wrapped in `Delivery` when sent to the consumer.
   * When the message has been processed the consumer is supposed to send [[Confirmed]] back
   * to the `ConsumerController` via the `confirmTo`.
   */
  final class Delivery[A](
      val message: A,
      val confirmTo: ActorRef[Confirmed],
      val producerId: String,
      val seqNr: SeqNr) {
    override def toString: String = s"Delivery($message,$confirmTo,$producerId,$seqNr)"
  }

  /**
   * Java API: The generic `Class` type for `ConsumerController.Delivery` that can be used when creating a
   * `messageAdapter` for `Class<Delivery<MessageType>>`.
   */
  def deliveryClass[A](): Class[Delivery[A]] = classOf[Delivery[A]]

  /**
   * Java API: The generic `Class` type for `ConsumerController.Command` that can be used when creating a `ServiceKey`
   * for `Class<Command<MessageType>>`.
   */
  def serviceKeyClass[A]: Class[Command[A]] = classOf[Command[A]]

  @DoNotInherit
  trait Confirmed extends UnsealedInternalCommand

  /**
   * When the message has been processed the consumer is supposed to send `Confirmed` back
   * to the `ConsumerController` via the `confirmTo` in the [[Delivery]] message.
   */
  case object Confirmed extends Confirmed

  /**
   * Java API: the singleton instance of the Confirmed message.
   * When the message has been processed the consumer is supposed to send `Confirmed` back
   * to the `ConsumerController` via the `confirmTo` in the [[Delivery]] message.
   */
  def confirmed(): Confirmed = Confirmed

  /**
   * Register the `ConsumerController` to the given `producerController`. It will
   * retry the registration until the `ProducerController` has acknowledged by sending its
   * first message.
   *
   * Alternatively, this registration can be done on the producer side with the
   * [[ProducerController.RegisterConsumer]] message.
   */
  final case class RegisterToProducerController[A](producerController: ActorRef[ProducerController.Command[A]])
      extends Command[A]

  final case class DeliverThenStop[A]() extends Command[A]

  object SequencedMessage {

    /**
     * SequencedMessage.message can be `A` or `ChunkedMessage`.
     */
    type MessageOrChunk = Any

    /**
     * INTERNAL API
     */
    @InternalApi private[akka] def fromChunked[A](
        producerId: String,
        seqNr: SeqNr,
        chunk: ChunkedMessage,
        first: Boolean,
        ack: Boolean,
        producerController: ActorRef[ProducerControllerImpl.InternalCommand]): SequencedMessage[A] =
      new SequencedMessage(producerId, seqNr, chunk, first, ack)(producerController)
  }

  /**
   * This is used between the `ProducerController` and `ConsumerController`. Should rarely be used in
   * application code but is public because it's in the signature for the `EntityTypeKey` when using
   * `ShardingConsumerController`.
   *
   * In the future we may also make the custom `send` in `ProducerController` public to make it possible to
   * wrap it or send it in other ways when building higher level abstractions that are using the `ProducerController`.
   * That is used by `ShardingProducerController`.
   *
   * @param producerController INTERNAL API: construction of SequencedMessage is internal
   */
  final case class SequencedMessage[A](
      producerId: String,
      seqNr: SeqNr,
      message: SequencedMessage.MessageOrChunk,
      first: Boolean,
      ack: Boolean)(@InternalApi private[akka] val producerController: ActorRef[ProducerControllerImpl.InternalCommand])
      extends Command[A]
      with DeliverySerializable
      with DeadLetterSuppression {

    /** INTERNAL API */
    @InternalApi private[akka] def asFirst: SequencedMessage[A] =
      copy(first = true)(producerController)

    /** INTERNAL API */
    @InternalApi private[akka] def isFirstChunk: Boolean = {
      message match {
        case c: ChunkedMessage => c.firstChunk
        case _                 => true
      }
    }

    /** INTERNAL API */
    @InternalApi private[akka] def isLastChunk: Boolean = {
      message match {
        case c: ChunkedMessage => c.lastChunk
        case _                 => true
      }
    }
  }

  object Settings {

    /**
     * Scala API: Factory method from config `akka.reliable-delivery.consumer-controller`
     * of the `ActorSystem`.
     */
    def apply(system: ActorSystem[_]): Settings =
      apply(system.settings.config.getConfig("akka.reliable-delivery.consumer-controller"))

    /**
     * Scala API: Factory method from Config corresponding to
     * `akka.reliable-delivery.consumer-controller`.
     */
    def apply(config: Config): Settings = {
      new Settings(
        flowControlWindow = config.getInt("flow-control-window"),
        resendIntervalMin = config.getDuration("resend-interval-min").toScala,
        resendIntervalMax = config.getDuration("resend-interval-max").toScala,
        onlyFlowControl = config.getBoolean("only-flow-control"))
    }

    /**
     * Java API: Factory method from config `akka.reliable-delivery.producer-controller`
     * of the `ActorSystem`.
     */
    def create(system: ActorSystem[_]): Settings =
      apply(system)

    /**
     * Java API: Factory method from Config corresponding to
     * `akka.reliable-delivery.producer-controller`.
     */
    def create(config: Config): Settings =
      apply(config)
  }

  final class Settings private (
      val flowControlWindow: Int,
      val resendIntervalMin: FiniteDuration,
      val resendIntervalMax: FiniteDuration,
      val onlyFlowControl: Boolean) {

    def withFlowControlWindow(newFlowControlWindow: Int): Settings =
      copy(flowControlWindow = newFlowControlWindow)

    /**
     * Scala API
     */
    def withResendIntervalMin(newResendIntervalMin: FiniteDuration): Settings =
      copy(resendIntervalMin = newResendIntervalMin)

    /**
     * Scala API
     */
    def withResendIntervalMax(newResendIntervalMax: FiniteDuration): Settings =
      copy(resendIntervalMax = newResendIntervalMax)

    /**
     * Java API
     */
    def withResendIntervalMin(newResendIntervalMin: JavaDuration): Settings =
      copy(resendIntervalMin = newResendIntervalMin.toScala)

    /**
     * Java API
     */
    def withResendIntervalMax(newResendIntervalMax: JavaDuration): Settings =
      copy(resendIntervalMax = newResendIntervalMax.toScala)

    /**
     * Java API
     */
    def getResendIntervalMax(): JavaDuration =
      resendIntervalMax.toJava

    def withOnlyFlowControl(newOnlyFlowControl: Boolean): Settings =
      copy(onlyFlowControl = newOnlyFlowControl)

    /**
     * Private copy method for internal use only.
     */
    private def copy(
        flowControlWindow: Int = flowControlWindow,
        resendIntervalMin: FiniteDuration = resendIntervalMin,
        resendIntervalMax: FiniteDuration = resendIntervalMax,
        onlyFlowControl: Boolean = onlyFlowControl) =
      new Settings(flowControlWindow, resendIntervalMin, resendIntervalMax, onlyFlowControl)

    override def toString: String =
      s"Settings($flowControlWindow, $resendIntervalMin, $onlyFlowControl)"
  }

  def apply[A](): Behavior[Command[A]] =
    Behaviors.setup { context =>
      apply(serviceKey = None, Settings(context.system))
    }

  def apply[A](settings: Settings): Behavior[Command[A]] =
    apply(serviceKey = None, settings)

  /**
   * To be used with [[WorkPullingProducerController]]. It will register itself to the
   * [[akka.actor.typed.receptionist.Receptionist]] with the given `serviceKey`, and the
   * `WorkPullingProducerController` subscribes to the same key to find active workers.
   */
  def apply[A](serviceKey: ServiceKey[Command[A]]): Behavior[Command[A]] =
    Behaviors.setup { context =>
      apply(Some(serviceKey), Settings(context.system))
    }

  def apply[A](serviceKey: ServiceKey[Command[A]], settings: Settings): Behavior[Command[A]] =
    apply(Some(serviceKey), settings)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def apply[A](
      serviceKey: Option[ServiceKey[Command[A]]],
      settings: Settings): Behavior[Command[A]] = {
    ConsumerControllerImpl(serviceKey, settings)
  }

  /**
   * Java API
   */
  def create[A](): Behavior[Command[A]] =
    apply()

  /**
   * Java API
   */
  def create[A](settings: Settings): Behavior[Command[A]] =
    apply(settings)

  /**
   * Java API: To be used with [[WorkPullingProducerController]]. It will register itself to the
   * [[akka.actor.typed.receptionist.Receptionist]] with the given `serviceKey`, and the
   * `WorkPullingProducerController` subscribes to the same key to find active workers.
   */
  def create[A](serviceKey: ServiceKey[Command[A]]): Behavior[Command[A]] =
    apply(serviceKey)

  /**
   * Java API
   */
  def create[A](serviceKey: ServiceKey[Command[A]], settings: Settings): Behavior[Command[A]] =
    apply(Some(serviceKey), settings)

}
