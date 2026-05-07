/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.time.{ Duration => JavaDuration }
import java.util.Optional

import scala.concurrent.duration._
import scala.jdk.OptionConverters._
import scala.jdk.DurationConverters._
import scala.reflect.ClassTag

import com.typesafe.config.Config

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.internal.DeliverySerializable
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.util.Helpers.Requiring
import akka.util.Helpers.toRootLowerCase

/**
 * Point-to-point reliable delivery between a single producer actor sending messages and a single consumer
 * actor receiving the messages. Used together with [[ConsumerController]].
 *
 * The producer actor will start the flow by sending a [[ProducerController.Start]] message to
 * the `ProducerController`. The `ActorRef` in the `Start` message is typically constructed
 * as a message adapter to map the [[ProducerController.RequestNext]] to the protocol of the
 * producer actor.
 *
 * For the `ProducerController` to know where to send the messages it must be connected with the
 * `ConsumerController`. You do this is with [[ProducerController.RegisterConsumer]] or
 * [[ConsumerController.RegisterToProducerController]] messages.
 *
 * The `ProducerController` sends `RequestNext` to the producer, which is then allowed to send one
 * message to the `ProducerController` via the `sendNextTo` in the `RequestNext`. Thereafter the
 * producer will receive a new `RequestNext` when it's allowed to send one more message.
 *
 * The producer and `ProducerController` actors are supposed to be local so that these messages are
 * fast and not lost. This is enforced by a runtime check.
 *
 * Many unconfirmed messages can be in flight between the `ProducerController` and `ConsumerController`.
 * The flow control is driven by the consumer side, which means that the `ProducerController` will
 * not send faster than the demand requested by the `ConsumerController`.
 *
 * Lost messages are detected, resent and deduplicated if needed. This is also driven by the consumer side,
 * which means that the `ProducerController` will not push resends unless requested by the
 * `ConsumerController`.
 *
 * Until sent messages have been confirmed the `ProducerController` keeps them in memory to be able to
 * resend them. If the JVM of the `ProducerController` crashes those unconfirmed messages are lost.
 * To make sure the messages can be delivered also in that scenario the `ProducerController` can be
 * used with a [[DurableProducerQueue]]. Then the unconfirmed messages are stored in a durable way so
 * that they can be redelivered when the producer is started again. An implementation of the
 * `DurableProducerQueue` is provided by `EventSourcedProducerQueue` in `akka-persistence-typed`.
 *
 * Instead of using `tell` with the `sendNextTo` in the `RequestNext` the producer can use `context.ask`
 * with the `askNextTo` in the `RequestNext`. The difference is that a reply is sent back when the
 * message has been handled. If a `DurableProducerQueue` is used then the reply is sent when the message
 * has been stored successfully, but it might not have been processed by the consumer yet. Otherwise the
 * reply is sent after the consumer has processed and confirmed the message.
 *
 * If the consumer crashes a new `ConsumerController` can be connected to the original `ProducerConsumer`
 * without restarting it. The `ProducerConsumer` will then redeliver all unconfirmed messages.
 *
 * It's also possible to use the `ProducerController` and `ConsumerController` without resending
 * lost messages, but the flow control is still used. This can for example be useful when both consumer and
 * producer are know to be located in the same local `ActorSystem`. This can be more efficient since messages
 * don't have to be kept in memory in the `ProducerController` until they have been
 * confirmed, but the drawback is that lost messages will not be delivered. See configuration
 * `only-flow-control` of the `ConsumerController`.
 *
 * The `producerId` is used in logging and included as MDC entry with key `"producerId"`. It's propagated
 * to the `ConsumerController` and is useful for correlating log messages. It can be any `String` but it's
 * recommended to use a unique identifier of representing the producer.
 *
 * If the `DurableProducerQueue` is defined it is created as a child actor of the `ProducerController` actor.
 * It will use the same dispatcher as the parent `ProducerController`.
 */
@ApiMayChange // TODO #28719 when removing ApiMayChange consider removing `case class` for some of the messages
object ProducerController {
  import ProducerControllerImpl.UnsealedInternalCommand

  type SeqNr = Long

  sealed trait Command[A] extends UnsealedInternalCommand

  /**
   * Initial message from the producer actor. The `producer` is typically constructed
   * as a message adapter to map the [[RequestNext]] to the protocol of the producer actor.
   *
   * If the producer is restarted it should send a new `Start` message to the
   * `ProducerController`.
   */
  final case class Start[A](producer: ActorRef[RequestNext[A]]) extends Command[A]

  /**
   * The `ProducerController` sends `RequestNext` to the producer when it is allowed to send one
   * message via the `sendNextTo` or `askNextTo`. Note that only one message is allowed, and then
   * it must wait for next `RequestNext` before sending one more message.
   */
  final case class RequestNext[A](
      producerId: String,
      currentSeqNr: SeqNr,
      confirmedSeqNr: SeqNr,
      sendNextTo: ActorRef[A],
      askNextTo: ActorRef[MessageWithConfirmation[A]])

  /**
   * Java API: The generic `Class` type for `ProducerController.RequestNext` that can be used when creating a
   * `messageAdapter` for `Class<RequestNext<MessageType>>`.
   */
  def requestNextClass[A](): Class[RequestNext[A]] = classOf[RequestNext[A]]

  /**
   * For sending confirmation message back to the producer when the message has been confirmed.
   * Typically used with `context.ask` from the producer.
   *
   * If `DurableProducerQueue` is used the confirmation reply is sent when the message has been
   * successfully stored, meaning that the actual delivery to the consumer may happen later.
   * If `DurableProducerQueue` is not used the confirmation reply is sent when the message has been
   * fully delivered, processed, and confirmed by the consumer.
   */
  final case class MessageWithConfirmation[A](message: A, replyTo: ActorRef[SeqNr]) extends UnsealedInternalCommand

  /**
   * Register the given `consumerController` to the `ProducerController`.
   *
   * Alternatively, this registration can be done on the consumer side with the
   * [[ConsumerController.RegisterToProducerController]] message.
   *
   * When using a custom `send` function for the `ProducerController` this should not be used.
   */
  final case class RegisterConsumer[A](consumerController: ActorRef[ConsumerController.Command[A]])
      extends Command[A]
      with DeliverySerializable

  object Settings {

    /**
     * Scala API: Factory method from config `akka.reliable-delivery.producer-controller`
     * of the `ActorSystem`.
     */
    def apply(system: ActorSystem[_]): Settings =
      apply(system.settings.config.getConfig("akka.reliable-delivery.producer-controller"))

    /**
     * Scala API: Factory method from Config corresponding to
     * `akka.reliable-delivery.producer-controller`.
     */
    def apply(config: Config): Settings = {
      val chunkLargeMessagesBytes = toRootLowerCase(config.getString("chunk-large-messages")) match {
        case "off" => 0
        case _ =>
          config.getBytes("chunk-large-messages").requiring(_ <= Int.MaxValue, "Too large chunk-large-messages.").toInt
      }
      new Settings(
        durableQueueRequestTimeout = config.getDuration("durable-queue.request-timeout").toScala,
        durableQueueRetryAttempts = config.getInt("durable-queue.retry-attempts"),
        durableQueueResendFirstInterval = config.getDuration("durable-queue.resend-first-interval").toScala,
        chunkLargeMessagesBytes)
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
      val durableQueueRequestTimeout: FiniteDuration,
      val durableQueueRetryAttempts: Int,
      val durableQueueResendFirstInterval: FiniteDuration,
      val chunkLargeMessagesBytes: Int) {

    def withDurableQueueRetryAttempts(newDurableQueueRetryAttempts: Int): Settings =
      copy(durableQueueRetryAttempts = newDurableQueueRetryAttempts)

    /**
     * Scala API
     */
    def withDurableQueueRequestTimeout(newDurableQueueRequestTimeout: FiniteDuration): Settings =
      copy(durableQueueRequestTimeout = newDurableQueueRequestTimeout)

    /**
     * Scala API
     */
    def withDurableQueueResendFirstInterval(newDurableQueueResendFirstInterval: FiniteDuration): Settings =
      copy(durableQueueResendFirstInterval = newDurableQueueResendFirstInterval)

    /**
     * Java API
     */
    def withDurableQueueRequestTimeout(newDurableQueueRequestTimeout: JavaDuration): Settings =
      copy(durableQueueRequestTimeout = newDurableQueueRequestTimeout.toScala)

    /**
     * Java API
     */
    def withDurableQueueResendFirstInterval(newDurableQueueResendFirstInterval: JavaDuration): Settings =
      copy(durableQueueResendFirstInterval = newDurableQueueResendFirstInterval.toScala)

    /**
     * Java API
     */
    def getDurableQueueRequestTimeout(): JavaDuration =
      durableQueueRequestTimeout.toJava

    def withChunkLargeMessagesBytes(newChunkLargeMessagesBytes: Int): Settings =
      copy(chunkLargeMessagesBytes = newChunkLargeMessagesBytes)

    /**
     * Private copy method for internal use only.
     */
    private def copy(
        durableQueueRequestTimeout: FiniteDuration = durableQueueRequestTimeout,
        durableQueueRetryAttempts: Int = durableQueueRetryAttempts,
        durableQueueResendFirstInterval: FiniteDuration = durableQueueResendFirstInterval,
        chunkLargeMessagesBytes: Int = chunkLargeMessagesBytes) =
      new Settings(
        durableQueueRequestTimeout,
        durableQueueRetryAttempts,
        durableQueueResendFirstInterval,
        chunkLargeMessagesBytes)

    override def toString: String =
      s"Settings($durableQueueRequestTimeout, $durableQueueRetryAttempts, $durableQueueResendFirstInterval, $chunkLargeMessagesBytes)"
  }

  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    Behaviors.setup { context =>
      ProducerControllerImpl(producerId, durableQueueBehavior, ProducerController.Settings(context.system))
    }
  }

  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: Settings): Behavior[Command[A]] = {
    ProducerControllerImpl(producerId, durableQueueBehavior, settings)
  }

  /**
   * INTERNAL API
   *
   * For custom `send` function. For example used with Sharding where the message must be wrapped in
   * `ShardingEnvelope(SequencedMessage(msg))`.
   *
   * When this factory is used the [[RegisterConsumer]] is not needed.
   *
   * In the future we may make the custom `send` in `ProducerController` public to make it possible to
   * wrap it or send it in other ways when building higher level abstractions that are using the `ProducerController`.
   * That is used by `ShardingProducerController`.
   */
  @InternalApi private[akka] def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: Settings,
      send: ConsumerController.SequencedMessage[A] => Unit): Behavior[Command[A]] = {
    ProducerControllerImpl(producerId, durableQueueBehavior, settings, send)
  }

  /**
   * Java API
   */
  def create[A](
      messageClass: Class[A],
      producerId: String,
      durableQueueBehavior: Optional[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    apply(producerId, durableQueueBehavior.toScala)(ClassTag(messageClass))
  }

  /**
   * Java API
   */
  def create[A](
      messageClass: Class[A],
      producerId: String,
      durableQueueBehavior: Optional[Behavior[DurableProducerQueue.Command[A]]],
      settings: Settings): Behavior[Command[A]] = {
    apply(producerId, durableQueueBehavior.toScala, settings)(ClassTag(messageClass))
  }

}
