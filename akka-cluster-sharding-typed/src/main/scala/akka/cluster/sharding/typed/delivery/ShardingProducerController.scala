/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.delivery

import java.util.Optional

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.jdk.OptionConverters._
import scala.reflect.ClassTag

import com.typesafe.config.Config

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.ApiMayChange
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.delivery.internal.ShardingProducerControllerImpl

/**
 * Reliable delivery between a producer actor sending messages to sharded consumer
 * actors receiving the messages.
 *
 * The `ShardingProducerController` should be used together with [[ShardingConsumerController]].
 *
 * A producer can send messages via a `ShardingProducerController` to any `ShardingConsumerController`
 * identified by an `entityId`. A single `ShardingProducerController` per `ActorSystem` (node) can be
 * shared for sending to all entities of a certain entity type. No explicit registration is needed
 * between the `ShardingConsumerController` and `ShardingProducerController`.
 *
 * The producer actor will start the flow by sending a [[ShardingProducerController.Start]]
 * message to the `ShardingProducerController`. The `ActorRef` in the `Start` message is
 * typically constructed as a message adapter to map the [[ShardingProducerController.RequestNext]]
 * to the protocol of the producer actor.
 *
 * The `ShardingProducerController` sends `RequestNext` to the producer, which is then allowed
 * to send one message to the `ShardingProducerController` via the `sendNextTo` in the `RequestNext`.
 * Thereafter the producer will receive a new `RequestNext` when it's allowed to send one more message.
 *
 * In the `RequestNext` message there is information about which entities that have demand. It is allowed
 * to send to a new `entityId` that is not included in the `RequestNext.entitiesWithDemand`. If sending to
 * an entity that doesn't have demand the message will be buffered. This support for buffering means that
 * it is even allowed to send several messages in response to one `RequestNext` but it's recommended to
 * only send one message and wait for next `RequestNext` before sending more messages.
 *
 * The producer and `ShardingProducerController` actors are supposed to be local so that these messages are
 * fast and not lost. This is enforced by a runtime check.
 *
 * There will be one `ShardingConsumerController` for each entity. Many unconfirmed messages can be in
 * flight between the `ShardingProducerController` and each `ShardingConsumerController`. The flow control
 * is driven by the consumer side, which means that the `ShardingProducerController` will not send faster
 * than the demand requested by the consumers.
 *
 * Lost messages are detected, resent and deduplicated if needed. This is also driven by the consumer side,
 * which means that the `ShardingProducerController` will not push resends unless requested by the
 * `ShardingConsumerController`.
 *
 * Until sent messages have been confirmed the `ShardingProducerController` keeps them in memory to be able to
 * resend them. If the JVM of the `ShardingProducerController` crashes those unconfirmed messages are lost.
 * To make sure the messages can be delivered also in that scenario the `ShardingProducerController` can be
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
 * It's also possible to use the `ShardingProducerController` and `ShardingConsumerController` without resending
 * lost messages, but the flow control is still used. This can be more efficient since messages
 * don't have to be kept in memory in the `ProducerController` until they have been
 * confirmed, but the drawback is that lost messages will not be delivered. See configuration
 * `only-flow-control` of the `ShardingConsumerController`.
 *
 * The `producerId` is used in logging and included as MDC entry with key `"producerId"`. It's propagated
 * to the `ConsumerController` and is useful for correlating log messages. It can be any `String` but it's
 * recommended to use a unique identifier of representing the producer.
 *
 * If the `DurableProducerQueue` is defined it is created as a child actor of the `ShardingProducerController` actor.
 * `ProducerController` actors are created for each destination entity. Those child actors use the same dispatcher
 * as the parent `ShardingProducerController`.
 */
@ApiMayChange // TODO #28719 when removing ApiMayChange consider removing `case class` for some of the messages
object ShardingProducerController {

  import ShardingProducerControllerImpl.UnsealedInternalCommand

  type EntityId = String

  sealed trait Command[A] extends UnsealedInternalCommand

  /**
   * Initial message from the producer actor. The `producer` is typically constructed
   * as a message adapter to map the [[RequestNext]] to the protocol of the producer actor.
   *
   * If the producer is restarted it should send a new `Start` message to the
   * `ShardingProducerController`.
   */
  final case class Start[A](producer: ActorRef[RequestNext[A]]) extends Command[A]

  /**
   * For sending confirmation message back to the producer when the message has been confirmed.
   * Typically used with `context.ask` from the producer.
   *
   * If `DurableProducerQueue` is used the confirmation reply is sent when the message has been
   * successfully stored, meaning that the actual delivery to the consumer may happen later.
   * If `DurableProducerQueue` is not used the confirmation reply is sent when the message has been
   * fully delivered, processed, and confirmed by the consumer.
   */
  final case class MessageWithConfirmation[A](entityId: EntityId, message: A, replyTo: ActorRef[Done])
      extends UnsealedInternalCommand

  /**
   * The `ProducerController` sends `RequestNext` to the producer when it is allowed to send one
   * message via the `sendNextTo` or `askNextTo`. It should wait for next `RequestNext` before
   * sending one more message.
   *
   * `entitiesWithDemand` contains information about which entities that have demand. It is allowed
   * to send to a new `entityId` that is not included in the `entitiesWithDemand`. If sending to
   * an entity that doesn't have demand the message will be buffered, and that can be seen in the
   * `bufferedForEntitiesWithoutDemand`.
   *
   * This support for buffering means that it is even allowed to send several messages in response
   * to one `RequestNext` but it's recommended to only send one message and wait for next `RequestNext`
   * before sending more messages.
   */
  final case class RequestNext[A](
      sendNextTo: ActorRef[ShardingEnvelope[A]],
      askNextTo: ActorRef[MessageWithConfirmation[A]],
      entitiesWithDemand: Set[EntityId],
      bufferedForEntitiesWithoutDemand: Map[EntityId, Int]) {

    /** Java API */
    def getEntitiesWithDemand: java.util.Set[String] = {
      import scala.jdk.CollectionConverters._
      entitiesWithDemand.asJava
    }

    /** Java API */
    def getBufferedForEntitiesWithoutDemand: java.util.Map[String, Integer] = {
      import scala.jdk.CollectionConverters._
      bufferedForEntitiesWithoutDemand.iterator.map { case (k, v) => k -> v.asInstanceOf[Integer] }.toMap.asJava
    }
  }

  /**
   * Java API: The generic `Class` type for `ShardingProducerController.RequestNext` that can be used when creating a
   * `messageAdapter` for `Class<RequestNext<MessageType>>`.
   */
  def requestNextClass[A](): Class[RequestNext[A]] = classOf[RequestNext[A]]

  object Settings {

    /**
     * Scala API: Factory method from config `akka.reliable-delivery.sharding.producer-controller`
     * of the `ActorSystem`.
     */
    def apply(system: ActorSystem[_]): Settings =
      apply(system.settings.config.getConfig("akka.reliable-delivery.sharding.producer-controller"))

    /**
     * Scala API: Factory method from Config corresponding to
     * `akka.reliable-delivery.sharding.producer-controller`.
     */
    def apply(config: Config): Settings = {
      new Settings(
        bufferSize = config.getInt("buffer-size"),
        config.getDuration("internal-ask-timeout").toScala,
        config.getDuration("cleanup-unused-after").toScala,
        config.getDuration("resend-first-unconfirmed-idle-timeout").toScala,
        ProducerController.Settings(config))
    }

    /**
     * Java API: Factory method from config `akka.reliable-delivery.sharding.producer-controller`
     * of the `ActorSystem`.
     */
    def create(system: ActorSystem[_]): Settings =
      apply(system)

    /**
     * Java API: Factory method from Config corresponding to
     * `akka.reliable-delivery.sharding.producer-controller`.
     */
    def create(config: Config): Settings =
      apply(config)
  }

  final class Settings private (
      val bufferSize: Int,
      val internalAskTimeout: FiniteDuration,
      val cleanupUnusedAfter: FiniteDuration,
      val resendFirstUnconfirmedIdleTimeout: FiniteDuration,
      val producerControllerSettings: ProducerController.Settings) {

    @Deprecated
    @deprecated("use resendFirstUnconfirmedIdleTimeout", "2.6.19")
    def resendFirsUnconfirmedIdleTimeout: FiniteDuration = resendFirstUnconfirmedIdleTimeout

    if (producerControllerSettings.chunkLargeMessagesBytes > 0)
      throw new IllegalArgumentException("Chunked messages not implemented for sharding yet.")

    def withBufferSize(newBufferSize: Int): Settings =
      copy(bufferSize = newBufferSize)

    def withInternalAskTimeout(newInternalAskTimeout: FiniteDuration): Settings =
      copy(internalAskTimeout = newInternalAskTimeout)

    def withInternalAskTimeout(newInternalAskTimeout: java.time.Duration): Settings =
      copy(internalAskTimeout = newInternalAskTimeout.toScala)

    def withCleanupUnusedAfter(newCleanupUnusedAfter: FiniteDuration): Settings =
      copy(cleanupUnusedAfter = newCleanupUnusedAfter)

    def withCleanupUnusedAfter(newCleanupUnusedAfter: java.time.Duration): Settings =
      copy(cleanupUnusedAfter = newCleanupUnusedAfter.toScala)

    def withResendFirstUnconfirmedIdleTimeout(newResendFirstUnconfirmedIdleTimeout: FiniteDuration): Settings =
      copy(resendFirstUnconfirmedIdleTimeout = newResendFirstUnconfirmedIdleTimeout)

    def withResendFirstUnconfirmedIdleTimeout(newResendFirstUnconfirmedIdleTimeout: java.time.Duration): Settings =
      copy(resendFirstUnconfirmedIdleTimeout = newResendFirstUnconfirmedIdleTimeout.toScala)

    @deprecated("use resendFirstUnconfirmedIdleTimeout", "2.6.19")
    def withResendFirsUnconfirmedIdleTimeout(newResendFirstUnconfirmedIdleTimeout: FiniteDuration): Settings =
      copy(resendFirstUnconfirmedIdleTimeout = newResendFirstUnconfirmedIdleTimeout)

    @Deprecated
    def withResendFirsUnconfirmedIdleTimeout(newResendFirstUnconfirmedIdleTimeout: java.time.Duration): Settings =
      copy(resendFirstUnconfirmedIdleTimeout = newResendFirstUnconfirmedIdleTimeout.toScala)

    def withProducerControllerSettings(newProducerControllerSettings: ProducerController.Settings): Settings =
      copy(producerControllerSettings = newProducerControllerSettings)

    /**
     * Private copy method for internal use only.
     */
    private def copy(
        bufferSize: Int = bufferSize,
        internalAskTimeout: FiniteDuration = internalAskTimeout,
        cleanupUnusedAfter: FiniteDuration = cleanupUnusedAfter,
        resendFirstUnconfirmedIdleTimeout: FiniteDuration = resendFirstUnconfirmedIdleTimeout,
        producerControllerSettings: ProducerController.Settings = producerControllerSettings) =
      new Settings(
        bufferSize,
        internalAskTimeout,
        cleanupUnusedAfter,
        resendFirstUnconfirmedIdleTimeout,
        producerControllerSettings)

    override def toString: String =
      s"Settings($bufferSize,$internalAskTimeout,$resendFirstUnconfirmedIdleTimeout,$producerControllerSettings)"
  }

  def apply[A: ClassTag](
      producerId: String,
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    Behaviors.setup { context =>
      ShardingProducerControllerImpl(producerId, region, durableQueueBehavior, Settings(context.system))
    }
  }

  def apply[A: ClassTag](
      producerId: String,
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: Settings): Behavior[Command[A]] = {
    ShardingProducerControllerImpl(producerId, region, durableQueueBehavior, settings)
  }

  /**
   * Java API
   */
  def create[A](
      messageClass: Class[A],
      producerId: String,
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueueBehavior: Optional[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    apply(producerId, region, durableQueueBehavior.toScala)(ClassTag(messageClass))
  }

  /**
   * Java API
   */
  def create[A](
      messageClass: Class[A],
      producerId: String,
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueueBehavior: Optional[Behavior[DurableProducerQueue.Command[A]]],
      settings: Settings): Behavior[Command[A]] = {
    apply(producerId, region, durableQueueBehavior.toScala, settings)(ClassTag(messageClass))
  }

  // TODO maybe there is a need for variant taking message extractor instead of ShardingEnvelope
}
