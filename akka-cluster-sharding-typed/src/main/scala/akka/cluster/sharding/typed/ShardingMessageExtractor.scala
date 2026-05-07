/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.annotation.nowarn

import akka.actor.{ InvalidMessageException, WrappedMessage }
import akka.cluster.sharding.typed.internal.ClusterShardingTypedSerializable

object ShardingMessageExtractor {

  /**
   * Scala API:
   *
   * Create the default message extractor, using envelopes to identify what entity a message is for
   * and the hashcode of the entityId to decide which shard an entity belongs to.
   *
   * This is recommended since it does not force details about sharding into the entity protocol
   */
  def apply[M](numberOfShards: Int): ShardingMessageExtractor[ShardingEnvelope[M], M] =
    new HashCodeMessageExtractor[M](numberOfShards)

  /**
   * Scala API: Create a message extractor for a protocol where the entity id is available in each message.
   */
  def noEnvelope[M](numberOfShards: Int, @nowarn("msg=never used") stopMessage: M)(
      extractEntityId: M => String): ShardingMessageExtractor[M, M] =
    new HashCodeNoEnvelopeMessageExtractor[M](numberOfShards) {
      def entityId(message: M) = extractEntityId(message)
    }
}

/**
 * Entirely customizable typed message extractor. Prefer [[HashCodeMessageExtractor]] or
 * [[HashCodeNoEnvelopeMessageExtractor]]if possible.
 *
 * @tparam E Possibly an Envelope around the messages accepted by the entity actor, is the same as `M` if there is no
 *           envelope.
 * @tparam M The type of message accepted by the entity actor
 */
abstract class ShardingMessageExtractor[E, M] {

  /**
   * Extract the entity id from an incoming `message`. If `null` is returned
   * the message will be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   */
  def entityId(message: E): String

  /**
   * The shard identifier for a given entity id. Only messages that passed the [[ShardingMessageExtractor#entityId]]
   * function will be used as input to this function.
   */
  def shardId(entityId: String): String

  /**
   * Extract the message to send to the entity from an incoming `message`.
   * Note that the extracted message does not have to be the same as the incoming
   * message to support wrapping in message envelope that is unwrapped before
   * sending to the entity actor.
   */
  def unwrapMessage(message: E): M

}

/**
 * Default message extractor type, using envelopes to identify what entity a message is for
 * and the hashcode of the entityId to decide which shard an entity belongs to.
 *
 * This is recommended since it does not force details about sharding into the entity protocol
 *
 * @tparam M The type of message accepted by the entity actor
 */
final class HashCodeMessageExtractor[M](val numberOfShards: Int)
    extends ShardingMessageExtractor[ShardingEnvelope[M], M] {

  override def entityId(envelope: ShardingEnvelope[M]): String = envelope.entityId
  override def shardId(entityId: String): String =
    akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor.shardId(entityId, numberOfShards)
  override def unwrapMessage(envelope: ShardingEnvelope[M]): M = envelope.message
}

/**
 * Default message extractor type, using a property of the message to identify what entity a message is for
 * and the hashcode of the entityId to decide which shard an entity belongs to.
 *
 * This is recommended since it does not force details about sharding into the entity protocol
 *
 * @tparam M The type of message accepted by the entity actor
 */
abstract class HashCodeNoEnvelopeMessageExtractor[M](val numberOfShards: Int) extends ShardingMessageExtractor[M, M] {

  override def shardId(entityId: String): String =
    akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor.shardId(entityId, numberOfShards)
  override final def unwrapMessage(message: M): M = message

  override def toString = s"HashCodeNoEnvelopeMessageExtractor($numberOfShards)"
}

/**
 * Default envelope type that may be used with Cluster Sharding.
 *
 * Cluster Sharding provides a default [[HashCodeMessageExtractor]] that is able to handle
 * these types of messages, by hashing the entityId into into the shardId. It is not the only,
 * but a convenient way to send envelope-wrapped messages via cluster sharding.
 *
 * The alternative way of routing messages through sharding is to not use envelopes,
 * and have the message types themselves carry identifiers.
 *
 * @param entityId The business domain identifier of the entity.
 * @param message The message to be send to the entity.
 * @throws `InvalidMessageException` if message is null.
 */
final case class ShardingEnvelope[M](entityId: String, message: M)
    extends WrappedMessage
    with ClusterShardingTypedSerializable {
  if (message == null) throw InvalidMessageException("[null] is not an allowed message")
}
