/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import akka.cluster.sharding.typed.internal.Murmur2

abstract class Murmur2NoEnvelopeMessageExtractor[M](val numberOfShards: Int) extends ShardingMessageExtractor[M, M] {
  override def shardId(entityId: String): String = Murmur2.shardId(entityId, numberOfShards)
  override def unwrapMessage(message: M): M = message
}

/**
 * The murmur2 message extractor uses the same algorithm as the default kafka partitioner
 * allowing kafka partitions to be mapped to shards.
 * This can be used with the [[akka.cluster.sharding.external.ExternalShardAllocationStrategy]] to have messages
 * processed locally.
 *
 * Extend [[Murmur2NoEnvelopeMessageExtractor]] to not use a message envelope extractor.
 */
final class Murmur2MessageExtractor[M](val numberOfShards: Int)
    extends ShardingMessageExtractor[ShardingEnvelope[M], M] {
  override def entityId(envelope: ShardingEnvelope[M]): String = envelope.entityId
  override def shardId(entityId: String): String = Murmur2.shardId(entityId, numberOfShards)
  override def unwrapMessage(envelope: ShardingEnvelope[M]): M = envelope.message
}
