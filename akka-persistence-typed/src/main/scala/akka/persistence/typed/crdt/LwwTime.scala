/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.crdt
import akka.persistence.typed.ReplicaId

/**
 * Utility class for comparing timestamp replica
 * identifier when implementing last-writer wins.
 */
final case class LwwTime(timestamp: Long, originReplica: ReplicaId) {

  /**
   * Create a new `LwwTime` that has a `timestamp` that is
   * `max` of the given timestamp and previous timestamp + 1,
   * i.e. monotonically increasing.
   */
  def increase(t: Long, replicaId: ReplicaId): LwwTime =
    LwwTime(math.max(timestamp + 1, t), replicaId)

  /**
   * Compare this `LwwTime` with the `other`.
   * Greatest timestamp wins. If both timestamps are
   * equal the `dc` identifiers are compared and the
   * one sorted first in alphanumeric order wins.
   */
  def isAfter(other: LwwTime): Boolean = {
    if (timestamp > other.timestamp) true
    else if (timestamp < other.timestamp) false
    else if (other.originReplica.id.compareTo(originReplica.id) > 0) true
    else false
  }
}
