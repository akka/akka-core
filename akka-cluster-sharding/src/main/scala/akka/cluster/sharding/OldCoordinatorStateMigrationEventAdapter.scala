/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.annotation.InternalApi
import akka.cluster.sharding.ShardCoordinator.Internal.ShardHomeAllocated
import akka.persistence.journal.EventAdapter
import akka.persistence.journal.EventSeq

/**
 * Used for migrating from persistent state store mode to the new event sourced remember entities. No user API,
 * used through configuration. See reference docs for details.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class OldCoordinatorStateMigrationEventAdapter extends EventAdapter {
  override def manifest(event: Any): String =
    ""

  override def toJournal(event: Any): Any =
    event

  override def fromJournal(event: Any, manifest: String): EventSeq = {
    event match {
      case ShardHomeAllocated(shardId, _) =>
        EventSeq.single(shardId)
      case _ => EventSeq.empty
    }

  }
}
