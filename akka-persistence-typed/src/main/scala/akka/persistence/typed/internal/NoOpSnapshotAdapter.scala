/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.annotation.InternalApi
import akka.persistence.typed.SnapshotAdapter

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class NoOpSnapshotAdapter extends SnapshotAdapter[Any] {
  override def toJournal(state: Any): Any = state
  override def fromJournal(from: Any): Any = from
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object NoOpSnapshotAdapter {
  val i = new NoOpSnapshotAdapter
  def instance[S]: SnapshotAdapter[S] = i.asInstanceOf[SnapshotAdapter[S]]
}
