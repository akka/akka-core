/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import akka.annotation.InternalApi
import akka.persistence.SnapshotMetadata
import akka.persistence.testkit.SnapshotStorage

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] class SimpleSnapshotStorageImpl extends SnapshotStorage {

  override type InternalRepr = (SnapshotMetadata, Any)

  override def toRepr(internal: (SnapshotMetadata, Any)): (SnapshotMetadata, Any) = identity(internal)

  override def toInternal(repr: (SnapshotMetadata, Any)): (SnapshotMetadata, Any) = identity(repr)

}
