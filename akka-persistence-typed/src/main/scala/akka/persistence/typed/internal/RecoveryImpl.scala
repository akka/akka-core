/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.annotation.InternalApi
import akka.persistence.typed.{ javadsl, scaladsl, SnapshotSelectionCriteria }

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object DefaultRecovery extends javadsl.Recovery with scaladsl.Recovery {
  override def asScala: scaladsl.Recovery = this
  override def asJava: javadsl.Recovery = this

  /**
   * INTERNAL API
   */
  override private[akka] def toClassic = akka.persistence.Recovery.default
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object DisabledRecovery extends javadsl.Recovery with scaladsl.Recovery {
  override def asScala: scaladsl.Recovery = this
  override def asJava: javadsl.Recovery = this

  /**
   * INTERNAL API
   */
  override private[akka] def toClassic = akka.persistence.Recovery.none
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object ReplayOnlyLastRecovery extends javadsl.Recovery with scaladsl.Recovery {
  override def asScala: scaladsl.Recovery = this
  override def asJava: javadsl.Recovery = this

  /**
   * INTERNAL API
   */
  override private[akka] val toClassic =
    akka.persistence.Recovery(akka.persistence.SnapshotSelectionCriteria.None)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case class RecoveryWithSnapshotSelectionCriteria(
    snapshotSelectionCriteria: SnapshotSelectionCriteria)
    extends javadsl.Recovery
    with scaladsl.Recovery {
  override def asScala: scaladsl.Recovery = this
  override def asJava: javadsl.Recovery = this

  /**
   * INTERNAL API
   */
  override private[akka] def toClassic = akka.persistence.Recovery(snapshotSelectionCriteria.toClassic)
}
