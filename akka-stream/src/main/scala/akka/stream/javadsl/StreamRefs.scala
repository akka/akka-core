/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import akka.stream._

/**
 * Factories for creating stream refs.
 */
object StreamRefs {

  /**
   * A local [[Sink]] which materializes a [[SourceRef]] which can be used by other streams (including remote ones),
   * to consume data from this local stream, as if they were attached in the spot of the local Sink directly.
   *
   * Adheres to [[StreamRefAttributes]].
   *
   * See more detailed documentation on [[SourceRef]].
   */
  def sourceRef[T](): javadsl.Sink[T, SourceRef[T]] =
    scaladsl.StreamRefs.sourceRef[T]().asJava

  /**
   * A local [[Sink]] which materializes a [[SourceRef]] which can be used by other streams (including remote ones),
   * to consume data from this local stream, as if they were attached in the spot of the local Sink directly.
   *
   * Adheres to [[StreamRefAttributes]].
   *
   * See more detailed documentation on [[SinkRef]].
   */
  def sinkRef[T](): javadsl.Source[T, SinkRef[T]] =
    scaladsl.StreamRefs.sinkRef[T]().asJava

}
