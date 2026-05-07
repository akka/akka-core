/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.stream.{ SinkRef, SourceRef }
import akka.stream.impl.streamref.{ SinkRefStageImpl, SourceRefStageImpl }
import akka.util.OptionVal

/**
 * Factories for creating stream refs.
 */
object StreamRefs {

  /**
   * A local [[Sink]] which materializes a [[SourceRef]] which can be used by other streams (including remote ones),
   * to consume data from this local stream, as if they were attached directly in place of the local Sink.
   *
   * Adheres to [[akka.stream.StreamRefAttributes]].
   *
   * See more detailed documentation on [[SourceRef]].
   */
  def sourceRef[T](): Sink[T, SourceRef[T]] =
    Sink.fromGraph(new SinkRefStageImpl[T](OptionVal.None))

  /**
   * A local [[Source]] which materializes a [[SinkRef]] which can be used by other streams (including remote ones),
   * to publish data to this local stream, as if they were attached directly in place of the local Source.
   *
   * Adheres to [[akka.stream.StreamRefAttributes]].
   *
   * See more detailed documentation on [[SinkRef]].
   */
  def sinkRef[T](): Source[T, SinkRef[T]] =
    Source.fromGraph(new SourceRefStageImpl[T](OptionVal.None))
}
