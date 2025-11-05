/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import SubstreamCancelStrategies._

/**
 * Represents a strategy that decides how to deal with substream events.
 */
sealed abstract class SubstreamCancelStrategy

private[akka] object SubstreamCancelStrategies {

  /**
   * INTERNAL API
   */
  private[akka] case object Propagate extends SubstreamCancelStrategy

  /**
   * INTERNAL API
   */
  private[akka] case object Drain extends SubstreamCancelStrategy
}

object SubstreamCancelStrategy {

  /**
   * Cancel the stream of streams if any substream is cancelled.
   */
  def propagate: SubstreamCancelStrategy = Propagate

  /**
   * Drain substream on cancellation in order to prevent stalling of the stream of streams.
   */
  def drain: SubstreamCancelStrategy = Drain
}
