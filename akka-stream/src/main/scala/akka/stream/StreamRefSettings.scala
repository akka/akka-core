/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.concurrent.duration._

import akka.annotation.DoNotInherit

/**
 * Settings specific to [[SourceRef]] and [[SinkRef]].
 * More detailed documentation about each of the settings is available in `reference.conf`.
 */
@DoNotInherit
trait StreamRefSettings {
  private[akka] def bufferCapacity: Int
  private[akka] def demandRedeliveryInterval: FiniteDuration
  private[akka] def subscriptionTimeout: FiniteDuration
  private[akka] def finalTerminationSignalDeadline: FiniteDuration

  def withBufferCapacity(value: Int): StreamRefSettings
  def withDemandRedeliveryInterval(value: FiniteDuration): StreamRefSettings
  def withSubscriptionTimeout(value: FiniteDuration): StreamRefSettings
  def withTerminationReceivedBeforeCompletionLeeway(value: FiniteDuration): StreamRefSettings
}
